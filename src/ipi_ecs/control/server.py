import sys
import queue
import time
import uuid

import ipi_ecs.core.tcp as tcp
import ipi_ecs.core.daemon as daemon
import ipi_ecs.core.mt_events as mt_events
import ipi_ecs.core.transactions as transactions
import ipi_ecs.core.segmented_bytearray as segmented_bytearray
from ipi_ecs.control.subsystem import SubsystemInfo
from ipi_ecs.control.magics import *
        

class ControlServer:
    __E_ON_CLIENT_CONNECT = 0
    __E_ON_CLIENT_DISCONNECT = 1

    class _ClientConnection:
        __E_MESSAGE = 0
        __E_CLOSED = 1
        __E_CONNECTED = 2
        __E_TRANSACT_DATA_AVAIL = 3
        __E_NEW_TRANSACT = 4

        def __init__(self, sock: tcp.TCPServerSocket, server: "ControlServer"):
            self.__socket = sock
            self.__server = server

            self.__event_consumer = mt_events.EventConsumer()
            
            self.__transactions_msg_out_queue = queue.Queue()
            self.__transactions = transactions.TransactionManager(self.__transactions_msg_out_queue)

            self.__socket.on_receive(self.__event_consumer, self.__E_MESSAGE)
            self.__socket.on_close(self.__event_consumer, self.__E_CLOSED)
            self.__socket.on_connect(self.__event_consumer, self.__E_CONNECTED)
            self.__transactions.on_send_data(self.__event_consumer, self.__E_TRANSACT_DATA_AVAIL)
            self.__transactions.on_receive_transaction(self.__event_consumer, self.__E_NEW_TRANSACT)

            self.__handshake_received = False
            self.__uuid = uuid.UUID(bytes=bytes(16))

            self.__daemon = daemon.Daemon()
            self.__daemon.add(self.__thread)
            self.__daemon.start()

        def __thread(self, stop_flag : daemon.StopFlag):
            while stop_flag.run():
                e = self.__event_consumer.get()

                if e == self.__E_MESSAGE:
                    self.__receive()
                elif e == self.__E_CLOSED:
                    self.close()
                elif e == self.__E_CONNECTED:
                    self.__connected()
                elif e == self.__E_TRANSACT_DATA_AVAIL:
                    self.__flush_transponder()
                elif e == self.__E_NEW_TRANSACT:
                    self.__receive_transact()

        def __receive(self):
            while not self.__socket.empty():
                d = self.__socket.get(timeout=1)

                if len(d) == 0:
                    continue

                if d == bytes([MAGIC_HANDSHAKE_CLIENT]):
                    if self.__handshake_received:
                        raise Exception("Handshake on existing connection!")

                    print("Handshake received from ", self.__socket.remote())
                    self.__handshake_received = True
                    self.__socket.put(bytes([MAGIC_HANDSHAKE_CLIENT]))
                    self.__transactions.send_transaction(bytes([TRANSACT_REQ_UUID])).then(self.__transact_status_change)

                if not self.__handshake_received:
                    raise Exception("Invalid handshake received!")

                if d[0] == MAGIC_TRANSACT:
                    self.__transactions.received(d[1:])
                
                if d[0] == MAGIC_REQ_SUBSCRIBE:
                    s_uuid, key = segmented_bytearray.decode(d[1:])
                    self.__server._subscribe(self.__uuid, uuid.UUID(bytes=s_uuid), key)

        def __transact_status_change(self, handle : transactions.TransactionManager.OutgoingTransactionHandle):
            if handle.get_data()[0] == TRANSACT_REQ_UUID:
                if handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_NAK:
                    print("Get UUID transaction was NAK'd!")
                    self.close()

                assert handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_RET

                self.__uuid = uuid.UUID(bytes=handle.get_result())
                print(f"Got client UUID: ", self.__uuid)
                self.__transactions.send_transaction(bytes([TRANSACT_CONN_READY])).then(self.__transact_status_change)
                self.__server._got_client_uuid(self)

            if handle.get_data()[0] == TRANSACT_CONN_READY:
                if handle.get_state() != transactions.TransactionManager.OutgoingTransactionHandle.STATE_RET:
                    print("Ready transaction was NAK'd!")
                    self.close()


        def __receive_transact(self):
            t = self.__transactions.get_incoming()

            if t.get_data()[0] == TRANSACT_REG_SUBSYSTEM:
                ok = self.__server._register_subsystem(self.__uuid, SubsystemInfo.decode(t.get_data()[1:]))

                if ok:
                    t.ret(bytes())
                else:
                    t.nak()

            if t.get_data()[0] == TRANSACT_SET_KV:
                (t_uuid, s_uuid, t_k, t_v) = segmented_bytearray.decode(t.get_data()[1:])

                self.__server._set_kv(t, uuid.UUID(bytes=s_uuid), uuid.UUID(bytes=t_uuid), t_k, t_v)

            if t.get_data()[0] == TRANSACT_GET_KV:
                (t_uuid, s_uuid, t_k) = segmented_bytearray.decode(t.get_data()[1:])

                self.__server._get_kv(t, uuid.UUID(bytes=s_uuid), uuid.UUID(bytes=t_uuid), t_k)

        def __flush_transponder(self):
            while not self.__transactions_msg_out_queue.empty():
                m = self.__transactions_msg_out_queue.get()

                to_send = bytes()
                to_send += bytes([MAGIC_TRANSACT])
                to_send += m

                self.__socket.put(to_send)

        def get_transactions(self):
            return self.__transactions

        def __connected(self):
            pass

        def get_uuid(self):
            return self.__uuid
        
        def close(self):
            self.__daemon.stop()
            self.__socket.close()

        def closed(self):
            return self.__socket.is_closed()
        
        def ok(self):
            return not self.__socket.is_closed() and self.__daemon.is_alive()
            #self.__socket.put(bytes([MAGIC_HANDSHAKE_SERVER]))

        def _on_subscription_update(self, s_uuid : uuid.UUID, key : bytes, value : bytes):
            self.__socket.put(bytes([MAGIC_SUBSCRIBED_UPD]) + segmented_bytearray.encode([s_uuid.bytes, key, value]))

        
    class _SubsystemClient:
        def __init__(self, info: "SubsystemInfo"):
            self.__info = info

            self.__client = None

            self.__kv_store = dict()
            self.__kv_subscribers = dict()

        def bind_client(self, client : "ControlServer._ClientConnection"):
            if self.__client is not None and self.__client.ok():
                return False
            
            self.__client = client
            return True
        
        def on_set_kv_request(self, r_uuid : uuid.UUID, t : transactions.TransactionManager.IncomingTransactionHandle, key: bytes, val: bytes):
            if r_uuid == self.__info.get_uuid():
                self.__kv_store[key] = val
                
                if self.__kv_subscribers.get(key) is not None:
                    for s in self.__kv_subscribers[key]:
                        if s.closed():
                            print("Unsubscribing", s.get_uuid(), "from", self.__info.get_name(), ":", key)
                            self.__kv_subscribers[key].remove(s)

                        s._on_subscription_update(self.__info.get_uuid(), key, val)

                t.ret(bytes([KV_STATE_OK]))
                return
            
            if self.__client is None:
                t.ret(bytes([KV_STATE_REJ]) + b"Subsystem client is disconnected")
                return

            self.__client.get_transactions().send_transaction(bytes([TRANSACT_RSET_KV]) + segmented_bytearray.encode([self.get_uuid().bytes, key, val])).then(self.__on_set_kv_returned, [t])
    
        def __on_set_kv_returned(self, t : transactions.TransactionManager.IncomingTransactionHandle, handle : transactions.TransactionManager.OutgoingTransactionHandle):
            if handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_NAK:
                t.ret(bytes([KV_STATE_REJ]) + b"Transaction rejected")
                return

            t.ret(handle.get_result())    

        def on_get_kv_request(self, r_uuid : uuid.UUID, t : transactions.TransactionManager.IncomingTransactionHandle, key: bytes):
            cached = self.__kv_store.get(key)
            if cached is not None:
                t.ret(bytes([KV_STATE_OK]) + cached)
                return
            
            if self.__client is None:
                t.ret(bytes([KV_STATE_REJ]) + b"Subsystem client is disconnected")
                return
            
            self.__client.get_transactions().send_transaction(bytes([TRANSACT_RGET_KV]) + segmented_bytearray.encode([self.get_uuid().bytes, key])).then(self.__on_get_kv_returned, [t])
    
        def __on_get_kv_returned(self, t : transactions.TransactionManager.IncomingTransactionHandle, handle : transactions.TransactionManager.OutgoingTransactionHandle):
            if handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_NAK:
                t.ret(bytes([KV_STATE_REJ]) + b"Transaction rejected")
                return
            
            t.ret(handle.get_result())        
        
        def get_uuid(self):
            return self.__info.get_uuid()
        
        def get_client_uuid(self):
            if self.__client is None:
                return None
            
            return self.__client.get_uuid()
        
        def _subscribe(self, client, key):
            if self.__kv_subscribers.get(key) is None:
                self.__kv_subscribers[key] = []
            
            if self.__kv_subscribers[key].count(client) == 0:
                self.__kv_subscribers[key].append(client)

    def __init__(self):
        self.__client_queue = queue.Queue()
        self.__server = tcp.TCPServer(("0.0.0.0", SERVER_PORT), self.__client_queue)

        self.__clients = []

        self.__subsystems = dict()
        self.__clients_uuid = dict()

        self.__pending_subscribers = []

        self.__event_consumer = mt_events.EventConsumer()

        self.__server.on_connected(self.__event_consumer, self.__E_ON_CLIENT_CONNECT)
        self.__server.on_disconnected(self.__event_consumer, self.__E_ON_CLIENT_DISCONNECT)

        self.__daemon = daemon.Daemon()
        self.__daemon.add(self.__client_upd_thread)

    def start(self):
        self.__server.start()
        self.__daemon.start()

    def __new_client(self):
        while not self.__client_queue.empty():
            sock = self.__client_queue.get()
            client = self._ClientConnection(sock, self)
            self.__clients.append(client)

    def __disconnected_client(self):
        for client in self.__clients:
            if client.closed():
                print("Removing: ", client.get_uuid())
                self.__clients.remove(client)
                self.__clients_uuid.pop(client.get_uuid())

                for s in self.__subsystems.values():
                    if s.get_client_uuid() == client.get_uuid():
                        print("Subsystem ", s.get_uuid(), " has lost connection")
                        s.bind_client(None)
                
                break

    def __client_upd_thread(self, stop_flag : daemon.StopFlag):
        while stop_flag.run():
            e = self.__event_consumer.get()
            
            if e == self.__E_ON_CLIENT_CONNECT:
                self.__new_client()

            if e == self.__E_ON_CLIENT_DISCONNECT:
                self.__disconnected_client()

    def _got_client_uuid(self, client : "ControlServer._ClientConnection"):
        self.__clients_uuid[client.get_uuid()] = client

    def _register_subsystem(self, c_uuid : uuid.UUID, s_info : SubsystemInfo):
        subsystem = self.__subsystems.get(s_info.get_uuid())

        if subsystem is None:
            self.__subsystems[s_info.get_uuid()] = self._SubsystemClient(s_info)
            subsystem = self.__subsystems[s_info.get_uuid()]

            for r_uuid, s_uuid, key in self.__pending_subscribers:
                if s_uuid == s_info.get_uuid():
                    self._subscribe(r_uuid, s_uuid, key)
                    self.__pending_subscribers.remove((r_uuid, s_uuid, key))

            print(f"Registered subsystem: {s_info.get_name()}({s_info.get_uuid()})")


        ok = subsystem.bind_client(self.__clients_uuid[c_uuid])
        if ok:
            print(f"Bound subsystem: {s_info.get_name()}({s_info.get_uuid()}) to client {c_uuid}")

        return ok
    
    def _set_kv(self, t : transactions.TransactionManager.IncomingTransactionHandle, r_uuid : uuid.UUID, t_uuid : uuid.UUID, key: bytes, val: bytes):
        s = self.__subsystems.get(t_uuid)

        #print("Set KV From ", r_uuid, " to ", t_uuid, " key: ", key, " value: ", val)

        if s == None:
            t.ret(bytes([KV_STATE_REJ]) + b"Target subsystem not found")
            return

        s.on_set_kv_request(r_uuid, t, key, val)

    def _get_kv(self, t : transactions.TransactionManager.IncomingTransactionHandle, r_uuid : uuid.UUID, t_uuid : uuid.UUID, key: bytes):
        s = self.__subsystems.get(t_uuid)

        #print("Get KV From ", r_uuid, " to ", t_uuid, " key: ", key)

        if s == None:
            t.ret(bytes([KV_STATE_REJ]) + b"Target subsystem not found")
            return

        s.on_get_kv_request(r_uuid, t, key)

    def ok(self):
        return self.__daemon.is_alive()
    
    def _subscribe(self, r_uuid: uuid.UUID, s_uuid: uuid.UUID, key: bytes):
        s = self.__subsystems.get(s_uuid)


        if s is None:
            self.__pending_subscribers.append((r_uuid, s_uuid, key))
            return
        
        s._subscribe(self.__clients_uuid[r_uuid], key)