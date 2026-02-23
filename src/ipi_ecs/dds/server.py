import sys
import os
import queue
import time
import uuid
import segment_bytes
import mt_events
import traceback

import ipi_ecs.core.tcp as tcp
import ipi_ecs.core.daemon as daemon
import ipi_ecs.core.transactions as transactions
from ipi_ecs.dds.subsystem import SubsystemInfo, StatusItem, SubsystemStatus
from ipi_ecs.dds.magics import *
from ipi_ecs.logging.client import LogClient


# pylint: disable=line-too-long
# pylint: disable=missing-function-docstring, missing-class-docstring, trailing-whitespace
# pylint: disable=unbalanced-tuple-unpacking
# pylint: disable=unused-private-member

ENV_DDS_PORT = "IPI_ECS_DDS_PORT"

class _DDSServer:
    class _ClientConnection:
        def __init__(self, sock: tcp.TCPServerSocket, server: "_DDSServer", logger : LogClient | None = None):
            self.__socket = sock
            self.__server = server
            self.__logger = logger

            self.__event_consumer = mt_events.EventConsumer()
            
            self.__transactions_msg_out_queue = queue.Queue()
            self.__transactions = transactions.TransactionManager(self.__transactions_msg_out_queue)

            self.__E_MESSAGE = self.__socket.on_receive().bind(self.__event_consumer)
            self.__E_CLOSED = self.__socket.on_close().bind(self.__event_consumer)
            self.__E_CONNECTED = self.__socket.on_connect().bind(self.__event_consumer)
            self.__E_TRANSACT_DATA_AVAIL = self.__transactions.on_send_data().bind(self.__event_consumer)
            self.__E_NEW_TRANSACT = self.__transactions.on_receive_transaction().bind(self.__event_consumer)

            self.__handshake_received = False
            self.__uuid = uuid.UUID(bytes=bytes(16))

            self.__daemon = daemon.Daemon(exception_handler=server.handle_exception)
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
            # pylint: disable=unbalanced-tuple-unpacking
            while not self.__socket.empty():
                d = self.__socket.get(timeout=1)

                try:

                    if len(d) == 0:
                        continue

                    if d == bytes([MAGIC_HANDSHAKE_CLIENT]):
                        if self.__handshake_received:
                            raise IOError("Handshake on existing connection!")

                        #print("Handshake received from ", self.__socket.remote())
                        self.__handshake_received = True
                        self.__socket.put(bytes([MAGIC_HANDSHAKE_CLIENT]))
                        self.__transactions.send_transaction(bytes([TRANSACT_REQ_UUID])).then(self.__transact_status_change)

                    if not self.__handshake_received:
                        raise IOError("Invalid handshake received!")

                    elif d[0] == MAGIC_TRANSACT:
                        self.__transactions.received(d[1:])
                    
                    elif d[0] == MAGIC_REQ_SUBSCRIBE:
                        s_uuid, key = segment_bytes.decode(d[1:])
                        self.__server.subscribe(self.__uuid, uuid.UUID(bytes=s_uuid), key)

                    elif d[0] == MAGIC_UPDATE_STATUS_ITEM:
                        b_s_uuid, b_status = segment_bytes.decode(d[1:])
                        status = StatusItem.decode(b_status)
                        s_uuid = uuid.UUID(bytes=b_s_uuid)

                        s = self.__server.find_subsystem(s_uuid=s_uuid)
                        s.add_status_item(status)

                    elif d[0] == MAGIC_CLEAR_STATUS_ITEM:
                        b_s_uuid, b_status = segment_bytes.decode(d[1:])
                        status = int.from_bytes(b_status, byteorder="big")
                        s_uuid = uuid.UUID(bytes=b_s_uuid)

                        s = self.__server.find_subsystem(s_uuid=s_uuid)
                        s.clear_status_item(status)

                    elif d[0] == MAGIC_EVENT_FEEDBACK:
                        b_s_uuid, b_e_uuid, b_status, b_value = segment_bytes.decode(d[1:])
                        s_uuid = uuid.UUID(bytes=b_s_uuid)
                        e_uuid = uuid.UUID(bytes=b_e_uuid)
                        status = int.from_bytes(b_status, byteorder="big")
                        value = b_value
                        self.__server.event_returned(s_uuid, e_uuid, status, value)
                except Exception as e:
                    self.__logger.log(f"Exception while trying to decode client message: {d}", level="ERROR", l_type="SW", subsystem="DDS Server")
                    self.__server.handle_exception(e)
                    pass
                

        def __transact_status_change(self, handle : transactions.TransactionManager.OutgoingTransactionHandle):
            if handle.get_data()[0] == TRANSACT_REQ_UUID:
                if handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_NAK:
                    #print("Get UUID transaction was NAK'd!")
                    self.close()

                assert handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_RET

                self.__uuid = uuid.UUID(bytes=handle.get_result())
                #print(f"Got client UUID: ", self.__uuid)
                self.__transactions.send_transaction(bytes([TRANSACT_CONN_READY])).then(self.__transact_status_change)
                self.__server.got_client_uuid(self)

            if handle.get_data()[0] == TRANSACT_CONN_READY:
                if handle.get_state() != transactions.TransactionManager.OutgoingTransactionHandle.STATE_RET:
                    #print("Ready transaction was NAK'd!")
                    self.close()


        def __receive_transact(self):
            # pylint: disable=unbalanced-tuple-unpacking
            t = self.__transactions.get_incoming()

            try:

                if t.get_data()[0] == TRANSACT_REG_SUBSYSTEM:
                    ok = self.__server.register_subsystem(self.__uuid, SubsystemInfo.decode(t.get_data()[1:]))

                    if ok:
                        t.ret(bytes())
                    else:
                        t.nak()
                    return

                elif t.get_data()[0] == TRANSACT_SET_KV:
                    (t_uuid, s_uuid, t_k, t_v) = segment_bytes.decode(t.get_data()[1:])

                    self.__server.set_kv(t, uuid.UUID(bytes=s_uuid), uuid.UUID(bytes=t_uuid), t_k, t_v)
                    return

                elif t.get_data()[0] == TRANSACT_GET_KV:
                    (t_uuid, s_uuid, t_k) = segment_bytes.decode(t.get_data()[1:])

                    self.__server.get_kv(t, uuid.UUID(bytes=s_uuid), uuid.UUID(bytes=t_uuid), t_k)
                    return

                elif t.get_data()[0] == TRANSACT_GET_KV_DESC:
                    (t_uuid, s_uuid, t_k) = segment_bytes.decode(t.get_data()[1:])

                    self.__server.get_kv_desc(t, uuid.UUID(bytes=s_uuid), uuid.UUID(bytes=t_uuid), t_k)
                    return

                elif t.get_data()[0] == TRANSACT_RESOLVE:
                    t_name, = segment_bytes.decode(t.get_data()[1:])

                    s = self.__server.find_subsystem(name=t_name)
                    if s is None:
                        t.ret(bytes([TRANSOP_STATE_REJ]) + E_SUBSYSTEM_NOT_FOUND)
                        return
                    
                    t.ret(bytes([TRANSOP_STATE_OK]) + s.get_uuid().bytes)
                    return

                elif t.get_data()[0] == TRANSACT_GET_SUBSYSTEM:
                    t_uuid, = segment_bytes.decode(t.get_data()[1:])

                    s = self.__server.find_subsystem(s_uuid=uuid.UUID(bytes=t_uuid))
                    if s is None:
                        t.ret(bytes([TRANSOP_STATE_REJ]) + E_SUBSYSTEM_NOT_FOUND)
                        return
                    
                    t.ret(bytes([TRANSOP_STATE_OK]) + s.get_info().encode())
                    return
                
                elif t.get_data()[0] == TRANSACT_CALL_EVENT:
                    b_t_uuids, b_s_uuid, name, param = segment_bytes.decode(t.get_data()[1:])

                    t_uuids = []
                    for t_uuid in segment_bytes.decode(b_t_uuids):
                        t_uuids.append(uuid.UUID(bytes=t_uuid))

                    self.__server.call_event(t, uuid.UUID(bytes=b_s_uuid), t_uuids, name, param)
                    return
                
                elif t.get_data()[0] == TRANSACT_GET_STATUS:
                    b_s_uuid, = segment_bytes.decode(t.get_data()[1:])
                    s_uuid = uuid.UUID(bytes=b_s_uuid)
                    s = self.__server.find_subsystem(s_uuid=s_uuid)

                    if s is None:
                        t.ret(bytes([TRANSOP_STATE_REJ]) + E_SUBSYSTEM_NOT_FOUND)
                        return

                    t.ret(bytes([TRANSOP_STATE_OK]) + s.get_status().encode())
                    return
                
                else:
                    t.nak()
            except Exception as e:
                self.__l
                self.__server.handle_exception(e)
                t.nak()
                pass

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
        
        def is_shutdown(self):
            return self.__socket.is_shutdown()
        
        def ok(self):
            return not self.__socket.is_closed() and self.__daemon.is_alive()
            #self.__socket.put(bytes([MAGIC_HANDSHAKE_SERVER]))

        def on_subscription_update(self, s_uuid : uuid.UUID, key : bytes, value : bytes):
            self.__socket.put(bytes([MAGIC_SUBSCRIBED_UPD]) + segment_bytes.encode([s_uuid.bytes, key, value]))

        def on_system_update(self, data : bytes):
            self.__socket.put(bytes([MAGIC_SYSTEM_UPD]) + data)

        def get_server(self):
            return self.__server
        
        def send(self, data: bytes):
            self.__socket.put(data)

        
    class _SubsystemClient:
        def __init__(self, info: "SubsystemInfo"):
            self.__info = info

            self.__client = None

            self.__kv_store = dict()
            self.__kv_subscribers = dict()

            self.__active_status_items = dict()

        def bind_client(self, client : "_DDSServer._ClientConnection"):            
            if self.__client is not None and self.__client.ok(): #  and self.__client.get_uuid() != client.get_uuid()
                return False
            
            self.__client = client
            self.__reset_cache()
            return True
        
        def __reset_cache(self):
            self.__kv_store.clear()
            self.__active_status_items.clear()
        
        def update_info(self, info: SubsystemInfo):
            self.__info = info

        def on_set_kv_request(self, r_uuid : uuid.UUID, t : transactions.TransactionManager.IncomingTransactionHandle, key: bytes, val: bytes):
            if r_uuid == self.__info.get_uuid():
                self.__kv_store[key] = val
                
                if self.__kv_subscribers.get(key) is not None:
                    for s in self.__kv_subscribers[key]:
                        if s.closed():
                            #print("Unsubscribing", s.get_uuid(), "from", self.__info.get_name(), ":", key)
                            self.__kv_subscribers[key].remove(s)

                        s.on_subscription_update(self.__info.get_uuid(), key, val)

                t.ret(bytes([TRANSOP_STATE_OK]))
                return
            
            if self.__client is None:
                t.ret(bytes([TRANSOP_STATE_REJ]) + E_SUBSYSTEM_DISCONNECTED)
                return

            self.__outgoing_transop(t, bytes([TRANSACT_RSET_KV]) + segment_bytes.encode([self.get_uuid().bytes, r_uuid.bytes, key, val]))

        def on_get_kv_request(self, r_uuid : uuid.UUID, t : transactions.TransactionManager.IncomingTransactionHandle, key: bytes):
            cached = self.__kv_store.get(key)

            if cached is not None:
                t.ret(bytes([TRANSOP_STATE_OK]) + cached)
                return
            
            self.__outgoing_transop(t, bytes([TRANSACT_RGET_KV]) + segment_bytes.encode([self.get_uuid().bytes, r_uuid.bytes, key]))

        def on_get_kv_desc_request(self, r_uuid : uuid.UUID, t : transactions.TransactionManager.IncomingTransactionHandle, key: bytes):
            self.__outgoing_transop(t, bytes([TRANSACT_RGET_KV_DESC]) + segment_bytes.encode([self.get_uuid().bytes, r_uuid.bytes, key]))

        def __outgoing_transop_returned(self, t : transactions.TransactionManager.IncomingTransactionHandle, handle : transactions.TransactionManager.OutgoingTransactionHandle):
            if handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_NAK:
                t.ret(bytes([TRANSOP_STATE_REJ]) + E_TRANSOP_TRANSACTIPN_REJ)
                return
            
            t.ret(handle.get_result())

        def __outgoing_transop(self, t: transactions.TransactionManager.IncomingTransactionHandle, data: bytes):
            if self.__client is None:
                t.ret(bytes([TRANSOP_STATE_REJ]) + E_SUBSYSTEM_DISCONNECTED)
                return
            
            self.__client.get_transactions().send_transaction(data).then(self.__outgoing_transop_returned, [t])
        
        def get_uuid(self):
            return self.__info.get_uuid()
        
        def get_info(self):
            return self.__info
        
        def get_client_uuid(self):
            if self.__client is None:
                return None
            
            return self.__client.get_uuid()
        
        def subscribe(self, client, key):
            if self.__kv_subscribers.get(key) is None:
                self.__kv_subscribers[key] = []
            
            if self.__kv_subscribers[key].count(client) == 0:
                self.__kv_subscribers[key].append(client)

        def ok(self):
            return self.__client is not None
        
        def send_event(self, s_uuid: uuid.UUID, e_uuid: uuid.UUID, name: bytes, param: bytes):
            if self.__client is None:
                return False
            
            def then(handle : transactions.TransactionManager.OutgoingTransactionHandle):
                if handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_RET:
                    self.__client.get_server().event_returned(self.get_uuid(), e_uuid, handle.get_result()[0], handle.get_result()[1:])
                elif handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_ACK:
                    self.__client.get_server().event_returned(self.get_uuid(), e_uuid, EVENT_IN_PROGRESS, bytes())
                elif handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_NAK:
                    self.__client.get_server().event_returned(self.get_uuid(), e_uuid, EVENT_REJ, bytes())

            self.__client.get_transactions().send_transaction(bytes([TRANSACT_RCALL_EVENT]) + segment_bytes.encode([self.get_uuid().bytes, s_uuid.bytes, e_uuid.bytes, name, param])).then(then)
            return True
        
        def on_event_return(self, e_uuid: uuid.UUID, s_uuid: uuid.UUID, status: int, value: bytes):
            if self.__client is None:
                return
            
            self.__client.send(bytes([MAGIC_EVENT_RET]) + segment_bytes.encode([self.get_uuid().bytes, s_uuid.bytes, e_uuid.bytes, status.to_bytes(length=1, byteorder="big"), value]))

        def add_status_item(self, status : StatusItem):
            self.__active_status_items[status.get_code()] = status

        def clear_status_item(self, status : int):
            self.__active_status_items.pop(status).get_message()

        def reset_status_items(self):
            self.__active_status_items.clear()

        def get_status(self):
            state = SubsystemStatus.STATE_ALIVE if self.ok() else SubsystemStatus.STATE_DISCONNECTED

            return SubsystemStatus(state, self.__active_status_items.values())


    class ServerHandle:
        def __init__(self, server: "_DDSServer"):
            self.__server = server

        def start(self):
            self.__server.start()

        def close(self):
            self.__server.close()

        def ok(self):
            return self.__server.ok()
        
    def __init__(self, host = "0.0.0.0", port = None, logger : LogClient | None = None):
        self.__client_queue = queue.Queue()
        
        if port is None:
            port = os.environ.get("ECS_PORT")

        if port is None:
            port = SERVER_PORT
        
        self.__logger = logger

        self.__server = tcp.TCPServer((host, port), self.__client_queue)
        
        self.__log(f"Binding {host}:{port}", level="DEBUG")

        self.__clients = []

        self.__subsystems = dict()
        self.__clients_uuid = dict()

        self.__in_progress_events = dict()

        self.__pending_subscribers = []

        self.__event_consumer = mt_events.EventConsumer()

        self.__E_ON_CLIENT_CONNECT = self.__server.on_connected().bind(self.__event_consumer)
        self.__E_ON_CLIENT_DISCONNECT = self.__server.on_disconnected().bind(self.__event_consumer)

        self.__daemon = daemon.Daemon(exception_handler=self.handle_exception)
        self.__daemon.add(self.__client_upd_thread)

    def start(self):
        self.__server.start()
        self.__daemon.start()

    def __new_client(self):
        while not self.__client_queue.empty():
            sock = self.__client_queue.get()
            client = self._ClientConnection(sock, self, self.__logger)
            self.__log(f"Client {client.get_uuid()} has connected", level="DEBUG", event="CONN")
            self.__clients.append(client)

    def __disconnected_client(self):
        for client in self.__clients:
            if client.closed():
                if not client.is_shutdown():
                    self.__log(f"Client {client.get_uuid()} has abruptly closed the connection", level="WARN", event="CONN")
                
                self.__clients.remove(client)

                if client.get_uuid() == uuid.UUID(bytes=bytes(16)):
                    #print("Client disconnected before config was finished!")
                    continue

                self.__clients_uuid.pop(client.get_uuid())

                removed = True
                while removed:
                    removed = False

                    for s in self.__subsystems.values():
                        if s.get_client_uuid() == client.get_uuid():
                            self.__log(f"Subsystem {s.get_info().get_name()} has disconnected", level="DEBUG", event="CONN")

                            s.bind_client(None)

                            if s.get_info().get_temporary():
                                self.__log(f"Temporary subsystem {s.get_info().get_name()} has been removed.", level="DEBUG", event="CONN")
                                self.__subsystems.pop(s.get_uuid())
                            
                                removed = True
                                break
                
                break

        self._send_subsystems()

    def __client_upd_thread(self, stop_flag : daemon.StopFlag):
        while stop_flag.run():
            e = self.__event_consumer.get()

            if e == self.__E_ON_CLIENT_CONNECT:
                self.__new_client()

            if e == self.__E_ON_CLIENT_DISCONNECT:
                self.__disconnected_client()

    def got_client_uuid(self, client : "_DDSServer._ClientConnection"):
        self.__log(f"Client {client.get_uuid()} has connected", level="DEBUG", event="CONN")

        self.__clients_uuid[client.get_uuid()] = client

    def register_subsystem(self, c_uuid : uuid.UUID, s_info : SubsystemInfo):
        subsystem = self.find_subsystem(s_uuid=s_info.get_uuid())

        if subsystem is None:
            self.__subsystems[s_info.get_uuid()] = self._SubsystemClient(s_info)
            subsystem = self.__subsystems[s_info.get_uuid()]

            for r_uuid, s_uuid, key in self.__pending_subscribers:
                if s_uuid == s_info.get_uuid():
                    self.subscribe(r_uuid, s_uuid, key)
                    self.__pending_subscribers.remove((r_uuid, s_uuid, key))

            self.__log(f"Registered subsystem: {s_info.get_name()}({s_info.get_uuid()})", level="DEBUG")
            #print(f"Registered subsystem: {s_info.get_name()}({s_info.get_uuid()})")

            #if s_info.get_temporary():
            #    print("Subsystem is TEMPORARY. It will be removed once it's client disconnects!")

        if subsystem.get_client_uuid() == c_uuid:
            subsystem.update_info(s_info)
            return True
        
        ok = subsystem.bind_client(self.__clients_uuid[c_uuid])

        self._send_subsystems()
        if ok:
            self.__log(f"Subsystem {s_info.get_name()} has connected", level="DEBUG", event="CONN")
            self.__log(f"Bound subsystem: {s_info.get_name()}({s_info.get_uuid()}) to client {c_uuid}", level="DEBUG")

        return ok
    
    def set_kv(self, t : transactions.TransactionManager.IncomingTransactionHandle, r_uuid : uuid.UUID, t_uuid : uuid.UUID, key: bytes, val: bytes):
        s = self.__subsystems.get(t_uuid)

        #print("Set KV From ", r_uuid, " to ", t_uuid, " key: ", key, " value: ", val)

        if s is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + E_SUBSYSTEM_NOT_FOUND)
            return

        s.on_set_kv_request(r_uuid, t, key, val)

    def get_kv(self, t : transactions.TransactionManager.IncomingTransactionHandle, r_uuid : uuid.UUID, t_uuid : uuid.UUID, key: bytes):
        s = self.__subsystems.get(t_uuid)

        #print("Get KV From ", r_uuid, " to ", t_uuid, " key: ", key)

        if s is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + E_SUBSYSTEM_NOT_FOUND)
            return

        s.on_get_kv_request(r_uuid, t, key)

    def get_kv_desc(self, t : transactions.TransactionManager.IncomingTransactionHandle, r_uuid : uuid.UUID, t_uuid : uuid.UUID, key: bytes):
        s = self.__subsystems.get(t_uuid)

        if s is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + E_SUBSYSTEM_NOT_FOUND)
            return

        s.on_get_kv_desc_request(r_uuid, t, key)

    def call_event(self, t : transactions.TransactionManager.IncomingTransactionHandle, s_uuid :uuid.UUID, t_uuids: list, name: bytes, param: bytes):
        subsystems = []

        if len(t_uuids) > 0:
            for t_uuid in t_uuids:
                s = self.__subsystems.get(t_uuid)

                if s is None:
                    t.ret(bytes([TRANSOP_STATE_REJ]) + E_SUBSYSTEMS_NOT_FOUND)
                    return
                
                subsystems.append(s)
        else:
            subsystems = self.__subsystems.values()

        e_uuid = uuid.uuid4()
        sent_status = []

        for s in subsystems:
            ok = s.send_event(s_uuid, e_uuid, name, param)

            sent_status.append(segment_bytes.encode([s.get_uuid().bytes, ok.to_bytes(byteorder="big", length=1)]))

        self.__in_progress_events[e_uuid] = (name, s_uuid)
        t.ret(bytes([TRANSOP_STATE_OK]) + segment_bytes.encode([e_uuid.bytes, segment_bytes.encode(sent_status)]))

    def event_returned(self, s_uuid: uuid.UUID, e_uuid: uuid.UUID, state: int, value: bytes):
        e = self.__in_progress_events.get(e_uuid)
        if e is None:
            self.__log(f"Received event return for event that does not exist: {e_uuid}", level="ERROR")
            return
        
        _, r_uuid = e
        s = self.find_subsystem(s_uuid=r_uuid)

        if s is None:
            self.__log(f"Received event return for event sent by subsystem that does not exist: {r_uuid}", level="ERROR")
            return
        
        s.on_event_return(e_uuid, s_uuid, state, value)

    def ok(self):
        return self.__daemon.is_alive()
    
    def subscribe(self, r_uuid: uuid.UUID, s_uuid: uuid.UUID, key: bytes):
        s = self.find_subsystem(s_uuid=s_uuid)
        r = self.__clients_uuid.get(r_uuid)

        if r is None:
            self.__log(f"Target publisher {r_uuid} to add subscriber not found, who are you?!", level="ERROR")
            return

        if s is None:
            self.__pending_subscribers.append((r_uuid, s_uuid, key))
            return
        
        s.subscribe(self.__clients_uuid[r_uuid], key)

    def _send_subsystems(self):
        subsystem_infos = []

        for s in self.__subsystems.values():
            subsystem_infos.append(segment_bytes.encode([s.get_info().encode(), s.get_status().encode()]))

        data = segment_bytes.encode(subsystem_infos)
        for c in self.__clients:
            c.on_system_update(data)

    def close(self):
        self.__daemon.stop()
        self.__server.close()

    def find_subsystem(self, name =  None, s_uuid = None) -> _SubsystemClient:
        if name is not None:
            for s in self.__subsystems.values():
                if s.get_info().get_name() == name.decode("utf-8"):
                    return s
            return None
        
        if s_uuid is not None:
            return self.__subsystems.get(s_uuid)
        
    def handle_exception(self, e: Exception):
        self.__log("Caught exception on daemon thread!", level="ERROR")
        for line in traceback.format_exception(None, e, e.__traceback__):
            for split in line.split('\n'):
                self.__log(split, level="ERROR")
        
    def __log(self, msg, level = "INFO", **data):
        if self.__logger is None:
            print(level, msg)
            return
        
        self.__logger.log(msg, level=level, l_type="SW", subsystem="DDS Server", **data)

def get_server(host, port, logger = None):
    return _DDSServer.ServerHandle(_DDSServer(host, port, logger))
