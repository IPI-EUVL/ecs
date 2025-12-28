import queue
import time
import uuid

import ipi_ecs.core.tcp as tcp
import ipi_ecs.core.daemon as daemon
import ipi_ecs.core.mt_events as mt_events
import ipi_ecs.core.transactions as transactions
import ipi_ecs.core.segmented_bytearray as segmented_bytearray

from ipi_ecs.control.subsystem import SubsystemInfo
from ipi_ecs.control.types import PropertyTypeSpecifier, ByteTypeSpecifier
from ipi_ecs.control.magics import *

class SubsystemHandle:
    def __init__(self, subsystem: "ControlServerClient._RegisteredSubsystem"):
        self.__subsystem = subsystem

    def get_info(self):
        return self.__subsystem.get_info()
    
    def get_state(self):
        return self.__subsystem.get_state()
    
    def add_kv(self, key : bytes, writable = True, readable = True, cache = False):
        return self.__subsystem.add_kv(key, writable, readable, cache)
    
    def add_remote_kv(self, key : bytes, s_uuid : uuid.UUID, subscribe = False):
        return self.__subsystem.add_remote_kv(s_uuid, key, subscribe)
    
    def get_kv(self, target_uuid : uuid.UUID, key : bytes, ret = KVP_RET_AWAIT):
        return self.__subsystem.get_kv(target_uuid, key, ret)
            
    def set_kv(self, target_uuid : uuid.UUID, key : bytes, val: bytes, ret = KVP_RET_AWAIT):
        return self.__subsystem.set_kv(target_uuid, key, val, ret)

class SubsystemInfo:
    def __init__(self, s_uuid: uuid.UUID, name : str):
        self.__uuid = s_uuid
        self.__name = name
    
    def get_uuid(self):
        return self.__uuid
    
    def get_name(self):
        return self.__name
    
    def encode(self):
        b = bytes()
        b += self.__uuid.bytes
        b += self.__name.encode("utf-8")
        return b
    
    def decode(d_bytes : bytes):
        s_uuid = uuid.UUID(bytes=d_bytes[:16])
        name = d_bytes[16:].decode("utf-8")

        return SubsystemInfo(s_uuid, name)
    
class _LocalProperty:
    class __PropertyHandler:
        def __init__(self, provider : "_LocalProperty"):
            self.__property = provider

        def __write(self, value):
            return self.__property.handle_set_value(value)

        def __read(self):
            return self.__property.handle_get_value()
        
        def __del(self): 
            return
        
        def set_type(self, p_type : PropertyTypeSpecifier):
            self.__property.set_type(p_type)

        value = property(__read, __write, __del)
    def __init__(self, key : str, subsystem: "ControlServerClient._RegisteredSubsystem", write = True, read = True, send = False):
        self.__key = key
        self.__writable = write
        self.__readable = read
        self.__subsystem = subsystem
        self.__send = send

        self.__p_type = ByteTypeSpecifier()

        self.__property_handler = self.__PropertyHandler(self)

        if send: # Cacned values are read-only
            self.__writable = False

        self.__value = None

    def remote_set(self, value : bytes):
        if not self.__writable:
            return (KV_STATE_REJ, b"Value is read-only")
        
        try:
            self.__p_type.parse(value)
        except ValueError:
            return (KV_STATE_REJ, b"Value is not valid for property type")
        
        self.__value = value
        return (KV_STATE_OK, bytes())

    def remote_get(self):
        if not self.__readable:
            return (KV_STATE_REJ, b"Value is write-only")
        
        if self.__value is None:
            return (KV_STATE_REJ, b"Value has not been set yet!")
        
        return (KV_STATE_OK, self.__value)
    
    def handle_set_value(self, value):
        encoded = None
        try:
            encoded = self.__p_type.encode(value)
        except ValueError:
            raise ValueError("Property type is incompatible with provided value")
        
        self.__value = encoded
        print(value)

        if self.__send:
            self.__subsystem.get_client()._set_kv_handle(self.__key, self.__value, self.__subsystem.get_uuid(), self.__subsystem.get_uuid())

    def handle_get_value(self, p_type : PropertyTypeSpecifier):
        return self.__p_type.parse(self.__value)
    
    def get_handle(self):
        return self.__property_handler
    
    def set_type(self, p_type : PropertyTypeSpecifier):
        self.__p_type = p_type

    def get_type_descriptor(self):
        return self.__p_type.encode_type()
    
class _RemoteProperty:
    class __PropertyHandler:
        def __init__(self, provider : "_RemoteProperty"):
            self.__property = provider

        def __write(self, value):
            return self.__property.handle_set_value(value)

        def __read(self):
            return self.__property.handle_get_value()
        
        def __del(self): 
            return
        
        def set_type(self, p_type : PropertyTypeSpecifier):
            self.__property.set_type(p_type)

        value = property(__read, __write, __del)
    def __init__(self, key : str, subsystem: "ControlServerClient._RegisteredSubsystem", remote : uuid.UUID, subscribe = True):
        self.__key = key
        self.__subsystem = subsystem
        self.__remote = remote
        self.__subscribe = subscribe

        self.__p_type = ByteTypeSpecifier()

        self.__property_handler = self.__PropertyHandler(self)

        self.__value = None

        if self.__subscribe:
            self.__subsystem.get_client()._add_active_subscriber(self)

    def remote_set(self, value : bytes):
        try:
            self.__p_type.parse(value)
        except ValueError:
            return
        
        self.__value = value
    
    def handle_set_value(self, value):
        encoded = None
        try:
            encoded = self.__p_type.encode(value)
        except ValueError:
            raise ValueError("Property type is incompatible with provided value")
        
        self.__subsystem.get_client()._set_kv_handle(self.__key, encoded, self.__remote, self.__subsystem.get_uuid())

    def handle_get_value(self):
        if self.__value is not None:
            return self.__p_type.parse(self.__value)
        
        if self.__subscribe:
            return None
        
        handle = self.__subsystem.get_kv(self.__remote, self.__key, KVP_RET_HANDLE)

        if handle is None:
            return None

        start = time.time()
        while handle.get_state() == KV_STATE_PENDING and time.time() - start < 1.0:
            time.sleep(0.01)

        if handle.get_state() != KV_STATE_OK:
            print("Failed to retrieve value: ", handle.get_reason())
            return None

        return self.__p_type.parse(handle.get_value())
    
    def get_handle(self):
        return self.__property_handler
    
    def set_type(self, p_type : PropertyTypeSpecifier):
        self.__p_type = p_type

    def get_type_descriptor(self):
        return self.__p_type.encode_type()
    
    def get_remote(self):
        return self.__remote
    
    def get_key(self):
        return self.__key

class ControlServerClient:
    __E_MESSAGE = 0
    __E_TRANSACT_DATA_AVAIL = 1
    __E_CONNECTED = 2
    __E_NEW_TRANSACT = 4
    __E_DISCONNECTED = 5

    REG_STATE_OK = 0
    REG_STATE_REFUSED = 1
    REG_STATE_NOT_REGISTERED = 2

    class _RegisteredSubsystem:
        def __init__(self, info: "SubsystemInfo", client: "ControlServerClient"):
            self.__info = info
            self.__client = client

            self.__kv_providers = dict()

        def get_info(self):
            return self.__info
        
        def get_state(self):
            return self.__client.get_registered()
        
        def get_handle(self):
            return SubsystemHandle(self)
        
        def get_uuid(self):
            return self.__info.get_uuid()

        def get_kvp(self, key):
            return self.__kv_providers.get(key)
        
        def get_kvps(self):
            return self.__kv_providers
        
        def add_kv(self, key : bytes, writable = True, readable = True, cache = False):
            lp = _LocalProperty(key, self, writable, readable, cache)
            self.__kv_providers[key] = lp
            return lp.get_handle()
        
        def add_remote_kv(self, t_uuid : uuid.UUID, key : bytes, subscribe = False):
            lp = _RemoteProperty(key, self, t_uuid, subscribe)
            return lp.get_handle()
        
        def get_client(self):
            return self.__client
        
        def get_kv(self, target_uuid : uuid.UUID, key : bytes, ret = KVP_RET_AWAIT):
            if ret == KVP_RET_AWAIT:
                return self.__client._get_kv_await(key, target_uuid, self.get_uuid())
            elif ret == KVP_RET_HANDLE:
                return self.__client._get_kv_handle(key, target_uuid, self.get_uuid())
            else:
                raise ValueError("Invalid return type")
            
        def set_kv(self, target_uuid : uuid.UUID, key : bytes, val: bytes, ret = KVP_RET_AWAIT):
            if ret == KVP_RET_AWAIT:
                return self.__client._set_kv_await(key, val, target_uuid, self.get_uuid())
            elif ret == KVP_RET_HANDLE:
                return self.__client._set_kv_handle(key, val, target_uuid, self.get_uuid())
            else:
                raise ValueError("Invalid return type")
            
        def get_kv_descriptor(self):
            r = []

            for (k, kvp) in self.__kv_providers.items:
                r.append(k)
                r.append(kvp.get_type_descriptor())
            
            return segmented_bytearray.encode(r)

    class __KVOpHandle:
        class _KVOpReturnHandle:
            def __init__(self, handle : "ControlServerClient.__KVOpHandle"):
                self.__handle = handle

            def get_state(self):
                return self.__handle.get_state()
            
            def get_reason(self):
                return self.__handle.get_reason()
            
            def get_value(self):
                return self.__handle.get_value()
            
        def __init__(self):
            self.__state = KV_STATE_PENDING
            self.__reason = None
            self.__value = None
            
        def set_state(self, state):
            self.__state = state 

        def set_reason(self, reason):
            self.__reason = reason 

        def set_value(self, value):
            self.__value = value

        def get_state(self):
            return self.__state
            
        def get_reason(self):
            return self.__reason
        
        def get_value(self):
            return self.__value
        
        def get_handle(self):
            return self._KVOpReturnHandle(self)

    def __init__(self, c_uuid : uuid.UUID, ip = "127.0.0.1"):
        self.__uuid = c_uuid

        self.__socket = tcp.TCPClientSocket()
        self.__socket.connect((ip, SERVER_PORT))
        print("Connecting to: ", (ip, SERVER_PORT))
        self.__socket.start()

        self.__registered = self.REG_STATE_NOT_REGISTERED
        self.__registered_awaiter = mt_events.Awaiter()
        self.__subsystem_handles = dict()
        self.__subsystem_info = []
        self.__active_subscribers = dict()

        self.__is_ready = False

        self.__transactions_msg_out_queue = queue.Queue()
        self.__transactions = transactions.TransactionManager(self.__transactions_msg_out_queue)

        self.__event_consumer = mt_events.EventConsumer()

        self.__socket.on_receive(self.__event_consumer, self.__E_MESSAGE)
        self.__socket.on_connect(self.__event_consumer, self.__E_CONNECTED)
        self.__socket.on_disconnect(self.__event_consumer, self.__E_DISCONNECTED)
        self.__transactions.on_send_data(self.__event_consumer, self.__E_TRANSACT_DATA_AVAIL)
        self.__transactions.on_receive_transaction(self.__event_consumer, self.__E_NEW_TRANSACT)

        self.__ready_event = mt_events.Event()
        self.__registered_event = mt_events.Event()

        self.__handshake_received = False

        self.__daemon = daemon.Daemon()
        self.__daemon.add(self.__thread)
        self.__daemon.start()

    def __receive(self):
        while not self.__socket.empty():
            d = self.__socket.get()

            if len(d) == 0:
                continue

            if d == bytes([MAGIC_HANDSHAKE_SERVER]):
                if self.__handshake_received:
                    raise Exception("Handshake on existing connection!")

                print("Handshake received from ", self.__socket.remote())
                self.__handshake_received = True

            if not self.__handshake_received:
                raise Exception("Invalid handshake received!")

            if d[0] == MAGIC_TRANSACT:
                self.__transactions.received(d[1:])
            if d[0] == MAGIC_SUBSCRIBED_UPD:
                s_uuid, key, val = segmented_bytearray.decode(d[1:])

                for kvs in self.__active_subscribers[uuid.UUID(bytes=s_uuid)]:
                    if kvs.get_key() == key:
                        kvs.remote_set(val)

    def __receive_transact(self):
        t = self.__transactions.get_incoming()

        if t.get_data()[0] == TRANSACT_REQ_UUID:
            t.ret(self.__uuid.bytes)
        
        if t.get_data()[0] == TRANSACT_CONN_READY:
            if self.__is_ready:
                raise Exception("Received ready transaction twice!")
            
            self.__ready()
            t.ret(self.__uuid.bytes)

        if t.get_data()[0] == TRANSACT_RGET_KV:
            self.__rget_kv(t)

        if t.get_data()[0] == TRANSACT_RSET_KV:
            self.__rset_kv(t)
    
    def __rget_kv(self, t: transactions.TransactionManager.IncomingTransactionHandle):
        (t_uuid, key) = segmented_bytearray.decode(t.get_data()[1:])

        t_uuid = uuid.UUID(bytes=t_uuid)

        if self.__subsystem_handles.get(t_uuid) is None:
            t.ret(bytes([KV_STATE_REJ]) + b"Specified subsystem not found.")
            return


        p = self.__subsystem_handles[t_uuid].get_kvp(key)
        if p is None:
            t.ret(bytes([KV_STATE_REJ]) + b"Specified value not found.")
            return

        state, data = p.remote_get()
        t.ret(bytes([state]) + data)

    def __rset_kv(self, t: transactions.TransactionManager.IncomingTransactionHandle):
        (t_uuid, key, value) = segmented_bytearray.decode(t.get_data()[1:])

        t_uuid = uuid.UUID(bytes=t_uuid)

        if self.__subsystem_handles.get(t_uuid) is None:
            t.ret(bytes([KV_STATE_REJ]) + b"Specified subsystem not found.")
            return

        p = self.__subsystem_handles[t_uuid].get_kvp(key)
        if p is None:
            t.ret(bytes([KV_STATE_REJ]) + b"Specified value not found.")
            return

        state, data = p.remote_set(value)
        t.ret(bytes([state]) + data)

    def __flush_transponder(self):
        while not self.__transactions_msg_out_queue.empty():
            m = self.__transactions_msg_out_queue.get()

            to_send = bytes()
            to_send += bytes([MAGIC_TRANSACT])
            to_send += m

            self.__socket.put(to_send)

    def __connected(self):
        self.__socket.put(bytes([MAGIC_HANDSHAKE_CLIENT]))

    def __disconnected(self):
        self.__handshake_received = False
        self.__is_ready = False

    def __thread(self, stop_flag : daemon.StopFlag):
        while stop_flag.run():
            e = self.__event_consumer.get()

            if e == self.__E_MESSAGE:
                self.__receive()
            elif e == self.__E_TRANSACT_DATA_AVAIL:
                self.__flush_transponder()
            elif e == self.__E_CONNECTED:
                self.__connected()
            elif e == self.__E_DISCONNECTED:
                self.__disconnected()
            elif e == self.__E_NEW_TRANSACT:
                self.__receive_transact()

    def __transact_status_change(self, handle : transactions.TransactionManager.OutgoingTransactionHandle):
        if handle.get_data()[0] == TRANSACT_REG_SUBSYSTEM:
            if handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_NAK:
                print("Could not register subsystem!")
                self.__registered = self.REG_STATE_REFUSED

            if handle.get_state() != transactions.TransactionManager.OutgoingTransactionHandle.STATE_RET:
                return
            
            self.__registered = self.REG_STATE_OK

            subsystem_handle = self._RegisteredSubsystem(SubsystemInfo.decode(handle.get_data()[1:]), self)
            print("Registered subsystem: ", subsystem_handle.get_info().get_name())

            self.__subsystem_handles[subsystem_handle.get_info().get_uuid()] = subsystem_handle

            print(self.__subsystem_handles[subsystem_handle.get_info().get_uuid()])

            self.__registered_event.call()
            self.__registered_awaiter.call(subsystem_handle.get_handle())


    def __ready(self):
        self.__ready_event.call()
        self.__is_ready = True

        if self.__subsystem_info is not None:
            self.__send_subsystem_info()

        self.__refresh_subscriptions()
        
    def close(self):
        self.__daemon.stop()
        self.__socket.close()

    def ok(self):
        return not self.__socket.is_closed() and self.__daemon.is_alive()
    
    def register_subsystem(self, s_info):
        self.__subsystem_info.append(s_info)
        return self.__registered_awaiter.get_handle()

    def _set_kv_await(self, key : str, val : bytes, t_uuid : uuid.UUID, s_uuid : uuid.UUID):
        if not self.__is_ready:
            return None
        
        ret_awaiter = mt_events.Awaiter()

        self.__transactions.send_transaction(bytes([TRANSACT_SET_KV]) + segmented_bytearray.encode([t_uuid.bytes, s_uuid.bytes, key, val])).then(self.__on_set_kv_returned_await, [ret_awaiter])
        return ret_awaiter.get_handle()
    
    def _set_kv_handle(self, key : str, val : bytes, t_uuid : uuid.UUID, s_uuid : uuid.UUID):
        if not self.__is_ready:
            return None
        
        ret_handle = self.__KVOpHandle()
        ret_handle.set_value(val)

        print(val)

        self.__transactions.send_transaction(bytes([TRANSACT_SET_KV]) + segmented_bytearray.encode([t_uuid.bytes, s_uuid.bytes, key, val])).then(self.__on_set_kv_returned_handle, [ret_handle])
        return ret_handle.get_handle()
    
    def __on_set_kv_returned_await(self, awaiter : mt_events.Awaiter, handle : transactions.TransactionManager.OutgoingTransactionHandle):
        if handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_NAK:
            print("Set KV NAK'd!!")
            awaiter.call(state=KV_STATE_REJ, reason=None)
            return

        s = KV_STATE_OK if handle.get_result()[0] == KV_STATE_OK else KV_STATE_REJ
        reason = None if s == KV_STATE_OK else handle.get_result()[1:].decode("utf-8")
        
        awaiter.call(state=s, reason=reason)
    
    def __on_set_kv_returned_handle(self, op_handle : "ControlServerClient.__KVOpHandle", handle : transactions.TransactionManager.OutgoingTransactionHandle):
        if handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_NAK:
            print("Set KV NAK'd!!")
            op_handle.set_state(KV_STATE_REJ)
            return

        s = KV_STATE_OK if handle.get_result()[0] == KV_STATE_OK else KV_STATE_REJ
        reason = None if s == KV_STATE_OK else handle.get_result()[1:].decode("utf-8")
        
        op_handle.set_state(s)
        op_handle.set_reason(reason)

    def _get_kv_await(self, key : str, t_uuid : uuid.UUID, s_uuid : uuid.UUID):
        if not self.__is_ready:
            return None
        
        ret_awaiter = mt_events.Awaiter()

        self.__transactions.send_transaction(bytes([TRANSACT_GET_KV]) + segmented_bytearray.encode([t_uuid.bytes, s_uuid.bytes, key])).then(self.__on_get_kv_returned_await, [ret_awaiter])
        return ret_awaiter.get_handle()
    
    def _get_kv_handle(self, key : str, t_uuid : uuid.UUID, s_uuid : uuid.UUID):
        if not self.__is_ready:
            return None
        
        ret_handle = self.__KVOpHandle()

        self.__transactions.send_transaction(bytes([TRANSACT_GET_KV]) + segmented_bytearray.encode([t_uuid.bytes, s_uuid.bytes, key])).then(self.__on_get_kv_returned_handle, [ret_handle])
        return ret_handle.get_handle()
    
    def __on_get_kv_returned_await(self, awaiter : mt_events.Awaiter, handle : transactions.TransactionManager.OutgoingTransactionHandle):
        if handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_NAK:
            print("Get KV NAK'd!!")
            awaiter.call(state=KV_STATE_REJ, reason=None)
            return

        s = KV_STATE_OK if handle.get_result()[0] == KV_STATE_OK else KV_STATE_REJ
        reason = None if s == KV_STATE_OK else handle.get_result()[1:].decode("utf-8")
        value = None if s != KV_STATE_OK else handle.get_result()[1:]
        
        awaiter.call(state=s, reason=reason, value=value)
    
    def __on_get_kv_returned_handle(self, op_handle : "ControlServerClient.__KVOpHandle", handle : transactions.TransactionManager.OutgoingTransactionHandle):
        if handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_NAK:
            print("Get KV NAK'd!!")
            op_handle.set_state(KV_STATE_REJ)
            return

        s = KV_STATE_OK if handle.get_result()[0] == KV_STATE_OK else KV_STATE_REJ
        reason = None if s == KV_STATE_OK else handle.get_result()[1:].decode("utf-8")
        value = None if s != KV_STATE_OK else handle.get_result()[1:]
        
        op_handle.set_state(s)
        op_handle.set_reason(reason)
        op_handle.set_value(value)
    
    def __send_subsystem_info(self):
        for info in self.__subsystem_info:
            self.__transactions.send_transaction(bytes([TRANSACT_REG_SUBSYSTEM]) + info.encode()).then(self.__transact_status_change)

    def __refresh_subscriptions(self):
        for l in self.__active_subscribers.values():
            for kv in l:
                self.__socket.put(bytes([MAGIC_REQ_SUBSCRIBE]) + segmented_bytearray.encode([kv.get_remote().bytes, kv.get_key()]))

    def _add_active_subscriber(self, kv):
        if self.__active_subscribers.get(kv.get_remote()) is None:
            self.__active_subscribers[kv.get_remote()] = []

        self.__active_subscribers[kv.get_remote()].append(kv)
        print(kv.get_remote())
        self.__socket.put(bytes([MAGIC_REQ_SUBSCRIBE]) + segmented_bytearray.encode([kv.get_remote().bytes, kv.get_key()]))


    def get_registered(self):
        return self.__registered
