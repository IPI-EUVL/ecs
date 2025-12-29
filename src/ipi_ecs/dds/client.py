import queue
import time
import uuid

import ipi_ecs.core.tcp as tcp
import ipi_ecs.core.daemon as daemon
import ipi_ecs.core.mt_events as mt_events
import ipi_ecs.core.transactions as transactions
import ipi_ecs.core.segmented_bytearray as segmented_bytearray

from ipi_ecs.dds.subsystem import SubsystemInfo
from ipi_ecs.dds.types import PropertyTypeSpecifier, ByteTypeSpecifier
from ipi_ecs.dds.magics import *

class SubsystemHandle:
    def __init__(self, subsystem: "DDSClient._RegisteredSubsystem"):
        self.__subsystem = subsystem

    def get_info(self):
        return self.__subsystem.get_info()
    
    def get_state(self):
        return self.__subsystem.get_state()
    
    def get_kv_property(self, key : bytes, writable = True, readable = True, cache = False):
        return self.__subsystem.get_kv_property(key, writable, readable, cache)
    
    def add_kv_handler(self, key : bytes):
        return self.__subsystem.add_kv_handler(key)
    
    def add_remote_kv(self, key : bytes, s_uuid : uuid.UUID, subscribe = False):
        return self.__subsystem.add_remote_kv(s_uuid, key, subscribe)
    
    def get_kv(self, target_uuid : uuid.UUID, key : bytes, ret = KVP_RET_AWAIT):
        return self.__subsystem.get_kv(target_uuid, key, ret)
    
    def get_kv_desc(self, target_uuid : uuid.UUID, key : bytes, ret = KVP_RET_AWAIT):
        return self.__subsystem.get_kv_desc(target_uuid, key, ret)
            
    def set_kv(self, target_uuid : uuid.UUID, key : bytes, val: bytes, ret = KVP_RET_AWAIT):
        return self.__subsystem.set_kv(target_uuid, key, val, ret)
    
class _KVHandlerBase:
    def remote_set(self, value):
        pass

    def remote_get(self, value):
        pass

    def get_handle(self):
        pass

    def get_type_descriptor(self):
        pass

class _KVHandler:
    class KVHandle:
        def __init__(self, handler : "_KVHandler"):
            self.__handler = handler

        def on_get(self, func):
            self.__handler.on_get(func)

        def on_set(self, func):
            self.__handler.on_set(func)

        def get_key(self):
            return self.__handler.get_key()

        
    def __init__(self, key : bytes):
        self.__key = key

        self.__on_get = None
        self.__on_set= None

        self.__p_type = ByteTypeSpecifier()

        self.__handle = self.KVHandle(self)

    def remote_set(self, value: bytes):
        if self.__on_set is None:
            return (TRANSOP_STATE_REJ, b"Value is write-only")
        
        return self.__on_set(self.__handle, self.__p_type.parse(value))
        
    def remote_get(self):
        if self.__on_get is None:
            return (TRANSOP_STATE_REJ, b"Value is read-only")
        
        state, ret = self.__on_get(self.__handle)

        if state == TRANSOP_STATE_OK:
            return (state, self.__p_type.encode(ret))
        
        return (state, ret)
        
    def get_key(self):
        return self.__key
    
    def set_type(self, p_type : PropertyTypeSpecifier):
        self.__p_type = p_type

    def on_get(self, func):
        self.__on_get = func

    def on_set(self, func):
        self.__on_set = func

    def get_handle(self):
        return self.__handle
    
    def get_type_descriptor(self):
        return segmented_bytearray.encode([self.__p_type.encode_type(), self.__key, False.to_bytes(length=1, byteorder="big")])

    
class _LocalProperty(_KVHandlerBase):
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

        def on_new_data_received(self, func):
            self.__property.on_new_data_received(func)
        

        value = property(__read, __write, __del)
    def __init__(self, key : str, subsystem: "DDSClient._RegisteredSubsystem", write = True, read = True, send = False):
        self.__key = key
        self.__writable = write
        self.__readable = read
        self.__subsystem = subsystem
        self.__send = send

        self.__new_data_handler = None

        self.__p_type = ByteTypeSpecifier()

        self.__property_handler = self.__PropertyHandler(self)

        if send: # Cacned values are read-only
            self.__writable = False

        self.__value = None

    def remote_set(self, value : bytes):
        if not self.__writable:
            return (TRANSOP_STATE_REJ, b"Value is read-only")
        
        try:
            self.__p_type.parse(value)
        except ValueError:
            return (TRANSOP_STATE_REJ, b"Value is not valid for property type")
        
        if self.__new_data_handler is not None:
            self.__new_data_handler(self.__p_type.parse(value))
        
        self.__value = value
        return (TRANSOP_STATE_OK, bytes())

    def remote_get(self):
        if not self.__readable:
            return (TRANSOP_STATE_REJ, b"Value is write-only")
        
        if self.__value is None:
            return (TRANSOP_STATE_REJ, b"Value has not been set yet!")
        
        return (TRANSOP_STATE_OK, self.__value)
    
    def handle_set_value(self, value):
        encoded = None
        try:
            encoded = self.__p_type.encode(value)
        except ValueError:
            raise ValueError("Property type is incompatible with provided value")
        
        self.__value = encoded

        if self.__send:
            self.__subsystem.get_client().set_kv(self.__key, self.__value, self.__subsystem.get_uuid(), self.__subsystem.get_uuid())

    def handle_get_value(self, p_type : PropertyTypeSpecifier):
        return self.__p_type.parse(self.__value)
    
    def get_handle(self):
        return self.__property_handler
    
    def set_type(self, p_type : PropertyTypeSpecifier):
        self.__p_type = p_type

    def get_type_descriptor(self):
        return segmented_bytearray.encode([self.__p_type.encode_type(), self.__key, self.__send.to_bytes(length=1, byteorder="big")])
    
    def on_new_data_received(self, func):
        self.__new_data_handler = func
    
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

        def on_new_data_received(self, func):
            self.__property.on_new_data_received(func)

        value = property(__read, __write, __del)
    def __init__(self, key : str, subsystem: "DDSClient._RegisteredSubsystem", remote : uuid.UUID, subscribe = True):
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
        
        if self.__new_data_handler is not None:
            self.__new_data_handler(self.__p_type.parse(value))
        
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
            try:
                return self.__p_type.parse(self.__value)
            except ValueError:
                raise ValueError("Received value type incompatible with declared value type!")
        
        if self.__subscribe:
            return None
        
        handle = self.__subsystem.get_kv(self.__remote, self.__key, KVP_RET_HANDLE)

        if handle is None:
            return None

        start = time.time()
        while handle.get_state() == TRANSOP_STATE_PENDING and time.time() - start < 1.0:
            time.sleep(0.01)

        if handle.get_state() != TRANSOP_STATE_OK:
            print("Failed to retrieve value: ", handle.get_reason())
            return None

        try:
            return self.__p_type.parse(handle.get_value())
        except ValueError:
            raise ValueError("Received value type incompatible with declared value type!")
    
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
    
    def on_new_data_received(self, func):
        self.__new_data_handler = func

class DDSClient:
    __E_MESSAGE = 0
    __E_TRANSACT_DATA_AVAIL = 1
    __E_CONNECTED = 2
    __E_NEW_TRANSACT = 4
    __E_DISCONNECTED = 5

    REG_STATE_OK = 0
    REG_STATE_REFUSED = 1
    REG_STATE_NOT_REGISTERED = 2

    class _RegisteredSubsystem:
        def __init__(self, info: "SubsystemInfo", client: "DDSClient"):
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
        
        def get_kv_property(self, key : bytes, writable = True, readable = True, cache = False):
            lp = _LocalProperty(key, self, writable, readable, cache)
            self.__kv_providers[key] = lp
            return lp.get_handle()
        
        def add_kv_handler(self, key : bytes):
            lp = _KVHandler(key)
            self.__kv_providers[key] = lp
            return lp.get_handle()
        
        def add_remote_kv(self, t_uuid : uuid.UUID, key : bytes, subscribe = False):
            lp = _RemoteProperty(key, self, t_uuid, subscribe)
            return lp.get_handle()
        
        def get_client(self):
            return self.__client
        
        def get_kv(self, target_uuid : uuid.UUID, key : bytes, ret = KVP_RET_AWAIT):
            return self.__client.get_kv(key, target_uuid, self.get_uuid(), ret)
            
        def get_kv_desc(self, target_uuid : uuid.UUID, key : bytes, ret = KVP_RET_AWAIT):
            return self.__client.get_kv_desc(key, target_uuid, self.get_uuid(), ret)
            
        def set_kv(self, target_uuid : uuid.UUID, key : bytes, val: bytes, ret = KVP_RET_AWAIT):
            return self.__client.set_kv(key, val, target_uuid, self.get_uuid(), ret)
            
        def get_kv_descriptors(self):
            r = []

            for (k, kvp) in self.__kv_providers.items:
                r.append(k)
                r.append(kvp.get_type_descriptor())
            
            return segmented_bytearray.encode(r)
        
        def get_kv_descriptor(self, key : bytes):
            kvp = self.__kv_providers.get(key)

            if kvp is None:
                return None
            
            return kvp.get_type_descriptor()

    class __TransOpHandle:
        class _TransOpReturnHandle:
            def __init__(self, handle : "DDSClient.__TransOpHandle"):
                self.__handle = handle

            def get_state(self):
                return self.__handle.get_state()
            
            def get_reason(self):
                return self.__handle.get_reason()
            
            def get_value(self):
                return self.__handle.get_value()
            
        def __init__(self):
            self.__state = TRANSOP_STATE_PENDING
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
            return self._TransOpReturnHandle(self)

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

        if t.get_data()[0] == TRANSACT_RGET_KV_DESC:
            s_uuid, key = segmented_bytearray.decode(t.get_data()[1:])
            s = self.__subsystem_handles.get(uuid.UUID(bytes=s_uuid))
            if s is None:
                t.ret(bytes([TRANSOP_STATE_REJ]) + b"Specified subsystem not found.")
                return

            desc = s.get_kv_descriptor(key)
            if desc is None:
                t.ret(bytes([TRANSOP_STATE_REJ]) + b"Specified subsystem does not contain specified key.")
                return
            
            t.ret(bytes([TRANSOP_STATE_OK]) + desc)
    
    def __rget_kv(self, t: transactions.TransactionManager.IncomingTransactionHandle):
        (t_uuid, key) = segmented_bytearray.decode(t.get_data()[1:])

        t_uuid = uuid.UUID(bytes=t_uuid)

        if self.__subsystem_handles.get(t_uuid) is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + b"Specified subsystem not found.")
            return


        p = self.__subsystem_handles[t_uuid].get_kvp(key)
        if p is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + b"Specified value not found.")
            return

        state, data = p.remote_get()
        t.ret(bytes([state]) + data)

    def __rset_kv(self, t: transactions.TransactionManager.IncomingTransactionHandle):
        (t_uuid, key, value) = segmented_bytearray.decode(t.get_data()[1:])

        t_uuid = uuid.UUID(bytes=t_uuid)

        if self.__subsystem_handles.get(t_uuid) is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + b"Specified subsystem not found.")
            return

        p = self.__subsystem_handles[t_uuid].get_kvp(key)
        if p is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + b"Specified value not found.")
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
    
    def __transop(self, data, await_type = KVP_RET_AWAIT):
        if not self.__is_ready:
            return None
        
        if await_type == KVP_RET_HANDLE:
            ret_handle = self.__TransOpHandle()

            self.__transactions.send_transaction(data).then(self.__on_transop_returned_handle, [ret_handle])
            return ret_handle.get_handle()
        elif await_type == KVP_RET_AWAIT:
            ret_awaiter = mt_events.Awaiter()

            self.__transactions.send_transaction(data).then(self.__on_transop_returned_await, [ret_awaiter])
            return ret_awaiter.get_handle()

    def __on_transop_returned_await(self, awaiter : mt_events.Awaiter, handle : transactions.TransactionManager.OutgoingTransactionHandle):
        if handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_NAK:
            print("Transop NAK'd!!")
            awaiter.call(state=TRANSOP_STATE_REJ, reason=None)
            return

        s = TRANSOP_STATE_OK if handle.get_result()[0] == TRANSOP_STATE_OK else TRANSOP_STATE_REJ
        reason = None if s == TRANSOP_STATE_OK else handle.get_result()[1:].decode("utf-8")
        value = None if s != TRANSOP_STATE_OK else handle.get_result()[1:]
        
        awaiter.call(state=s, reason=reason, value=value)
    
    def __on_transop_returned_handle(self, op_handle : "DDSClient.__TransOpHandle", handle : transactions.TransactionManager.OutgoingTransactionHandle):
        if handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_NAK:
            print("Transop NAK'd!!")
            op_handle.set_state(TRANSOP_STATE_REJ)
            return

        s = TRANSOP_STATE_OK if handle.get_result()[0] == TRANSOP_STATE_OK else TRANSOP_STATE_REJ
        reason = None if s == TRANSOP_STATE_OK else handle.get_result()[1:].decode("utf-8")
        value = None if s != TRANSOP_STATE_OK else handle.get_result()[1:]

        op_handle.set_state(s)
        op_handle.set_reason(reason)
        op_handle.set_value(value)

    def set_kv(self, key : str, val : bytes, t_uuid : uuid.UUID, s_uuid : uuid.UUID, ret_type = KVP_RET_AWAIT):
        return self.__transop(bytes([TRANSACT_SET_KV]) + segmented_bytearray.encode([t_uuid.bytes, s_uuid.bytes, key, val]), ret_type)

    def get_kv(self, key : str, t_uuid : uuid.UUID, s_uuid : uuid.UUID, ret_type = KVP_RET_AWAIT):
        return self.__transop(bytes([TRANSACT_GET_KV]) + segmented_bytearray.encode([t_uuid.bytes, s_uuid.bytes, key]), ret_type)
    
    def get_kv_desc(self, key : str, t_uuid : uuid.UUID, s_uuid : uuid.UUID, ret_type = KVP_RET_AWAIT):
        return self.__transop(bytes([TRANSACT_GET_KV_DESC]) + segmented_bytearray.encode([t_uuid.bytes, s_uuid.bytes, key]), ret_type)
    
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
        self.__socket.put(bytes([MAGIC_REQ_SUBSCRIBE]) + segmented_bytearray.encode([kv.get_remote().bytes, kv.get_key()]))


    def get_registered(self):
        return self.__registered
