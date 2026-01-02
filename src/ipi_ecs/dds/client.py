import queue
import time
import uuid

import ipi_ecs.core.tcp as tcp
import ipi_ecs.core.daemon as daemon
import ipi_ecs.core.mt_events as mt_events
import ipi_ecs.core.transactions as transactions
import ipi_ecs.core.segmented_bytearray as segmented_bytearray

from ipi_ecs.dds.subsystem import SubsystemInfo, KVDescriptor, EventDescriptor
from ipi_ecs.dds.types import PropertyTypeSpecifier, ByteTypeSpecifier
from ipi_ecs.dds.magics import *

class _TransOpHandle:
    class _TransOpReturnHandle:
        def __init__(self, handle : "_TransOpHandle"):
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
    
class TransopException(Exception):
    pass

class RegisteredSubsystemHandle:
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
    
    def add_remote_kv(self, t_uuid: uuid.UUID, desc: KVDescriptor):
        return self.__subsystem.add_remote_kv(t_uuid, desc)
    
    def get_kv(self, target_uuid : uuid.UUID, key : bytes, ret = KVP_RET_AWAIT):
        return self.__subsystem.get_kv(target_uuid, key, ret)
    
    def get_kv_desc(self, target_uuid : uuid.UUID, key : bytes, ret = KVP_RET_AWAIT):
        return self.__subsystem.get_kv_desc(target_uuid, key, ret)
            
    def set_kv(self, target_uuid : uuid.UUID, key : bytes, val: bytes, ret = KVP_RET_AWAIT):
        return self.__subsystem.set_kv(target_uuid, key, val, ret)
    
    def get_subsystem(self, target_uuid : uuid.UUID, ret = KVP_RET_AWAIT):
        return self.__subsystem.get_subsystem(target_uuid, ret)
    
    def get_all(self):
        return self.__subsystem.get_system()
    
    def add_event_provider(self, name : bytes):
        return self.__subsystem.add_event_provider(name)
    
    def add_event_handler(self, name : bytes):
        return self.__subsystem.add_event_handler(name)

class RemoteSubsystemHandle:
    def __init__(self, client: "DDSClient", info: SubsystemInfo, me: "DDSClient._RegisteredSubsystem"):
        self.__info = info
        self.__client = client
        self.__me = me

    def get_info(self):
        return self.__info
    
    def get_kv(self, key : bytes):
        awaiter = mt_events.Awaiter()

        def __ret(value: KVDescriptor):
            kv = self.__me.add_remote_kv(self.__info.get_uuid(), value)
            awaiter.call(kv)
        
        self.__me.get_kv_desc(self.__info.get_uuid(), key, KVP_RET_AWAIT).then(__ret)
        return awaiter.get_handle()
    
class InProgressEvent:
    class _Handle:
        def __init__(self, handle: "InProgressEvent"):
            self.__handle = handle

        def get_result(self, t_uuid):
            return self.__handle.get_result(t_uuid)
    
        def get_state(self, t_uuid):
            return self.__handle.get_state(t_uuid)
        
        def get_event_state(self):
            return self.__handle.get_event_state()
        
        def get_reason(self):
            return self.__handle.get_reason()
        
        def is_in_progress(self):
            return self.__handle.is_in_progress()

        def after(self):
            return self.__handle.after()
        
        def get_name(self):
            return self.__handle.get_name()
        
        def get_uuid(self):
            return self.__handle.get_uuid()

    def __init__(self, name: bytes, subsystem: "DDSClient._RegisteredSubsystem", r_type: PropertyTypeSpecifier, call_transop : mt_events.Awaiter.AwaiterHandle):
        self.__name = name
        self.__results = dict()
        self.__subsystem = subsystem
        self.__r_type = r_type
        self.__call_transop = call_transop

        self.__uuid = None

        self.__state = EVENT_PENDING
        self.__reason = None

        def _state_change(v):
            self.__state = EVENT_IN_PROGRESS
            b_e_uuid, b_rets = segmented_bytearray.decode(v)
            ret_status = []
            rets = segmented_bytearray.decode(b_rets)

            for ret in rets:
                b_uuid, b_ok = segmented_bytearray.decode(ret)
                s_uuid = uuid.UUID(bytes=b_uuid)
                ok = bool.from_bytes(b_ok, byteorder="big")

                self.set_result(s_uuid, EVENT_IN_PROGRESS if ok else EVENT_REJ, b"Subsystem disconnected" if not ok else None)

            self.__uuid = uuid.UUID(bytes=b_e_uuid)
            self.__subsystem.add_in_progress_event(self)
        
        def _transop_rej(state, reason):
            self.__reason = reason

            if state == TRANSOP_STATE_REJ:
                self.__state = EVENT_REJ

            self.__awaiter.throw(state=state, reason=reason)

        self.__call_transop.then(_state_change).catch(_transop_rej)
        self.__awaiter = mt_events.Awaiter()

    def set_result(self, t_uuid: uuid.UUID, status = EVENT_PENDING, data = None):
        if status == EVENT_OK:
            try:
                v = self.__r_type.parse(data)
            except ValueError:
                raise ValueError("Returned value is incompatible with expected return type")
        else:
            v = data
        
        self.__results[t_uuid] = (status, v)

        if not self.is_in_progress():
            self.__awaiter.call(self.get_handle())

    def get_result(self, t_uuid):
        v = self.__results.get(t_uuid)
        if v is None:
            return None
        s, d = v

        return d
    
    def get_state(self, t_uuid):
        v = self.__results.get(t_uuid)
        if v is None:
            return None
        s, d = v

        return s
    
    def get_event_state(self):
        return self.__state
    
    def get_reason(self):
        return self.__reason

    def is_in_progress(self):
        if self.__state == EVENT_REJ:
            return False
        
        if len(self.__results.keys()) == 0:
            return True
        
        for s, r in self.__results.values():
            if s == EVENT_IN_PROGRESS:
                return True
            
        return False

    def after(self):
        return self.__awaiter

    def get_handle(self):
        return self._Handle(self)
    
    def get_name(self):
        return self.__name
    
    def get_uuid(self):
        return self.__uuid

class _EventHandler:
    class _Handle:
        def __init__(self, handler : "_EventHandler"):
            self.__handler = handler

        def on_called(self, func):
            self.__handler.on_call(func)

        def get_name(self):
            return self.__handler.get_name()
        
        def set_types(self, paramerer_type, return_type):
            self.__handler.set_types(paramerer_type, return_type)
        
    class IncomingEventHandle:
        def __init__(self, handler: "_EventHandler", e_uuid: uuid.UUID):
            self.__handler = handler
            self.__e_uuid = e_uuid
        
        def ret(self, value):
            self.__handler.handle_return(self.__e_uuid, EVENT_OK, value)
        
        def fail(self, reason):
            self.__handler.handle_return(self.__e_uuid, EVENT_REJ, reason)

        
    def __init__(self, subsystem: "DDSClient._RegisteredSubsystem", name : bytes):
        self.__name = name
        self.__subsystem = subsystem

        self.__p_type = ByteTypeSpecifier()
        self.__r_type = ByteTypeSpecifier()

        self.__handle = self._Handle(self)
        
    def handle_call(self, sender: uuid.UUID, e_uuid: uuid.UUID, value: bytes):
        v = None
        try:
            v = self.__p_type.parse(value)
        except ValueError:
            return (EVENT_REJ, b"Value is not valid for property type")
        
        self.__on_call(sender, v, self.IncomingEventHandle(self, e_uuid))

    def handle_return(self, e_uuid: uuid.UUID, state, value):
        if state != EVENT_OK:
            self.__subsystem.send_event_return(e_uuid, state, value)
            return

        try:
            v = self.__r_type.encode(value)
        except ValueError:
            print("Received invalid data from handler function!")
            return (TRANSOP_STATE_REJ, b"Internal error, handler returned invalid data!")

        self.__subsystem.send_event_return(e_uuid, state, v)
        
        
    def get_name(self):
        return self.__key
    
    def set_types(self, p_type : PropertyTypeSpecifier, r_type : PropertyTypeSpecifier):
        self.__p_type = p_type
        self.__r_type = r_type

    def on_call(self, func):
        self.__on_call = func

    def get_handle(self):
        return self.__handle
    
    def get_descriptor(self, requester: uuid.UUID):
        return EventDescriptor(self.__p_type, self.__r_type, self.__name).encode()
    
class _EventProvider:
    class _Handle:
        def __init__(self, handler : "_EventProvider"):
            self.__handler = handler

        def call(self, value, target: uuid.UUID):
            return self.__handler.call(value, target)

        def get_name(self):
            return self.__handler.get_name()
        
        def set_types(self, paramerer_type, return_type):
            self.__handler.set_types(paramerer_type, return_type)
        
    def __init__(self, name : bytes, subsystem: "DDSClient._RegisteredSubsystem"):
        self.__name = name

        self.__p_type = ByteTypeSpecifier()
        self.__r_type = ByteTypeSpecifier()

        self.__subsystem = subsystem

        self.__handle = self._Handle(self)

    def call(self, value, targets: list):
        v = None
        try:
            v = self.__p_type.encode(value)
        except ValueError:
            raise ValueError("Parameter type is incompatible with provided value")
        
        t_bytes = []

        for target in targets:
            t_bytes.append(target.bytes)
        
        t_bytes = segmented_bytearray.encode(t_bytes)
        
        a = self.__subsystem.get_client()._call_event(self.__name, v, t_bytes, self.__subsystem.get_uuid(), KVP_RET_AWAIT)
        h = InProgressEvent(self.__name, self.__subsystem, self.__r_type, a)

        return h.get_handle()
        
    def get_name(self):
        return self.__key
    
    def set_types(self, p_type : PropertyTypeSpecifier, r_type : PropertyTypeSpecifier):
        self.__p_type = p_type
        self.__r_type = r_type

    def get_handle(self):
        return self.__handle
    
    def get_descriptor(self, requester: uuid.UUID):
        return EventDescriptor(self.__p_type, self.__r_type, self.__name).encode()
    
class _KVHandlerBase:
    def remote_set(self, requester: uuid.UUID, value: bytes):
        pass

    def remote_get(self, requester: uuid.UUID):
        pass

    def get_handle(self):
        pass

    def get_type_descriptor(self, requester: uuid.UUID):
        pass

class _KVHandler(_KVHandlerBase):
    class KVHandle:
        def __init__(self, handler : "_KVHandler"):
            self.__handler = handler

        def on_get(self, func):
            self.__handler.on_get(func)

        def on_set(self, func):
            self.__handler.on_set(func)

        def get_key(self):
            return self.__handler.get_key()
        
        def set_type(self, type):
            self.__handler.set(type)

        
    def __init__(self, key : bytes, subsystem: "DDSClient._RegisteredSubsystem"):
        self.__key = key

        self.__on_get = None
        self.__on_set= None

        self.__subsystem = subsystem

        self.__p_type = ByteTypeSpecifier()

        self.__handle = self.KVHandle(self)

    def remote_set(self, requester: uuid.UUID, value: bytes):
        if self.__on_set is None:
            return (TRANSOP_STATE_REJ, b"Value is write-only")
        
        return self.__on_set(self.__handle, requester, self.__p_type.parse(value))
        
    def remote_get(self, requester: uuid.UUID):
        if self.__on_get is None:
            return (TRANSOP_STATE_REJ, b"Value is read-only")
        
        state, ret = self.__on_get(requester)

        if state == TRANSOP_STATE_OK:
            return (state, self.__p_type.encode(ret))
        
        return (state, ret)
        
    def get_key(self):
        return self.__key
    
    def set_type(self, p_type : PropertyTypeSpecifier):
        self.__p_type = p_type
        self.__subsystem.invalidate()

    def on_get(self, func):
        self.__on_get = func
        self.__subsystem.invalidate()

    def on_set(self, func):
        self.__on_set = func
        self.__subsystem.invalidate()

    def get_handle(self):
        return self.__handle
    
    def get_type_descriptor(self, requester: uuid.UUID):
        return KVDescriptor(self.__p_type, self.__key, False, self.__on_get is not None, self.__on_set is not None).encode()

    
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

    def remote_set(self, requester: uuid.UUID, value : bytes):
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

    def remote_get(self, requester: uuid.UUID):
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
            self.__subsystem.get_client()._set_kv(self.__key, self.__value, self.__subsystem.get_uuid(), self.__subsystem.get_uuid())

    def handle_get_value(self, p_type : PropertyTypeSpecifier):
        return self.__p_type.parse(self.__value)
    
    def get_handle(self):
        return self.__property_handler
    
    def set_type(self, p_type : PropertyTypeSpecifier):
        self.__p_type = p_type
        self.__subsystem.invalidate()

    def get_type_descriptor(self, requester: uuid.UUID):
        return KVDescriptor(self.__p_type, self.__key, self.__send, self.__readable, self.__writable).encode()
        #return segmented_bytearray.encode([self.__p_type.encode_type(), self.__key, self.__send.to_bytes(length=1, byteorder="big"), self.__readable.to_bytes(length=1, byteorder="big"), self.__writable.to_bytes(length=1, byteorder="big")])
    
    def on_new_data_received(self, func):
        self.__new_data_handler = func
    
class _RemoteProperty:
    class __PropertyHandler:
        def __init__(self, provider : "_RemoteProperty"):
            self.__property = provider

        def __write(self, value):
            self.__property.handle_set_value(value)

        def __read(self):
            return self.__property.handle_get_value()
        
        def __del(self): 
            return
        
        def set_type(self, p_type : PropertyTypeSpecifier):
            self.__property.set_type(p_type)

        def on_new_data_received(self, func):
            self.__property.on_new_data_received(func)

        def is_cached(self):
            return self.__property.is_cached()

        value = property(__read, __write, __del)
    def __init__(self, key : str, subsystem: "DDSClient._RegisteredSubsystem", remote : uuid.UUID, subscribe = True, readable = True, writable = True, p_type = None):
        self.__key = key
        self.__subsystem = subsystem
        self.__remote = remote
        self.__subscribe = subscribe

        self.__p_type = p_type

        if self.__p_type is None:
            self.__p_type = ByteTypeSpecifier()

        self.__property_handler = self.__PropertyHandler(self)

        self.__value = None

        self.__readable = readable
        self.__writable = writable

        if self.__subscribe:
            self.__subsystem.get_client()._add_active_subscriber(self)

    def from_descriptor(d : KVDescriptor, subsystem : "DDSClient._RegisteredSubsystem", remote: uuid.UUID):
        key = d.get_key()
        sub = d.get_published()
        r = d.get_readable()
        w = d.get_writable()
        t = d.get_type()

        return _RemoteProperty(key, subsystem, remote, sub, r, w, t)
        

    def remote_set(self, value : bytes):
        try:
            self.__p_type.parse(value)
        except ValueError:
            return
        
        if self.__new_data_handler is not None:
            self.__new_data_handler(self.__p_type.parse(value))
        
        self.__value = value
    
    def handle_set_value(self, value):
        if not self.__writable:
            raise ValueError("Property is read-only")

        encoded = None
        try:
            encoded = self.__p_type.encode(value)
        except ValueError:
            raise ValueError("Property type is incompatible with provided value")
        
        self.__subsystem.get_client()._set_kv(self.__key, encoded, self.__remote, self.__subsystem.get_uuid())

    def handle_get_value(self):
        if not self.__readable:
            raise ValueError("Property is write-only")
        
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
            #print("Failed to retrieve value: ", handle.get_reason())
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
        return KVDescriptor(self.__p_type, self.__key, self.__subscribe, self.__readable, self.__writable).encode()
    
    def get_remote(self):
        return self.__remote
    
    def get_key(self):
        return self.__key
    
    def is_cached(self):
        return self.__subscribe
    
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
            self.__event_handlers = dict()
            self.__event_providers = dict()

            self.__in_progress_events = dict()
            self.__incoming_events = dict()

        def get_info(self):
            return self.__info
        
        def get_state(self):
            return self.__client.get_registered()
        
        def get_handle(self):
            return RegisteredSubsystemHandle(self)
        
        def get_uuid(self):
            return self.__info.get_uuid()

        def get_kvp(self, key):
            return self.__kv_providers.get(key)
        
        def get_event_handler(self, key : bytes):
            return self.__event_handlers.get(key)
        
        def get_event_provider(self, key : bytes):
            return self.__event_handlers.get(key)
        
        def get_kv_property(self, key : bytes, writable = True, readable = True, cache = False):
            lp = _LocalProperty(key, self, writable, readable, cache)
            self.__kv_providers[key] = lp

            self.invalidate()
            return lp.get_handle()
        
        def add_kv_handler(self, key : bytes):
            lp = _KVHandler(key, self)
            self.__kv_providers[key] = lp

            self.invalidate()
            return lp.get_handle()
        
        def add_remote_kv(self, t_uuid : uuid.UUID, desc : KVDescriptor):
            lp = _RemoteProperty.from_descriptor(desc, self, t_uuid)
            return lp.get_handle()
        
        def get_client(self):
            return self.__client
        
        def get_kv(self, target_uuid : uuid.UUID, key : bytes, ret = KVP_RET_AWAIT):
            return self.__client._get_kv(key, target_uuid, self.get_uuid(), ret)
            
        def get_kv_desc(self, target_uuid : uuid.UUID, key : bytes, ret = KVP_RET_AWAIT):
            return self.__client._get_kv_desc(key, target_uuid, self.get_uuid(), ret)
            
        def set_kv(self, target_uuid : uuid.UUID, key : bytes, val: bytes, ret = KVP_RET_AWAIT):
            return self.__client._set_kv(key, val, target_uuid, self.get_uuid(), ret)
        
        def get_subsystem(self, target_uuid : uuid.UUID, ret = KVP_RET_AWAIT):
            return self.__client._get_subsystem(target_uuid, self.get_uuid(), ret)
            
        def get_kv_descriptors(self):
            r = []

            for (k, kvp) in self.__kv_providers.items():
                r.append(kvp.get_type_descriptor(self.get_uuid()))
            
            return segmented_bytearray.encode(r)
        
        def get_event_descriptors(self):
            h = []
            p = []

            for (k, e) in self.__event_handlers.items():
                h.append(e.get_descriptor(self.get_uuid()))

            for (k, e) in self.__event_providers.items():
                p.append(e.get_descriptor(self.get_uuid()))
            
            return segmented_bytearray.encode([segmented_bytearray.encode(h), segmented_bytearray.encode(p)])
        
        def get_kv_descriptor(self, requester: uuid.UUID, key : bytes):
            kvp = self.__kv_providers.get(key)

            if kvp is None:
                return None
            
            return kvp.get_type_descriptor(requester)
        
        def invalidate(self):
            self.__info = SubsystemInfo(self.__info.get_uuid(), self.__info.get_name(), self.__info.get_temporary(), self.get_kv_descriptors(), self.get_event_descriptors())
            self.__client._send_subsystem_info(self.__info)

        def get_system(self):
            return self.__client._get_system(self)
        
        def add_in_progress_event(self, e: InProgressEvent):
            self.__in_progress_events[e.get_uuid()] = e

        def incoming_event(self, e: uuid.UUID, t: transactions.TransactionManager.IncomingTransactionHandle, s_uuid: uuid.UUID, name: bytes, param: bytes):
            e_h = self.__event_handlers.get(name)

            if e_h is None:
                t.ret(bytes([EVENT_REJ]) + b"Subsystem does not handle specified event.")
                return

            self.__incoming_events[e] = t

            e_h.handle_call(s_uuid, e, param)

        def add_event_provider(self, name : bytes):
            e = _EventProvider(name, self)

            self.__event_providers[name] = e
            self.invalidate()
            return e.get_handle()
        
        def add_event_handler(self, name : bytes):
            e = _EventHandler(self, name)

            self.__event_handlers[name] = e
            self.invalidate()
            return e.get_handle()
        
        def on_event_return(self, e_uuid: uuid.UUID, r_uuid: uuid.UUID, status: int, ret_value: bytes):
            e = self.__in_progress_events.get(e_uuid)

            if e is None:
                print("Received event return that this subsystem did not send!")
                return
            
            e.set_result(r_uuid, status, ret_value)

        def send_event_return(self, e_uuid: uuid.UUID, state: int, v: bytes):
            #print("senjdi pop: ", e_uuid)
            t = self.__incoming_events.pop(e_uuid)


            if t is None:
                print("Received request to send event return for an event that this subsystem did not receive!")
                return
            
            t.ret(bytes([state]) + v)

    def __init__(self, c_uuid : uuid.UUID, ip = "127.0.0.1"):
        self.__uuid = c_uuid

        self.__socket = tcp.TCPClientSocket()
        self.__socket.connect((ip, SERVER_PORT))
        #print("Connecting to: ", (ip, SERVER_PORT))
        self.__socket.start()

        self.__registered = self.REG_STATE_NOT_REGISTERED
        self.__registered_awaiter = mt_events.Awaiter()
        self.__subsystem_handles = dict()
        self.__subsystem_info = []
        self.__active_subscribers = dict()

        self.__cached_subsystems = dict()

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
        self.__remote_subsystem_update_event = mt_events.Event()

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

                #print("Handshake received from ", self.__socket.remote())
                self.__handshake_received = True

            if not self.__handshake_received:
                raise Exception("Invalid handshake received!")

            if d[0] == MAGIC_TRANSACT:
                self.__transactions.received(d[1:])
            elif d[0] == MAGIC_SUBSCRIBED_UPD:
                s_uuid, key, val = segmented_bytearray.decode(d[1:])


                for kvs in self.__active_subscribers[uuid.UUID(bytes=s_uuid)]:
                    if kvs.get_key() == key:
                        kvs.remote_set(val)
            elif d[0] == MAGIC_SYSTEM_UPD:
                s_data = segmented_bytearray.decode(d[1:])
                for b_data in s_data:
                    b_info, b_ok = segmented_bytearray.decode(b_data)
                    info = SubsystemInfo.decode(b_info)
                    ok = bool.from_bytes(b_ok, byteorder="big")

                    self.__cached_subsystems[info.get_uuid()] = (info, ok)

                    self.__remote_subsystem_update_event.call()
            elif d[0] == MAGIC_EVENT_RET:
                b_s_uuid, b_r_uuid, b_e_uuid, b_status, ret_value = segmented_bytearray.decode(d[1:])
                s_uuid = uuid.UUID(bytes=b_s_uuid)
                r_uuid = uuid.UUID(bytes=b_r_uuid)
                e_uuid = uuid.UUID(bytes=b_e_uuid)

                status = int.from_bytes(b_status, byteorder="big")

                s = self.__subsystem_handles.get(s_uuid)
                if s is None:
                    print("Received event return for event that originated from nonexistent subsystem!")
                    return
                #print(s, status, ret_value)
                s.on_event_return(e_uuid, r_uuid, status, ret_value)

    def __receive_transact(self):
        t = self.__transactions.get_incoming()

        if t.get_data()[0] == TRANSACT_REQ_UUID:
            t.ret(self.__uuid.bytes)
        
        elif t.get_data()[0] == TRANSACT_CONN_READY:
            if self.__is_ready:
                raise Exception("Received ready transaction twice!")
            
            self.__ready()
            t.ret(self.__uuid.bytes)

        elif t.get_data()[0] == TRANSACT_RGET_KV:
            self.__rget_kv(t)

        elif t.get_data()[0] == TRANSACT_RSET_KV:
            self.__rset_kv(t)

        elif t.get_data()[0] == TRANSACT_RGET_KV_DESC:
            s_uuid, r_uuid, key = segmented_bytearray.decode(t.get_data()[1:])
            s = self.__subsystem_handles.get(uuid.UUID(bytes=s_uuid))
            if s is None:
                t.ret(bytes([TRANSOP_STATE_REJ]) + b"Specified subsystem not found.")
                return

            desc = s.get_kv_descriptor(r_uuid, key)
            if desc is None:
                t.ret(bytes([TRANSOP_STATE_REJ]) + b"Specified subsystem does not contain specified key.")
                return
            
            t.ret(bytes([TRANSOP_STATE_OK]) + desc)
        elif t.get_data()[0] == TRANSACT_RCALL_EVENT:
            b_s_uuid, b_r_uuid, b_e_uuid, name, param = segmented_bytearray.decode(t.get_data()[1:])
            s_uuid = uuid.UUID(bytes=b_s_uuid)
            r_uuid = uuid.UUID(bytes=b_r_uuid)
            e_uuid = uuid.UUID(bytes=b_e_uuid)

            s = self.__subsystem_handles.get(s_uuid)
            if s is None:
                t.ret(bytes([EVENT_REJ]) + b"Specified subsystem not found.")
                return
            
            s.incoming_event(e_uuid, t, r_uuid, name, param)
        else:
            t.nak()
    
    def __rget_kv(self, t: transactions.TransactionManager.IncomingTransactionHandle):
        (t_uuid, s_uuid, key) = segmented_bytearray.decode(t.get_data()[1:])

        t_uuid = uuid.UUID(bytes=t_uuid)
        s_uuid = uuid.UUID(bytes=s_uuid)

        if self.__subsystem_handles.get(t_uuid) is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + b"Specified subsystem not found.")
            return


        p = self.__subsystem_handles[t_uuid].get_kvp(key)
        if p is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + b"Specified value not found.")
            return

        state, data = p.remote_get(s_uuid)
        t.ret(bytes([state]) + data)

    def __rset_kv(self, t: transactions.TransactionManager.IncomingTransactionHandle):
        (t_uuid, s_uuid, key, value) = segmented_bytearray.decode(t.get_data()[1:])

        t_uuid = uuid.UUID(bytes=t_uuid)
        s_uuid = uuid.UUID(bytes=s_uuid)

        if self.__subsystem_handles.get(t_uuid) is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + b"Specified subsystem not found.")
            return

        p = self.__subsystem_handles[t_uuid].get_kvp(key)
        if p is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + b"Specified value not found.")
            return

        state, data = p.remote_set(s_uuid, value)
        t.ret(bytes([state]) + data)

    def __r_event(self, t: transactions.TransactionManager.IncomingTransactionHandle):
        (t_uuid, s_uuid, e_name, value) = segmented_bytearray.decode(t.get_data()[1:])

        t_uuid = uuid.UUID(bytes=t_uuid)
        s_uuid = uuid.UUID(bytes=s_uuid)

        if self.__subsystem_handles.get(t_uuid) is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + b"Specified subsystem not found.")
            return

        p = self.__subsystem_handles[t_uuid].get_event_handler(e_name)

        if p is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + b"Subsystem does not handle specified event.")
            return

        state, data = p.call(data)
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
                #print("Could not register subsystem!")
                self.__registered = self.REG_STATE_REFUSED

            if handle.get_state() != transactions.TransactionManager.OutgoingTransactionHandle.STATE_RET:
                return
            
            info = SubsystemInfo.decode(handle.get_data()[1:])
            
            if self.__subsystem_handles.get(info.get_uuid()) is not None:
                return
            
            self.__registered = self.REG_STATE_OK

            subsystem_handle = self._RegisteredSubsystem(info, self)
            #print("Registered subsystem: ", subsystem_handle.get_info().get_name())

            self.__subsystem_handles[subsystem_handle.get_info().get_uuid()] = subsystem_handle

            self.__registered_event.call()
            self.__registered_awaiter.call(subsystem_handle.get_handle())


    def __ready(self):
        self.__ready_event.call()
        self.__is_ready = True

        self.__send_subsystem_infos()

        self.__refresh_subscriptions()
        
    def close(self):
        #print("Shutting down socket")
        self.__socket.shutdown()

        while not self.__socket.is_closed():
            time.sleep(0.1)

        self.__daemon.stop()
        self.__socket.close()

    def ok(self):
        return not self.__socket.is_closed() and self.__daemon.is_alive()
    
    def register_subsystem(self, name: str, uuid: uuid.UUID, temporary = False):
        self.__subsystem_info.append(SubsystemInfo(uuid, name, temporary))
        return self.__registered_awaiter.get_handle()
    
    def __transop(self, data, await_type = KVP_RET_AWAIT, unpack_value = None):
        if not self.__is_ready:
            return None
        
        if await_type == KVP_RET_HANDLE:
            ret_handle = _TransOpHandle()

            self.__transactions.send_transaction(data).then(self.__on_transop_returned_handle, [ret_handle, unpack_value])
            return ret_handle.get_handle()
        elif await_type == KVP_RET_AWAIT:
            ret_awaiter = mt_events.Awaiter()

            self.__transactions.send_transaction(data).then(self.__on_transop_returned_await, [ret_awaiter, unpack_value])
            return ret_awaiter.get_handle()

    def __on_transop_returned_await(self, awaiter : mt_events.Awaiter, unpack_value, handle : transactions.TransactionManager.OutgoingTransactionHandle):
        if handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_NAK:
            #print("Transop NAK'd!!")
            awaiter.call(state=TRANSOP_STATE_REJ, reason=None)
            return

        s = TRANSOP_STATE_OK if handle.get_result()[0] == TRANSOP_STATE_OK else TRANSOP_STATE_REJ
        reason = None if s == TRANSOP_STATE_OK else handle.get_result()[1:].decode("utf-8")
        value = None if s != TRANSOP_STATE_OK else handle.get_result()[1:]

        if s != TRANSOP_STATE_OK:
            awaiter.throw(state=s, reason=reason)
            return

        if unpack_value is not None and value is not None:
            value = unpack_value(value)

        awaiter.call(value)
    
    def __on_transop_returned_handle(self, op_handle : "DDSClient.__TransOpHandle", unpack_value, handle : transactions.TransactionManager.OutgoingTransactionHandle):
        if handle.get_state() == transactions.TransactionManager.OutgoingTransactionHandle.STATE_NAK:
            #print("Transop NAK'd!!")
            op_handle.set_state(TRANSOP_STATE_REJ)
            return

        s = TRANSOP_STATE_OK if handle.get_result()[0] == TRANSOP_STATE_OK else TRANSOP_STATE_REJ
        reason = None if s == TRANSOP_STATE_OK else handle.get_result()[1:].decode("utf-8")
        value = None if s != TRANSOP_STATE_OK else handle.get_result()[1:]

        if unpack_value is not None and value is not None:
            value = unpack_value(value)

        op_handle.set_state(s)
        op_handle.set_reason(reason)
        op_handle.set_value(value)

    def _set_kv(self, key : str, val : bytes, t_uuid : uuid.UUID, s_uuid : uuid.UUID, ret_type = KVP_RET_AWAIT):
        return self.__transop(bytes([TRANSACT_SET_KV]) + segmented_bytearray.encode([t_uuid.bytes, s_uuid.bytes, key, val]), ret_type)

    def _get_kv(self, key : str, t_uuid : uuid.UUID, s_uuid : uuid.UUID, ret_type = KVP_RET_AWAIT):
        return self.__transop(bytes([TRANSACT_GET_KV]) + segmented_bytearray.encode([t_uuid.bytes, s_uuid.bytes, key]), ret_type)
    
    def _get_kv_desc(self, key : str, t_uuid : uuid.UUID, s_uuid : uuid.UUID, ret_type = KVP_RET_AWAIT):
        unpacker = lambda v: KVDescriptor.decode(v)

        return self.__transop(bytes([TRANSACT_GET_KV_DESC]) + segmented_bytearray.encode([t_uuid.bytes, s_uuid.bytes, key]), ret_type, unpack_value=unpacker)
    
    def _call_event(self, key : str, param : bytes, t_uuids : bytes, s_uuid : uuid.UUID, ret_type = KVP_RET_AWAIT):
        return self.__transop(bytes([TRANSACT_CALL_EVENT]) + segmented_bytearray.encode([t_uuids, s_uuid.bytes, key, param]), ret_type)
    
    def resolve(self, name : bytes, ret_type = KVP_RET_AWAIT):
        unpacker = lambda v: uuid.UUID(bytes=v)

        return self.__transop(bytes([TRANSACT_RESOLVE]) + segmented_bytearray.encode([name]), ret_type, unpack_value=unpacker)
    
    def _get_subsystem(self, t_uuid : uuid.UUID, s_uuid : uuid.UUID, ret_type = KVP_RET_AWAIT):
        unpacker = lambda v: RemoteSubsystemHandle(self, SubsystemInfo.decode(v), self.__subsystem_handles[s_uuid])

        return self.__transop(bytes([TRANSACT_GET_SUBSYSTEM]) + segmented_bytearray.encode([t_uuid.bytes]), ret_type, unpack_value=unpacker)
    
    def __send_subsystem_infos(self):
        for s in self.__subsystem_info:
            self._send_subsystem_info(s)
    
    def _send_subsystem_info(self, info):
        self.__transactions.send_transaction(bytes([TRANSACT_REG_SUBSYSTEM]) + info.encode()).then(self.__transact_status_change)

    def __refresh_subscriptions(self):
        for l in self.__active_subscribers.values():
            for kv in l:
                self.__socket.put(bytes([MAGIC_REQ_SUBSCRIBE]) + segmented_bytearray.encode([kv.get_remote().bytes, kv.get_key()]))

    def _add_active_subscriber(self, kv: _RemoteProperty):
        if self.__active_subscribers.get(kv.get_remote()) is None:
            self.__active_subscribers[kv.get_remote()] = []

        self.__active_subscribers[kv.get_remote()].append(kv)
        self.__socket.put(bytes([MAGIC_REQ_SUBSCRIBE]) + segmented_bytearray.encode([kv.get_remote().bytes, kv.get_key()]))


    def get_registered(self):
        return self.__registered
    
    def _get_system(self, subsystem : "DDSClient._RegisteredSubsystem"):
        handles = []
        for info in self.__cached_subsystems:
            handles.append(RemoteSubsystemHandle(self, info, subsystem))

        return self.__cached_subsystems
    
    def on_remote_system_update(self, c: mt_events.EventConsumer, e):
        self.__remote_subsystem_update_event.bind(c, e)

