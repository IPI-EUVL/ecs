import queue
import time
import uuid
import traceback
import segment_bytes
import mt_events

from ipi_ecs.core import tcp
from ipi_ecs.core import daemon
from ipi_ecs.core import transactions

from ipi_ecs.dds.subsystem import SubsystemInfo, KVDescriptor, EventDescriptor, StatusItem, SubsystemStatus
from ipi_ecs.dds.types import PropertyTypeSpecifier, ByteTypeSpecifier

# I don't want to have to add all magic values one by one, pylance! Stop complaining!
# pylint: disable=wildcard-import, unused-wildcard-import
from ipi_ecs.dds.magics import *


# pylint: disable=line-too-long
# pylint: disable=missing-function-docstring, missing-class-docstring, trailing-whitespace
# pylint: disable=unbalanced-tuple-unpacking
# pylint: disable=unused-private-member

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

class _RegisteredSubsystemHandle:
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
    
    def get_status_item_exists(self, code: int):
        return self.__subsystem.get_status_item_exists(code)
    
    def put_status_item(self, item: StatusItem):
        self.__subsystem.put_status_item(item)

    def clear_status_item(self, code: int):
        self.__subsystem.clear_status_item(code)

    def reset_status_items(self):
        self.__subsystem.reset_status_items()

class _RemoteSubsystemHandle:
    def __init__(self, client: "DDSClient", info: SubsystemInfo, me: "DDSClient._RegisteredSubsystem"):
        self.__info = info
        self.__client = client
        self.__me = me

    def get_info(self):
        return self.__info
    
    def get_status(self):
        return self.__me.get_client().get_status(self.__info.get_uuid())
    
    def get_kv(self, key : bytes):
        awaiter = mt_events.Awaiter()

        def __ret(value: KVDescriptor):
            kv = self.__me.add_remote_kv(self.__info.get_uuid(), value)
            awaiter.call(kv)

        self.__me.get_kv_desc(self.__info.get_uuid(), key, KVP_RET_AWAIT).then(__ret).catch(awaiter.throw)
        return awaiter.get_handle()
    
class _InProgressEvent:
    class _Handle:
        def __init__(self, handle: "_InProgressEvent"):
            self.__handle = handle

        def get_result(self, t_uuid):
            return self.__handle.get_result(t_uuid)
    
        def get_state(self, t_uuid):
            return self.__handle.get_state(t_uuid)
        
        def get_states(self):
            return self.__handle.get_states()
        
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
        
        def on_data(self):
            return self.__handle.on_data()
        
        def abort(self):
            self.__handle.abort()

    def __init__(self, name: bytes, subsystem: "DDSClient._RegisteredSubsystem", r_type: PropertyTypeSpecifier, call_transop : mt_events.Awaiter._AwaiterHandle):
        self.__name = name
        self.__results = dict()
        self.__subsystem = subsystem
        self.__r_type = r_type
        self.__call_transop = call_transop

        self.__uuid = None

        self.__state = EVENT_PENDING
        self.__reason = None

        self.__on_data_event = mt_events.Event()

        self.__t_initiated = time.time()

        def _state_change(v):
            self.__state = EVENT_IN_PROGRESS
            b_e_uuid, b_rets = segment_bytes.decode(v)

            rets = segment_bytes.decode(b_rets)

            for ret in rets:
                b_uuid, b_ok = segment_bytes.decode(ret)
                s_uuid = uuid.UUID(bytes=b_uuid)
                ok = bool.from_bytes(b_ok, byteorder="big")

                self.set_result(s_uuid, EVENT_IN_PROGRESS if ok else EVENT_REJ, E_SUBSYSTEM_DISCONNECTED if not ok else None)

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
            except ValueError as exc:
                raise ValueError("Returned value is incompatible with expected return type") from exc
        else:
            v = data
        
        self.__results[t_uuid] = (status, v)
        self.__on_data_event.call()

        if not self.is_in_progress():
            self.__state = EVENT_OK
            self.__awaiter.call(self.get_handle())

    def get_result(self, t_uuid):
        v = self.__results.get(t_uuid)
        if v is None:
            return None
        _, d = v

        return d
    
    def get_state(self, t_uuid):
        v = self.__results.get(t_uuid)
        if v is None:
            return None
        s, _ = v

        return s
    
    def get_states(self):
        return self.__results.copy()
    
    def get_event_state(self):
        return self.__state
    
    def get_reason(self):
        return self.__reason

    def is_in_progress(self):
        if self.__state == EVENT_REJ:
            return False
        
        if len(self.__results.keys()) == 0:
            return True
        
        for s, _ in self.__results.values():
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
    
    def on_data(self):
        return self.__on_data_event

    def get_t_initiated(self):
        return self.__t_initiated
    
    def abort(self):
        self.__state = EVENT_ABORTED
        self.__awaiter.throw(state=EVENT_ABORTED, reason=E_EVENT_ABORTED)
        self.__on_data_event.call()

        self.__subsystem.on_event_abort(self.__uuid)


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
        
    class _IncomingEventHandle:
        def __init__(self, handler: "_EventHandler", e_uuid: uuid.UUID):
            self.__handler = handler
            self.__e_uuid = e_uuid
        
        def ret(self, value):
            self.__handler.handle_return(self.__e_uuid, EVENT_OK, value)
        
        def fail(self, reason):
            self.__handler.handle_return(self.__e_uuid, EVENT_REJ, reason)

        def feedback(self, reason):
            self.__handler.handle_feedback(self.__e_uuid, EVENT_IN_PROGRESS, reason)

        
    def __init__(self, subsystem: "DDSClient._RegisteredSubsystem", name : bytes):
        self.__name = name
        self.__subsystem = subsystem

        self.__p_type = ByteTypeSpecifier()
        self.__r_type = ByteTypeSpecifier()

        self.__handle = self._Handle(self)
        self.__on_call = None
        
    def handle_call(self, sender: uuid.UUID, e_uuid: uuid.UUID, value: bytes):
        v = None
        try:
            v = self.__p_type.parse(value)
        except ValueError:
            return (EVENT_REJ, E_INVALID_VALUE)
        
        self.__on_call(sender, v, self._IncomingEventHandle(self, e_uuid))

    def handle_return(self, e_uuid: uuid.UUID, state, value: bytes):
        if state != EVENT_OK:
            self.__subsystem.send_event_return(e_uuid, state, value)
            return

        try:
            v = self.__r_type.encode(value)
        except ValueError:
            self.__subsystem.log("Received invalid data from handler function!", level="ERROR")
            return (TRANSOP_STATE_REJ, E_INVALID_VALUE)

        self.__subsystem.send_event_return(e_uuid, state, v)

    def handle_feedback(self, e_uuid: uuid.UUID, state, value):
        self.__subsystem.send_event_feedback(e_uuid, state, value)
        
        
    def get_name(self):
        return self.__name
    
    def set_types(self, p_type : PropertyTypeSpecifier, r_type : PropertyTypeSpecifier):
        self.__p_type = p_type
        self.__r_type = r_type

    def on_call(self, func):
        self.__on_call = func

    def get_handle(self):
        return self.__handle
    
    def get_descriptor(self, _: uuid.UUID):
        return EventDescriptor(self.__p_type, self.__r_type, self.__name).encode()
    
class _EventProvider:
    class _Handle:
        def __init__(self, handler : "_EventProvider"):
            self.__handler = handler

        def call(self, value, target: list[uuid.UUID]):
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
        except ValueError as exc:
            raise ValueError("Parameter type is incompatible with provided value") from exc
        
        t_bytes = []

        for target in targets:
            t_bytes.append(target.bytes)
        
        t_bytes = segment_bytes.encode(t_bytes)
        
        a = self.__subsystem.get_client().call_event(self.__name, v, t_bytes, self.__subsystem.get_uuid(), KVP_RET_AWAIT)
        if a is None:
            return None
        
        h = _InProgressEvent(self.__name, self.__subsystem, self.__r_type, a)

        return h.get_handle()
        
    def get_name(self):
        return self.__name
    
    def set_types(self, p_type : PropertyTypeSpecifier, r_type : PropertyTypeSpecifier):
        self.__p_type = p_type
        self.__r_type = r_type

    def get_handle(self):
        return self.__handle
    
    def get_descriptor(self, _: uuid.UUID):
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
    class _KVHandle:
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

        self.__handle = self._KVHandle(self)

    def remote_set(self, requester: uuid.UUID, value: bytes):
        if self.__on_set is None:
            return (TRANSOP_STATE_REJ, E_WRITEONLY)
        
        return self.__on_set(self.__handle, requester, self.__p_type.parse(value))
        
    def remote_get(self, requester: uuid.UUID):
        if self.__on_get is None:
            return (TRANSOP_STATE_REJ, E_READONLY)
        
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
    class _PropertyHandler:
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

        self.__property_handler = self._PropertyHandler(self)

        if send: # Cacned values are read-only
            self.__writable = False

        self.__value = None

    def remote_set(self, requester: uuid.UUID, value : bytes):
        if not self.__writable:
            return (TRANSOP_STATE_REJ, E_READONLY)
        
        try:
            self.__p_type.parse(value)
        except ValueError:
            return (TRANSOP_STATE_REJ, E_INVALID_VALUE)
        
        if self.__new_data_handler is not None:
            self.__new_data_handler(self.__p_type.parse(value))
        
        self.__value = value
        return (TRANSOP_STATE_OK, bytes())

    def remote_get(self, requester: uuid.UUID):
        if not self.__readable:
            return (TRANSOP_STATE_REJ, E_WRITEONLY)
        
        if self.__value is None:
            return (TRANSOP_STATE_REJ, E_NO_CACNE)
        
        return (TRANSOP_STATE_OK, self.__value)
    
    def handle_set_value(self, value):
        encoded = None
        try:
            encoded = self.__p_type.encode(value)
        except ValueError as exc:
            raise ValueError("Property type is incompatible with provided value") from exc
        
        self.__value = encoded

        if self.__send:
            self.__subsystem.get_client().set_kv(self.__key, self.__value, self.__subsystem.get_uuid(), self.__subsystem.get_uuid())

    def handle_get_value(self):
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
    class _PropertyHandler:
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

        self.__property_handler = self._PropertyHandler(self)

        self.__value = None

        self.__readable = readable
        self.__writable = writable

        self.__new_data_handler = None

        if self.__subscribe:
            self.__subsystem.get_client()._add_active_subscriber(self)

    @staticmethod
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
        except ValueError as exc:
            raise ValueError("Property type is incompatible with provided value") from exc
        
        self.__subsystem.get_client().set_kv(self.__key, encoded, self.__remote, self.__subsystem.get_uuid())

    def handle_get_value(self):
        if not self.__readable:
            raise ValueError("Property is write-only")
        
        if self.__value is not None:
            try:
                return self.__p_type.parse(self.__value)
            except ValueError as exc:
                raise ValueError("Received value type incompatible with declared value type!") from exc
        
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
        except ValueError as exc:
            raise ValueError("Received value type incompatible with declared value type!") from exc
    
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
    REG_STATE_OK = 0
    REG_STATE_REFUSED = 1
    REG_STATE_NOT_REGISTERED = 2

    class _RegisteredSubsystem:
        def __init__(self, info: "SubsystemInfo", client: "DDSClient", logger = None):
            self.__info = info
            self.__client = client

            self.__kv_providers = dict()
            self.__event_handlers = dict()
            self.__event_providers = dict()

            self.__in_progress_events = dict()
            self.__incoming_events = dict()
            self.__active_status_items = dict()

            self.__logger = logger

            self.log(f"Create subsystem: {self.get_info().get_name()}", level="DEBUG")


        def get_info(self):
            return self.__info
        
        def get_state(self):
            return self.__client.get_registered()
        
        def get_handle(self):
            return _RegisteredSubsystemHandle(self)
        
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
            return self.__client.get_kv(key, target_uuid, self.get_uuid(), ret)
            
        def get_kv_desc(self, target_uuid : uuid.UUID, key : bytes, ret = KVP_RET_AWAIT):
            return self.__client.get_kv_desc(key, target_uuid, self.get_uuid(), ret)
            
        def set_kv(self, target_uuid : uuid.UUID, key : bytes, val: bytes, ret = KVP_RET_AWAIT):
            return self.__client.set_kv(key, val, target_uuid, self.get_uuid(), ret)
        
        def get_subsystem(self, target_uuid : uuid.UUID, ret = KVP_RET_AWAIT):
            return self.__client.get_subsystem(target_uuid, self.get_uuid(), ret)
            
        def get_kv_descriptors(self):
            r = []

            for (_, kvp) in self.__kv_providers.items():
                r.append(kvp.get_type_descriptor(self.get_uuid()))
            
            return segment_bytes.encode(r)
        
        def get_event_descriptors(self):
            h = []
            p = []

            for (_, e) in self.__event_handlers.items():
                h.append(e.get_descriptor(self.get_uuid()))

            for (_, e) in self.__event_providers.items():
                p.append(e.get_descriptor(self.get_uuid()))
            
            return segment_bytes.encode([segment_bytes.encode(p), segment_bytes.encode(h)])
        
        def get_kv_descriptor(self, requester: uuid.UUID, key : bytes):
            kvp = self.__kv_providers.get(key)

            if kvp is None:
                return None
            
            return kvp.get_type_descriptor(requester)
        
        def invalidate(self):
            self.__info = SubsystemInfo(self.__info.get_uuid(), self.__info.get_name(), self.__info.get_temporary(), self.get_kv_descriptors(), self.get_event_descriptors())
            self.__client.send_subsystem_info(self.__info)

        def reconnected(self):
            for item in self.__active_status_items.values():
                self.__client.send_status_item(self.get_uuid(), item)

        def get_system(self):
            return self.__client.get_system(self)
        
        def add_in_progress_event(self, e: _InProgressEvent):
            self.__in_progress_events[e.get_uuid()] = e

        def incoming_event(self, e: uuid.UUID, t: transactions.TransactionManager.IncomingTransactionHandle, s_uuid: uuid.UUID, name: bytes, param: bytes):
            e_h = self.__event_handlers.get(name)

            if e_h is None:
                t.ret(bytes([EVENT_REJ]) + E_DOES_NOT_HANDLE_EVENT)
                return

            self.__incoming_events[e] = (t, s_uuid, name)

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
                self.log("Received event return for an event that this subsystem did not send!", level="ERROR")
                return
            
            e.set_result(r_uuid, status, ret_value)

            if not e.is_in_progress():
                self.__in_progress_events.pop(e_uuid)

        def on_event_abort(self, e_uuid: uuid.UUID):
            e = self.__in_progress_events.get(e_uuid)

            if e is None:
                self.log("Received event abort for an event that this subsystem did not send!", level="ERROR")
                return
            
            if e.is_in_progress():
                e.abort()

            self.__in_progress_events.pop(e_uuid)

        def send_event_return(self, e_uuid: uuid.UUID, state: int, v: bytes):
            (t, s_uuid, name) = self.__incoming_events.get(e_uuid)

            if t is None:
                self.log("Received request to send event return for an event that this subsystem did not receive", level="ERROR")
                return
            
            t.ret(bytes([state]) + v)

        def send_event_feedback(self, e_uuid: uuid.UUID, state: int, v: bytes):
            (t, s_uuid, name) = self.__incoming_events.get(e_uuid)

            self.__client.send_event_feedback(e_uuid, s_uuid, state, v)

        def log(self, msg, level = "INFO", **data):
            if self.__logger is None:
                print(level, msg)
                return
            
            self.__logger.log(msg, level=level, l_type="SW", subsystem=self.get_info().get_name(), **data)

        def put_status_item(self, item: StatusItem):
            self.__client.send_status_item(self.get_uuid(), item)

            self.__active_status_items[item.get_code()] = item

        def clear_status_item(self, code: int):
            self.__client.clear_status_item(self.get_uuid(), code)

            self.__active_status_items.pop(code)

        def reset_status_items(self):
            self.__active_status_items.clear()

    def __init__(self, c_uuid : uuid.UUID, ip = "127.0.0.1", logger = None):
        self.__uuid = c_uuid
        self.__logger = logger

        self.__socket = tcp.TCPClientSocket()
        self.__socket.connect((ip, SERVER_PORT))
        #print("Connecting to: ", (ip, SERVER_PORT))
        self.__socket.start()

        self.__registered = self.REG_STATE_NOT_REGISTERED
        self.__subsystem_handles = dict()
        self.__active_subscribers = dict()

        self.__cached_subsystems = dict()

        self.__is_ready = False

        self.__transactions_msg_out_queue = queue.Queue()
        self.__transactions = transactions.TransactionManager(self.__transactions_msg_out_queue)

        self.__event_consumer = mt_events.EventConsumer()

        #pylint: disable=invalid-name
        self.__E_MESSAGE = self.__socket.on_receive().bind(self.__event_consumer)
        self.__E_CONNECTED = self.__socket.on_connect().bind(self.__event_consumer)
        self.__E_DISCONNECTED = self.__socket.on_disconnect().bind(self.__event_consumer)
        self.__E_TRANSACT_DATA_AVAIL = self.__transactions.on_send_data().bind(self.__event_consumer)
        self.__E_NEW_TRANSACT = self.__transactions.on_receive_transaction().bind(self.__event_consumer)

        self.__ready_awaiter = mt_events.Awaiter()

        self.__ready_event = mt_events.Event()
        self.__remote_subsystem_update_event = mt_events.Event()

        self.__handshake_received = False

        self.__daemon = daemon.Daemon()
        self.__daemon.add(self.__thread)
        self.__daemon.start()

    def when_ready(self):
        return self.__ready_awaiter.get_handle()

    def __receive(self):
        #pylint: disable=unbalanced-tuple-unpacking
        while not self.__socket.empty():
            d = self.__socket.get()

            if len(d) == 0:
                continue

            if d == bytes([MAGIC_HANDSHAKE_SERVER]):
                if self.__handshake_received:
                    raise IOError("Handshake on existing connection!")

                #print("Handshake received from ", self.__socket.remote())
                self.__handshake_received = True

            if not self.__handshake_received:
                raise IOError("Invalid handshake received!")
            
            try:
                if d[0] == MAGIC_TRANSACT:
                    self.__transactions.received(d[1:])
                elif d[0] == MAGIC_SUBSCRIBED_UPD:
                    s_uuid, key, val = segment_bytes.decode(d[1:])


                    for kvs in self.__active_subscribers[uuid.UUID(bytes=s_uuid)]:
                        if kvs.get_key() == key:
                            kvs.remote_set(val)
                elif d[0] == MAGIC_SYSTEM_UPD:
                    s_data = segment_bytes.decode(d[1:])
                    for b_data in s_data:
                        b_info, b_status = segment_bytes.decode(b_data)
                        info = SubsystemInfo.decode(b_info)
                        state = SubsystemStatus.decode(b_status)

                        self.__cached_subsystems[info.get_uuid()] = (info, state)

                        self.__remote_subsystem_update_event.call()
                elif d[0] == MAGIC_EVENT_RET:
                    b_s_uuid, b_r_uuid, b_e_uuid, b_status, ret_value = segment_bytes.decode(d[1:])
                    s_uuid = uuid.UUID(bytes=b_s_uuid)
                    r_uuid = uuid.UUID(bytes=b_r_uuid)
                    e_uuid = uuid.UUID(bytes=b_e_uuid)

                    status = int.from_bytes(b_status, byteorder="big")

                    s = self.__subsystem_handles.get(s_uuid)
                    if s is None:
                        self.__log("Received event return for event from subsystem not registered with this client", level="ERROR")
                        return
                    #print(s, status, ret_value)
                    s.on_event_return(e_uuid, r_uuid, status, ret_value)
            except Exception as e:
                self.__log(f"Error while parsing data: {d}", level="ERROR")

                for line in traceback.format_exception(type(e), value=e, tb=None):
                    self.__log(line.removesuffix('\n'), level="ERROR")

                raise


    def __receive_transact(self):
        # pylint: disable=unbalanced-tuple-unpacking
        t = self.__transactions.get_incoming()

        if t.get_data()[0] == TRANSACT_REQ_UUID:
            t.ret(self.__uuid.bytes)
        
        elif t.get_data()[0] == TRANSACT_CONN_READY:
            if self.__is_ready:
                raise IOError("Received ready transaction twice!")
            
            self.__ready()
            t.ret(self.__uuid.bytes)

        elif t.get_data()[0] == TRANSACT_RGET_KV:
            self.__rget_kv(t)

        elif t.get_data()[0] == TRANSACT_RSET_KV:
            self.__rset_kv(t)

        elif t.get_data()[0] == TRANSACT_RGET_KV_DESC:
            s_uuid, r_uuid, key = segment_bytes.decode(t.get_data()[1:])
            s = self.__subsystem_handles.get(uuid.UUID(bytes=s_uuid))
            if s is None:
                self.__log("Received request to get KV descriptor for subsystem not registered with this client", level="ERROR")
                t.ret(bytes([TRANSOP_STATE_REJ]) + E_SUBSYSTEM_NOT_FOUND)
                return

            desc = s.get_kv_descriptor(r_uuid, key)
            if desc is None:
                t.ret(bytes([TRANSOP_STATE_REJ]) + E_KVP_NOT_FOUND)
                return
            
            t.ret(bytes([TRANSOP_STATE_OK]) + desc)
        elif t.get_data()[0] == TRANSACT_RCALL_EVENT:
            b_s_uuid, b_r_uuid, b_e_uuid, name, param = segment_bytes.decode(t.get_data()[1:])
            s_uuid = uuid.UUID(bytes=b_s_uuid)
            r_uuid = uuid.UUID(bytes=b_r_uuid)
            e_uuid = uuid.UUID(bytes=b_e_uuid)

            s = self.__subsystem_handles.get(s_uuid)
            if s is None:
                self.__log("Received request to call event for subsystem not registered with this client", level="ERROR")
                t.ret(bytes([EVENT_REJ]) + E_SUBSYSTEM_NOT_FOUND)
                return
            
            s.incoming_event(e_uuid, t, r_uuid, name, param)
        else:
            t.nak()
    
    def __rget_kv(self, t: transactions.TransactionManager.IncomingTransactionHandle):
        # pylint: disable=unbalanced-tuple-unpacking
        (t_uuid, s_uuid, key) = segment_bytes.decode(t.get_data()[1:])

        t_uuid = uuid.UUID(bytes=t_uuid)
        s_uuid = uuid.UUID(bytes=s_uuid)

        if self.__subsystem_handles.get(t_uuid) is None:
            self.__log("Received request to get kv for subsystem not registered with this client", level="ERROR")
            t.ret(bytes([TRANSOP_STATE_REJ]) + E_SUBSYSTEM_NOT_FOUND)
            return


        p = self.__subsystem_handles[t_uuid].get_kvp(key)
        if p is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + E_KVP_NOT_FOUND)
            return

        state, data = p.remote_get(s_uuid)
        t.ret(bytes([state]) + data)

    def __rset_kv(self, t: transactions.TransactionManager.IncomingTransactionHandle):
        # pylint: disable=unbalanced-tuple-unpacking
        (t_uuid, s_uuid, key, value) = segment_bytes.decode(t.get_data()[1:])

        t_uuid = uuid.UUID(bytes=t_uuid)
        s_uuid = uuid.UUID(bytes=s_uuid)

        if self.__subsystem_handles.get(t_uuid) is None:
            self.__log("Received request to set kv for subsystem not registered with this client", level="ERROR")
            t.ret(bytes([TRANSOP_STATE_REJ]) + E_SUBSYSTEM_NOT_FOUND)
            return

        p = self.__subsystem_handles[t_uuid].get_kvp(key)
        if p is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + E_KVP_NOT_FOUND)
            return

        state, data = p.remote_set(s_uuid, value)
        t.ret(bytes([state]) + data)

    # pylint: disable=pointless-string-statement
    """def __r_event(self, t: transactions.TransactionManager.IncomingTransactionHandle):
        (t_uuid, s_uuid, e_name, value) = segmented_bytearray.decode(t.get_data()[1:])

        t_uuid = uuid.UUID(bytes=t_uuid)
        s_uuid = uuid.UUID(bytes=s_uuid)

        if self.__subsystem_handles.get(t_uuid) is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + E_SUBSYSTEM_NOT_FOUND)
            return

        p = self.__subsystem_handles[t_uuid].get_event_handler(e_name)

        if p is None:
            t.ret(bytes([TRANSOP_STATE_REJ]) + b"Subsystem does not handle specified event.")
            return

        state, data = p.call(value)
        t.ret(bytes([state]) + data)"""

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
            info = SubsystemInfo.decode(handle.get_data()[1:])

            if self.__subsystem_handles.get(info.get_uuid()) is None:
                self.__log(f"Subsystem {info.get_name()} not found but was registered?!", level="ERROR")
                return
            
            subsystem_handle = self.__subsystem_handles[info.get_uuid()]

            if handle.get_state() != transactions.TransactionManager.OutgoingTransactionHandle.STATE_RET:
                self.__log(f"Could not register subsystem: {subsystem_handle.get_info().get_name()}!", level="ERROR")
                return
            
            #self.__log(f"Registered subsystem: {subsystem_handle.get_info().get_name()}", level="DEBUG")

            subsystem_handle.reconnected()
            #print("Registered subsystem: ", subsystem_handle.get_info().get_name())


            #self.__registered_event.call()
            #self.__registered_awaiter.call(subsystem_handle.get_handle())


    def __ready(self):
        self.__ready_event.call()
        self.__is_ready = True

        for s in self.__subsystem_handles.values():
            s.invalidate()

        #self.__send_subsystem_infos()
        self.__refresh_subscriptions()

        self.__ready_awaiter.call()
        
    def close(self):
        #print("Shutting down socket")
        self.__socket.shutdown()

        while not self.__socket.is_closed():
            time.sleep(0.1)

        self.__daemon.stop()
        self.__socket.close()

    def ok(self):
        return not self.__socket.is_closed() and self.__daemon.is_ok()
    
    def register_subsystem(self, name: str, s_uuid: uuid.UUID, temporary = False):
        info = SubsystemInfo(s_uuid, name, temporary)
        subsystem_handle = self._RegisteredSubsystem(info, self, self.__logger)
        self.__subsystem_handles[s_uuid] = subsystem_handle

        if self.__is_ready:
            self.send_subsystem_info(info)

        return subsystem_handle.get_handle()

    
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
            self.__log("TRANSOP transaction has been NAK'd", level="ERROR")
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
            self.__log("TRANSOP transaction has been NAK'd", level="ERROR")
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

    def set_kv(self, key : str, val : bytes, t_uuid : uuid.UUID, s_uuid : uuid.UUID, ret_type = KVP_RET_AWAIT):
        return self.__transop(bytes([TRANSACT_SET_KV]) + segment_bytes.encode([t_uuid.bytes, s_uuid.bytes, key, val]), ret_type)

    def get_kv(self, key : str, t_uuid : uuid.UUID, s_uuid : uuid.UUID, ret_type = KVP_RET_AWAIT):
        return self.__transop(bytes([TRANSACT_GET_KV]) + segment_bytes.encode([t_uuid.bytes, s_uuid.bytes, key]), ret_type)
    
    def get_kv_desc(self, key : str, t_uuid : uuid.UUID, s_uuid : uuid.UUID, ret_type = KVP_RET_AWAIT):
        return self.__transop(bytes([TRANSACT_GET_KV_DESC]) + segment_bytes.encode([t_uuid.bytes, s_uuid.bytes, key]), ret_type, unpack_value=KVDescriptor.decode)
    
    def call_event(self, key : str, param : bytes, t_uuids : bytes, s_uuid : uuid.UUID, ret_type = KVP_RET_AWAIT):
        return self.__transop(bytes([TRANSACT_CALL_EVENT]) + segment_bytes.encode([t_uuids, s_uuid.bytes, key, param]), ret_type)
    
    def resolve(self, name : bytes, ret_type = KVP_RET_AWAIT):
        return self.__transop(bytes([TRANSACT_RESOLVE]) + segment_bytes.encode([name]), ret_type, unpack_value=lambda v: uuid.UUID(bytes=v))
    
    def get_status(self, s_uuid : uuid.UUID, ret_type = KVP_RET_AWAIT):
        return self.__transop(bytes([TRANSACT_GET_STATUS]) + segment_bytes.encode([s_uuid.bytes]), ret_type, unpack_value=SubsystemStatus.decode)
    
    def get_subsystem(self, t_uuid : uuid.UUID, s_uuid : uuid.UUID, ret_type = KVP_RET_AWAIT):
        def unpack(v):
            return _RemoteSubsystemHandle(self, SubsystemInfo.decode(v), self.__subsystem_handles[s_uuid])

        return self.__transop(bytes([TRANSACT_GET_SUBSYSTEM]) + segment_bytes.encode([t_uuid.bytes]), ret_type, unpack_value=unpack)
    
    def __send_subsystem_infos(self):
        for s in self.__subsystem_handles.values():
            self.send_subsystem_info(s.get_info())
    
    def send_subsystem_info(self, info):
        self.__transactions.send_transaction(bytes([TRANSACT_REG_SUBSYSTEM]) + info.encode()).then(self.__transact_status_change)

    def send_status_item(self, s_uuid: uuid.UUID, status: StatusItem):
        self.__socket.put(bytes([MAGIC_UPDATE_STATUS_ITEM]) + segment_bytes.encode([s_uuid.bytes, status.encode()]))

    def clear_status_item(self, s_uuid: uuid.UUID, code: int):
        self.__socket.put(bytes([MAGIC_CLEAR_STATUS_ITEM]) + segment_bytes.encode([s_uuid.bytes, code.to_bytes(length=1, byteorder="big")]))

    def __refresh_subscriptions(self):
        for l in self.__active_subscribers.values():
            for kv in l:
                self.__socket.put(bytes([MAGIC_REQ_SUBSCRIBE]) + segment_bytes.encode([kv.get_remote().bytes, kv.get_key()]))

    def _add_active_subscriber(self, kv: _RemoteProperty):
        if self.__active_subscribers.get(kv.get_remote()) is None:
            self.__active_subscribers[kv.get_remote()] = []

        self.__active_subscribers[kv.get_remote()].append(kv)
        self.__socket.put(bytes([MAGIC_REQ_SUBSCRIBE]) + segment_bytes.encode([kv.get_remote().bytes, kv.get_key()]))


    def get_registered(self):
        return self.__registered
    
    def get_system(self, subsystem : "DDSClient._RegisteredSubsystem"):
        ret = []
        for info, state in self.__cached_subsystems.values():
            ret.append((_RemoteSubsystemHandle(self, info, subsystem), state))

        return ret
    
    def on_remote_system_update(self):
        return self.__remote_subsystem_update_event
    
    def send_event_feedback(self, e_uuid: uuid.UUID, s_uuid: uuid.UUID, state: int, v: bytes):
        self.__socket.put(bytes([MAGIC_EVENT_FEEDBACK]) + segment_bytes.encode([s_uuid.bytes, e_uuid.bytes, state.to_bytes(length=1, byteorder="big"), v]))

    def __log(self, msg, level = "INFO", **data):
        if self.__logger is None:
            print(level, msg)
            return
        
        self.__logger.log(msg, level=level, l_type="SW", **data)

