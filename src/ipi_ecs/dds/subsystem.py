import uuid
import segment_bytes

import ipi_ecs.dds.types as types

# pylint: disable=unbalanced-tuple-unpacking

class SubsystemInfo:
    def __init__(self, s_uuid: uuid.UUID, name : str, temporary = False, kv_infos = segment_bytes.encode([]), events = segment_bytes.encode([])):
        self.__uuid = s_uuid
        self.__name = name
        self.__temporary = temporary
        self.__kv_infos = kv_infos
        self.__events = events
    
    def get_uuid(self):
        return self.__uuid
    
    def get_name(self):
        return self.__name
    
    def get_temporary(self):
        return self.__temporary
    
    def get_kvs(self):
        kv_sep = segment_bytes.decode(self.__kv_infos)
        descs = []

        for kv_desc in kv_sep:
            descs.append(KVDescriptor.decode(kv_desc))

        return descs
    
    def get_events(self):
        if len(self.__events) == 0:
            return ([], [])
        
        b_providers, b_handlers = segment_bytes.decode(self.__events)
        providers = []
        handlers = []

        for desc in segment_bytes.decode(b_providers):
            providers.append(EventDescriptor.decode(desc))

        for desc in segment_bytes.decode(b_handlers):
            handlers.append(EventDescriptor.decode(desc))

        return (providers, handlers)
    
    def encode(self):
        return segment_bytes.encode([self.__uuid.bytes, self.__name.encode("utf-8"), self.__temporary.to_bytes(length=1, byteorder="big"), self.__kv_infos, self.__events])
    
    @staticmethod
    def decode(d_bytes : bytes):
        b_s_uuid, b_name, b_temporary, b_kv, b_events = segment_bytes.decode(d_bytes)
        s_uuid = uuid.UUID(bytes=b_s_uuid)
        name = b_name.decode("utf-8")
        temporary = bool.from_bytes(b_temporary, "big")

        return SubsystemInfo(s_uuid, name, temporary, b_kv, b_events)
    
class KVDescriptor:
    def __init__(self, p_type: types.PropertyTypeSpecifier, key : bytes, published = False, readable = True, writable = True):
        self.__p_type = p_type
        self.__key = key
        self.__published = published

        self.__readable = readable
        self.__writable = writable
    
    def get_type(self):
        return self.__p_type
    
    def get_key(self):
        return self.__key
    
    def get_published(self):
        return self.__published
    
    def get_readable(self):
        return self.__readable
    
    def get_writable(self):
        return self.__writable
    
    def encode(self):
        return segment_bytes.encode([types.encode(self.__p_type), self.__key, self.__published.to_bytes(length=1, byteorder="big"), self.__readable.to_bytes(length=1, byteorder="big"), self.__writable.to_bytes(length=1, byteorder="big")])
    
    @staticmethod
    def decode(d_bytes : bytes):
        b_type, key, b_pub, b_read, b_write = segment_bytes.decode(d_bytes)
        s_type = types.decode(b_type)
        s_pub = bool.from_bytes(b_pub, "big")

        s_read = bool.from_bytes(b_read, "big")
        s_write = bool.from_bytes(b_write, "big")

        return KVDescriptor(s_type, key, s_pub, s_read, s_write)
    
class EventDescriptor:
    def __init__(self, p_type: types.PropertyTypeSpecifier, r_type: types.PropertyTypeSpecifier, name : bytes):
        self.__p_type = p_type
        self.__r_type = r_type
        self.__name = name
    
    def get_parameter_type(self):
        return self.__p_type
    
    def get_return_type(self):
        return self.__r_type
    
    def get_name(self):
        return self.__name
    
    def encode(self):
        return segment_bytes.encode([types.encode(self.__p_type), types.encode(self.__r_type), self.__name])
    
    @staticmethod
    def decode(d_bytes : bytes):
        b_ptype, b_rtype, name = segment_bytes.decode(d_bytes)
        s_ptype = types.decode(b_ptype)
        s_rtype = types.decode(b_rtype)

        return EventDescriptor(s_ptype, s_rtype, name)
    
class StatusItem:
    STATE_INFO = 0
    STATE_WARN = 1
    STATE_ALARM = 2

    def __init__(self, severity : int, code: int, message: str):
        self.__severity = severity
        self.__code = code
        self.__message = message

    def get_message(self):
        return self.__message
    
    def get_severity(self):
        return self.__severity
    
    def get_code(self):
        return self.__code
    
    def encode(self):
        return self.__severity.to_bytes(length=1, byteorder="big") + self.__code.to_bytes(length=1, byteorder="big") + self.__message.encode("utf-8")
    
    @staticmethod
    def decode(b : bytes):
        severity = int.from_bytes(bytes([b[0]]), byteorder="big")
        code = int.from_bytes(bytes([b[1]]), byteorder="big")

        message = b[2:].decode("utf-8")

        return StatusItem(severity, code, message)
    
class SubsystemStatus:
    STATE_ALIVE = 0
    STATE_DISCONNECTED = 1

    def __init__(self, status : int, status_items: list | None = None):
        self.__status = status
        self.__status_items = status_items if status_items is not None else []

    def get_status(self):
        return self.__status
    
    def get_status_items(self):
        return self.__status_items
    
    def encode(self):
        b_status = []
        for s in self.__status_items:
            b_status.append(s.encode())
        
        return self.__status.to_bytes(length=1, byteorder="big") + segment_bytes.encode(b_status)
    
    @staticmethod
    def decode(b : bytes):
        status = int.from_bytes(bytes([b[0]]), byteorder="big")
        b_status_items = segment_bytes.decode(b[1:])
        status_items = []

        for b_status_item in b_status_items:
            status_items.append(StatusItem.decode(b_status_item))
        
        return SubsystemStatus(status, status_items)