import uuid

import ipi_ecs.core.segmented_bytearray as segmented_bytearray
import ipi_ecs.dds.types as types

class SubsystemInfo:
    def __init__(self, s_uuid: uuid.UUID, name : str, temporary = False, kv_infos = segmented_bytearray.encode([]), events = segmented_bytearray.encode([])):
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
        kv_sep = segmented_bytearray.decode(self.__kv_infos)
        descs = []

        for kv_desc in kv_sep:
            descs.append(KVDescriptor.decode(kv_desc))

        return descs
    
    def get_events(self):
        if len(self.__events) == 0:
            return ([], [])
        
        b_providers, b_handlers = segmented_bytearray.decode(self.__events)
        providers = []
        handlers = []

        for desc in segmented_bytearray.decode(b_providers):
            providers.append(EventDescriptor.decode(desc))

        for desc in segmented_bytearray.decode(b_handlers):
            handlers.append(EventDescriptor.decode(desc))

        return (providers, handlers)
    
    def encode(self):
        return segmented_bytearray.encode([self.__uuid.bytes, self.__name.encode("utf-8"), self.__temporary.to_bytes(length=1, byteorder="big"), self.__kv_infos, self.__events])
    
    def decode(d_bytes : bytes):
        b_s_uuid, b_name, b_temporary, b_kv, b_events = segmented_bytearray.decode(d_bytes)
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
        return segmented_bytearray.encode([self.__p_type.encode_type(), self.__key, self.__published.to_bytes(length=1, byteorder="big"), self.__readable.to_bytes(length=1, byteorder="big"), self.__writable.to_bytes(length=1, byteorder="big")])
    
    def decode(d_bytes : bytes):
        b_type, key, b_pub, b_read, b_write = segmented_bytearray.decode(d_bytes)
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
        return segmented_bytearray.encode([self.__p_type.encode_type(),self.__r_type.encode_type(), self.__name])
    
    def decode(d_bytes : bytes):
        b_ptype, b_rtype, name = segmented_bytearray.decode(d_bytes)
        s_ptype = types.decode(b_ptype)
        s_rtype = types.decode(b_rtype)

        return EventDescriptor(s_ptype, s_rtype, name)