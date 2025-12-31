import uuid

import ipi_ecs.core.segmented_bytearray as segmented_bytearray
import ipi_ecs.dds.types as types

class SubsystemInfo:
    def __init__(self, s_uuid: uuid.UUID, name : str, temporary = False):
        self.__uuid = s_uuid
        self.__name = name
        self.__temporary = temporary
    
    def get_uuid(self):
        return self.__uuid
    
    def get_name(self):
        return self.__name
    
    def get_temporary(self):
        return self.__temporary
    
    def encode(self):
        return segmented_bytearray.encode([self.__uuid.bytes, self.__name.encode("utf-8"), self.__temporary.to_bytes(length=1, byteorder="big")])
    
    def decode(d_bytes : bytes):
        b_s_uuid, b_name, b_temporary = segmented_bytearray.decode(d_bytes)
        s_uuid = uuid.UUID(bytes=b_s_uuid)
        name = b_name.decode("utf-8")
        temporary = bool.from_bytes(b_temporary, "big")

        return SubsystemInfo(s_uuid, name, temporary)
    
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