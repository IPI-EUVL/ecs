import uuid

import ipi_ecs.core.segmented_bytearray as segmented_bytearray

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