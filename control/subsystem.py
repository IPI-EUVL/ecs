import uuid

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