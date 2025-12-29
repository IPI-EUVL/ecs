import ipi_ecs.core.segmented_bytearray as segmented_bytearray
from ipi_ecs.dds.magics import *

def encode(s : "PropertyTypeSpecifier"):
    return s.encode_type()

def decode(d : bytes):
    if d[0] == TYPE_UNSPEC:
        return PropertyTypeSpecifier.decode_type(d)
    elif d[0] == TYPE_BYTES:
        return ByteTypeSpecifier.decode_type(d)
    elif d[0] == TYPE_INT:
        return IntegerTypeSpecifier.decode_type(d)

class PropertyTypeSpecifier:
    def parse(self, data : bytes):
        return None

    def encode(self, data : any):
        return None
    def encode_type(self):
        return bytes([TYPE_UNSPEC])
    
    def decode_type(data : bytes):
        return PropertyTypeSpecifier()
    
class ByteTypeSpecifier(PropertyTypeSpecifier):
    def parse(self, data : bytes):
        return bytes(data)
    
    def encode(self, data : bytes):
        if type(data) != bytes:
            raise ValueError()
        
        return bytes(data)
    
    def encode_type(self):
        return bytes([TYPE_BYTES])
    
    def decode_type(data : bytes):
        return ByteTypeSpecifier()
    
class IntegerTypeSpecifier(PropertyTypeSpecifier):
    def __init__(self, r_min = None, r_max = None):
        self.__min = r_min
        self.__max = r_max

    def parse(self, data : bytes):
        if len(data) != 4:
            raise ValueError()
        
        v = int.from_bytes(data, byteorder="big", signed=True)

        if (self.__max is not None and v > self.__max) or (self.__min is not None and v < self.__min):
            raise ValueError()
        
        return v
    
    def encode(self, data : int):
        if (self.__max is not None and data > self.__max) or (self.__min is not None and data < self.__min):
            raise ValueError()
        
        return data.to_bytes(byteorder="big", length=4, signed=True)
    
    def encode_type(self):
        if self.__min is not None:
            return bytes([TYPE_INT]) + segmented_bytearray.encode([self.__min.to_bytes(length=4, byteorder="big"), self.__max.to_bytes(length=4, byteorder="big")])
        else:
            return bytes([TYPE_INT])
        
    def decode_type(data : bytes):
        values = segmented_bytearray.decode(data[1:])

        if len(values) == 0:
            return IntegerTypeSpecifier()
        else:
            return IntegerTypeSpecifier(int.from_bytes(values[0], byteorder="big"), int.from_bytes(values[1], byteorder="big"))