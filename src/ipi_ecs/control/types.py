import ipi_ecs.core.segmented_bytearray as segmented_bytearray
from ipi_ecs.control.magics import *

class PropertyTypeSpecifier:
    def parse(self, data : bytes):
        return None

    def encode(self, data : any):
        return None
    def encode_type(self):
        return bytes([TYPE_UNSPEC])
    
class ByteTypeSpecifier(PropertyTypeSpecifier):
    def parse(self, data : bytes):
        return bytes(data)
    
    def encode(self, data : bytes):
        if type(data) != bytes:
            raise ValueError()
        
        return bytes(data)
    
    def encode_type(self):
        return bytes([TYPE_BYTES])
    
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
        return data.to_bytes(byteorder="big", length=4, signed=True)
    
    def encode_type(self):
        if self.__min is not None:
            return segmented_bytearray.encode(bytes([TYPE_INT]), bytes([self.__min]), bytes([self.__max]))
        else:
            return bytes([TYPE_INT])