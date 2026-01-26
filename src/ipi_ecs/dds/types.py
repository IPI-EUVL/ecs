import struct
import segment_bytes
from ipi_ecs.dds.magics import *

class TypeManager:
    def __init__(self):
        self.__identifiers = dict()
        self.__types = dict()
        self.__next_identifier = 0

    def define_type(self, t_type):
        if self.__identifiers.get(t_type) is not None:
            return False
        
        self.__identifiers[t_type] = self.__next_identifier
        self.__types[self.__next_identifier] = t_type
        self.__next_identifier += 1

        return True

    def get_identifier(self, t_type) -> int:
        return self.__identifiers.get(t_type)
    
    def get_type(self, identifier):
        return self.__types.get(identifier)

class PropertyTypeSpecifier:
    def parse(self, data : bytes):
        return None

    def encode(self, data : any):
        return None
    
    def encode_type(self):
        return bytes()
    
    @staticmethod
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
        return bytes()
    
    @staticmethod
    def decode_type(data : bytes):
        return ByteTypeSpecifier()
    
class IntegerTypeSpecifier(PropertyTypeSpecifier):
    def __init__(self, r_min = None, r_max = None):
        self.__min = r_min
        self.__max = r_max

    def parse(self, data : bytes):
        if len(data) != 8:
            raise ValueError()
        
        v = int.from_bytes(data, byteorder="big", signed=True)

        if (self.__max is not None and v > self.__max) or (self.__min is not None and v < self.__min):
            raise ValueError()
        
        return v
    
    def encode(self, data : int):
        if (self.__max is not None and data > self.__max) or (self.__min is not None and data < self.__min):
            raise ValueError()
        
        return data.to_bytes(byteorder="big", length=8, signed=True)
    
    def encode_type(self):
        if self.__min is not None:
            return segment_bytes.encode([self.__min.to_bytes(length=8, byteorder="big"), self.__max.to_bytes(length=8, byteorder="big")])
        else:
            return bytes()
    
    @staticmethod
    def decode_type(data : bytes):
        values = segment_bytes.decode(data)

        if len(values) == 0:
            return IntegerTypeSpecifier()
        else:
            return IntegerTypeSpecifier(int.from_bytes(values[0], byteorder="big"), int.from_bytes(values[1], byteorder="big"))
        
class FloatTypeSpecifier(PropertyTypeSpecifier):
    def __init__(self, r_min = None, r_max = None):
        self.__min = r_min
        self.__max = r_max

    def parse(self, data : bytes):
        if len(data) != 8:
            raise ValueError()
        
        v = struct.unpack("d", data)[0]

        if (self.__max is not None and v > self.__max) or (self.__min is not None and v < self.__min):
            raise ValueError()
        
        return v
    
    def encode(self, data : float):
        if (self.__max is not None and data > self.__max) or (self.__min is not None and data < self.__min):
            raise ValueError()
        
        return struct.pack("d", data)
    
    def encode_type(self):
        if self.__min is not None:
            return segment_bytes.encode([struct.pack("d", self.__min), struct.pack("d", self.__max)])
        else:
            return bytes()
    
    @staticmethod
    def decode_type(data : bytes):
        values = segment_bytes.decode(data)

        if len(values) == 0:
            return FloatTypeSpecifier()
        else:
            return FloatTypeSpecifier(struct.unpack("d", values[0])[0], struct.unpack("d", values[1])[0])
        
class VectorTypeSpecifier(PropertyTypeSpecifier):
    def __init__(self, element_type : PropertyTypeSpecifier, length : int):
        self.__element_type = element_type
        self.__length = length
    
    def parse(self, data : bytes):
        elements = segment_bytes.decode(data)
        if len(elements) != self.__length:
            raise ValueError()
        
        result = []
        for e in elements:
            result.append(self.__element_type.parse(e))
        
        return result
    
    def encode(self, data : list):
        if len(data) != self.__length:
            raise ValueError()
        
        encoded_elements = []
        for e in data:
            encoded_elements.append(self.__element_type.encode(e))
        
        return segment_bytes.encode(encoded_elements)
    
    def encode_type(self):
        element_type_data = self.__element_type.encode_type()
        length_data = self.__length.to_bytes(length=4, byteorder="big")

        return segment_bytes.encode([element_type_data, length_data])
    
    @staticmethod
    def decode_type(data : bytes):
        datas = segment_bytes.decode(data)

        element_type = PropertyTypeSpecifier.decode_type(datas[0])
        length = int.from_bytes(datas[1], byteorder="big")

        return VectorTypeSpecifier(element_type, length)

types = TypeManager()
types.define_type(ByteTypeSpecifier)
types.define_type(IntegerTypeSpecifier)
types.define_type(FloatTypeSpecifier)
types.define_type(VectorTypeSpecifier)

def encode(s : "PropertyTypeSpecifier"):
    type_data = s.encode_type()
    identifier = bytes([types.get_identifier(type(s))])

    return segment_bytes.encode([identifier, type_data])

def decode(d : bytes):
    datas = segment_bytes.decode(d)


    identifier = int.from_bytes(datas[0], byteorder="big")
    data = datas[1]

    p_type = types.get_type(identifier)
    return p_type.decode_type(data)