import time
import uuid
import sys
import argparse

import ipi_ecs.dds.client as client
import ipi_ecs.dds.subsystem as subsystem
import ipi_ecs.dds.types as types
import ipi_ecs.dds.magics as magics
import ipi_ecs.core.segmented_bytearray as segmented_bytearray
import ipi_ecs.core.mt_events as mt_events

class EchoClient:
    def __init__(self, target__uuid, key):
        self.__target = target__uuid
        self.__key = key
        self.__run = True

        self.__remote_kv = None

        self.__nd_event = mt_events.Event()

        print("Registering subsystem...")
        self.__client = client.DDSClient(uuid.uuid4())
        self.__client.register_subsystem(subsystem.SubsystemInfo(uuid.uuid4(), "__cli")).then(self.__on_got_subsystem)

        self.__subsystem = None

    def __on_got_subsystem(self, handle: client.SubsystemHandle):
        print("Registered:", handle.get_info().get_name())
        self.__subsystem = handle
        #remote_kv.set_type(types.IntegerTypeSpecifier())
        self.__subsystem.get_kv_desc(self.__target, self.__key).then(self.__on_got_descriptor)

    def __on_got_descriptor(self, state, reason, value = None):
        if value is None:
            print("Could not get descriptor due to: ", reason)
            self.__run = False
            return
        
        desc, key, is_cached = segmented_bytearray.decode(value)
        self.__type = types.decode(desc)
        assert key == self.__key
        self.__is_cached = bool.from_bytes(is_cached, byteorder="big")

        self.__remote_kv = self.__subsystem.add_remote_kv(self.__key, self.__target, self.__is_cached)
        self.__remote_kv.set_type(self.__type)
        self.__remote_kv.on_new_data_received(self.__on_rcv_new_data)

    def get_value(self):
        if self.__remote_kv is None:
            return None
        
        return self.__remote_kv.value
    
    def ok(self):
        return self.__run and self.__client.ok()
    
    def __on_rcv_new_data(self, v):
        self.__nd_event.call()

    def on_new_data(self, c : mt_events.EventConsumer, event):
        self.__nd_event.bind(c, event)

def main(args: argparse.Namespace):
    m_client = EchoClient(uuid.UUID(args.sys), args.key.encode("utf-8"))

    m_awaiter = mt_events.EventConsumer()
    m_client.on_new_data(m_awaiter, 0)

    while m_client.ok():
        e = m_awaiter.get(timeout=0.1)
        if e == 0:
            print(m_client.get_value())

    return 0