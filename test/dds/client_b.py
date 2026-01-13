import time
import uuid
import sys
import segment_bytes

import ipi_ecs.dds.client as client
import ipi_ecs.dds.subsystem as subsystem
import ipi_ecs.dds.types as types


def print_kvs(value = None):
    print(f"GET KV Op resulted in value {value}")

def handle_event(s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
    global rec_event_handle
    print("called event handle", param)
    print("sender is", s_uuid)
    rec_event_handle = handle

remote_kv = None

def setup_subsystem(handle: client._RegisteredSubsystemHandle):
    global remote_kv

    print("Registered:", handle.get_info().get_name())
    remote_kv = handle.add_remote_kv(uuid.uuid3(uuid.NAMESPACE_OID, "1"), subsystem.KVDescriptor(types.ByteTypeSpecifier(), b"test property handler"))
    #remote_kv.set_type(types.IntegerTypeSpecifier())
    handle.get_kv_desc(uuid.uuid3(uuid.NAMESPACE_OID, "1"), b"test property handler").then(print_kvs)

    e_handler = handle.add_event_handler(b"test eventer") #Subsystems CAN handle events sent by themselves! (This is not a bug, totally a feature!!)
    e_handler.on_called(handle_event)


m_client = client.DDSClient(uuid.uuid4())
m_client.register_subsystem("my other subsystem", uuid.uuid3(uuid.NAMESPACE_OID, "2")).then(setup_subsystem)
time.sleep(1)
#client.get_kv_await("test readonly property3", uuid.uuid3(uuid.NAMESPACE_OID, "1")).then(print_kvs)

while m_client.ok():
    time.sleep(0.1)
    print(remote_kv.value)
    remote_kv.value = b"set"