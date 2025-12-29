import time
import uuid
import sys

import ipi_ecs.dds.client as client
import ipi_ecs.dds.subsystem as subsystem
import ipi_ecs.dds.types as types


def print_kvs(state, reason, value = None):
    print(f"GET KV Op resulted in state {state}, with value {value} and reason {reason}")

def print_set_kvs(state, reason, value = None):
    print(f"SET KV Op resulted in state {state}, with value {value} and reason {reason}")
remote_kv = None

def setup_subsystem(handle: client.SubsystemHandle):
    global remote_kv

    print("Registered:", handle.get_info().get_name())
    remote_kv = handle.add_remote_kv(b"test property handler", uuid.uuid3(uuid.NAMESPACE_OID, "1"), False)
    remote_kv.set_type(types.ByteTypeSpecifier())

m_client = client.DDSClient(uuid.uuid4())
m_client.register_subsystem(subsystem.SubsystemInfo(uuid.uuid3(uuid.NAMESPACE_OID, "2"), "my subsystem2")).then(setup_subsystem)
time.sleep(1)
#client.get_kv_await("test readonly property3", uuid.uuid3(uuid.NAMESPACE_OID, "1")).then(print_kvs)

while m_client.ok():
    time.sleep(0.1)
    print(remote_kv.value)
    remote_kv.value = b"set"