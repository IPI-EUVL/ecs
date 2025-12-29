import time
import uuid
import sys

import ipi_ecs.dds.subsystem as subsystem
import ipi_ecs.dds.types as types
import ipi_ecs.dds.client as client
import ipi_ecs.dds.magics as magics

p = None
def handle_set(h, v):
    print("Set value handasas", v)
    return (magics.KV_STATE_OK, bytes())

def handle_get(h):
    print("Get value handasas")
    return (magics.KV_STATE_OK, b"MY VALUE")


def setup_subsystem(handle: client.SubsystemHandle):
    global p

    print("Registered:", handle.get_info().get_name())
          
    p = handle.get_kv_property(b"test property", cache=True)
    kv_h = handle.add_kv_handler(b"test property handler")
    kv_h.on_set(handle_set)
    kv_h.on_get(handle_get)

    t = types.IntegerTypeSpecifier(0, 5)
    p.set_type(t)
    p.value = 0

m_client = client.DDSClient(uuid.uuid4())
m_client.register_subsystem(subsystem.SubsystemInfo(uuid.uuid3(uuid.NAMESPACE_OID, "1"), "my subsystem")).then(setup_subsystem)

time.sleep(1)
#p2 = client.add_kv(b"test property2")
#p2.value = b"my value2"

i = 0
while m_client.ok():
    time.sleep(1)
    p.value = i
    i += 1