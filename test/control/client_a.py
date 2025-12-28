import time
import uuid
import sys

import ipi_ecs.control.subsystem as subsystem
import ipi_ecs.control.types as types
import ipi_ecs.control.client as client

p = None
def setup_subsystem(handle: client.SubsystemHandle):
    global p

    print("Registered:", handle.get_info().get_name())
          
    p = handle.add_kv(b"test property", cache=True)

    t = types.IntegerTypeSpecifier(0, 5)
    p.set_type(t)
    p.value = 0

m_client = client.ControlServerClient(uuid.uuid4())
m_client.register_subsystem(subsystem.SubsystemInfo(uuid.uuid3(uuid.NAMESPACE_OID, "1"), "my subsystem")).then(setup_subsystem)

time.sleep(1)
#p2 = client.add_kv(b"test property2")
#p2.value = b"my value2"

i = 0
while m_client.ok():
    time.sleep(1)
    p.value = i
    i += 1