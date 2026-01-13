import time
import uuid
import sys

import ipi_ecs.dds.subsystem as subsystem
import ipi_ecs.dds.types as types
import ipi_ecs.dds.client as client
import ipi_ecs.dds.magics as magics
import ipi_ecs.core.tcp as tcp

from ipi_ecs.logging.client import LogClient

p = None
e_p = None
e_h = None

rec_event_handle = None
def handle_set(h, requester, v):
    print("Set value handle", v)
    return (magics.TRANSOP_STATE_OK, bytes())

def handle_get(requester):
    print("Get value handle", requester)
    return (magics.TRANSOP_STATE_OK, b"MY VALUE")

def handle_event(s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
    global rec_event_handle
    print("called event handle", param)
    print("sender is", s_uuid)
    rec_event_handle = handle
    #handle.ret(b"MY BALUE")


def setup_subsystem(handle: client._RegisteredSubsystemHandle):
    global p, e_p, e_h

    if p is not None:
        return

    print("Registered:", handle.get_info().get_name())
          
    p = handle.get_kv_property(b"test property", cache=True)
    kv_h = handle.add_kv_handler(b"test property handler")
    e_p = handle.add_event_provider(b"test eventer")
    kv_h.on_set(handle_set)
    kv_h.on_get(handle_get)

    e_handler = handle.add_event_handler(b"test eventer") #Subsystems CAN handle events sent by themselves! (This is not a bug, totally a feature!!)
    e_handler.on_called(handle_event)

    e_h = e_p.call(b"I HAVE CALLED THE EVENT", [])
    if e_h is None:
        print("event has failed to call*")
    else:
        e_h.after().catch(lambda state, reason: print("Problem with event: ", reason)).then(lambda h: print("Event has finished"))


    t = types.IntegerTypeSpecifier()
    p.set_type(t)
    p.value = 0

c_uuid = uuid.uuid4()

sock = tcp.TCPClientSocket()

sock.connect(("127.0.0.1", 11751))
sock.start()

logger = LogClient(sock, origin_uuid=c_uuid)

m_client = client.DDSClient(c_uuid, logger=logger)
s = None

def print_sys_data(self):
    if self.__subsystem is None:
        return

    sys_dat = self.__subsystem.get_all()

    for i, _ in sys_dat:
        print("Found subsystem: ", i.get_info().get_name())
        print("UUID: ", i.get_info().get_uuid())
        print("Is temporary: ", i.get_info().get_temporary())

        for kv in i.get_info().get_kvs():
            print(f"Provides KV: {kv.get_key().decode()} R:{kv.get_readable()} W:{kv.get_writable()} P:{kv.get_published()}")

        ps, hs = i.get_info().get_events()

        for p in ps:
            print("Provides Event: ", p.get_name())

        for h in hs:
            print("Handles Event: ", h.get_name())

def reg_s():
    global s

    if s is not None:
        return
    
    print("REGISTER EEJEJE")
    s = m_client.register_subsystem("my subsystem", uuid.uuid3(uuid.NAMESPACE_OID, "1"))
    setup_subsystem(s)

m_client.when_ready().then(reg_s)
time.sleep(1)

s.put_status_item(subsystem.StatusItem(subsystem.StatusItem.STATE_WARN, 0, "MY STATUS ITEM HAS BEEN PUSHED"))

cli_uuid = None
def set_cli_uuid(val):
    global cli_uuid
    cli_uuid = val

m_client.resolve(b"__cli").then(set_cli_uuid).catch(lambda state, reason: print("Could not resolve:", reason))

time.sleep(1)

print(e_h.get_event_state())
print(e_h.is_in_progress())
print(e_h.get_uuid())
print(e_h.get_result(uuid.uuid3(uuid.NAMESPACE_OID, "1")))
print(e_h.get_result(cli_uuid))
print(e_h.get_state(uuid.uuid3(uuid.NAMESPACE_OID, "1")))
print(e_h.get_state(cli_uuid))

time.sleep(1)
rec_event_handle.ret(b"MY RETURN VALUE")
time.sleep(0.1)
print(e_h.get_event_state())
print(e_h.is_in_progress())
print(e_h.get_uuid())
print(e_h.get_result(uuid.uuid3(uuid.NAMESPACE_OID, "1")))
print(e_h.get_result(cli_uuid))
print(e_h.get_state(uuid.uuid3(uuid.NAMESPACE_OID, "1")))
print(e_h.get_state(cli_uuid))

time.sleep(10)
print("AAAAA")
s.clear_status_item(0)
#p2 = client.add_kv(b"test property2")
#p2.value = b"my value2"

i = 0


try:
    while m_client.ok():
        time.sleep(1)
        p.value = i
        i += 1
except KeyboardInterrupt:
    pass
finally:
    m_client.close()