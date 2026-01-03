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

def print_transop(state, reason, value = None):
    print(f"GET KV Op resulted in state {state}, with value {value} and reason {reason}")

class EchoClient:
    def __init__(self, t_name, target__uuid, key):
        self.__target = uuid.UUID(target__uuid) if target__uuid is not None else None
        self.__t_name = t_name.encode("utf-8") if t_name is not None else None
        self.__key = key.encode("utf-8")
        self.__run = True

        self.__remote_kv = None

        self.__nd_event = mt_events.Event()

        #print("Registering subsystem...")
        self.__client = client.DDSClient(uuid.uuid4())
        self.__client.register_subsystem("__cli", uuid.uuid4(), temporary=True).then(self.__on_got_subsystem)

    def __on_got_subsystem(self, handle: client.RegisteredSubsystemHandle):
        def __setup_kv(uuid):
            handle.get_subsystem(uuid).then(lambda subsystem: subsystem.get_kv(self.__key).then(self.__on_got_kv))

        sys = handle.get_all()

        for i, ok in sys.values():
            print("Found subsystem: ", i.get_name())
            print("UUID: ", i.get_uuid())
            print("Is connected: ", ok)
            print("Is temporary: ", i.get_temporary())

            for kv in i.get_kvs():
                print(f"Provides KV: {kv.get_key().decode()} R:{kv.get_readable()} W:{kv.get_writable()} P:{kv.get_published()}")

            ps, hs = i.get_events()

            for p in ps:
                print("Provides Event: ", p.get_name())

            for h in hs:
                print("Handles Event: ", h.get_name())

        if self.__target is not None:
            __setup_kv(self.__target)
        else:
            self.__client.resolve(self.__t_name).then(__setup_kv)

    def __on_got_kv(self, value):
        self.__remote_kv = value
        self.__remote_kv.on_new_data_received(lambda v: self.__nd_event.call())

    def get_value(self):
        return self.__remote_kv.value if self.__remote_kv is not None else None
    
    def ok(self):
        return self.__run and self.__client.ok()
    
    def on_new_data(self, c : mt_events.EventConsumer, event):
        self.__nd_event.bind(c, event)

    def close(self):
        self.__client.close()

        self.__run = False

    def is_cached(self):
        return self.__remote_kv.is_cached() if self.__remote_kv is not None else True

def main(args: argparse.Namespace):
    print(args.name, args.sys, args.key)
    m_client = EchoClient(args.name, args.sys, args.key)

    m_awaiter = mt_events.EventConsumer()
    m_client.on_new_data(m_awaiter, 0)
    hz = args.hz if args.hz is not None else None

    try:
        while m_client.ok():
            if m_client.is_cached():
                if hz is not None and m_client.get_value() is not None:
                    print("--hz set, but property is cached. Values will be displayed whenever the originator sends them regardless of desired rate.")
                    hz = None

                e = m_awaiter.get(timeout=0.1)
                if e == 0:
                    print(m_client.get_value())
            else:
                if hz is None:
                    hz = 1
                print(m_client.get_value())
                time.sleep(1 / hz)
    except KeyboardInterrupt:
        pass
    finally:
        m_client.close()

    return 0