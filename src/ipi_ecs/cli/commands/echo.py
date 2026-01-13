import time
import uuid
import sys
import argparse
import segment_bytes
import mt_events

import ipi_ecs.dds.client as client
import ipi_ecs.core.tcp as tcp
import ipi_ecs.dds.subsystem as subsystem
import ipi_ecs.dds.types as types
import ipi_ecs.dds.magics as magics

from ipi_ecs.logging.client import LogClient

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
        c_uuid = uuid.uuid4()

        self.__logger_sock = tcp.TCPClientSocket()

        self.__logger_sock.connect(("127.0.0.1", 11751))
        self.__logger_sock.start()

        self.__logger = LogClient(self.__logger_sock, origin_uuid=c_uuid)

        self.__did_config = False
        self.__subsystem = None

        def _on_ready():
            if self.__did_config:
                return
            
            self.__did_config = True
            sh = self.__client.register_subsystem("__cli", uuid.uuid4(), temporary=True)

            self.__on_got_subsystem(sh)

        #print("Registering subsystem...")
        self.__client = client.DDSClient(c_uuid, logger=self.__logger)
        self.__client.when_ready().then(_on_ready)
        

    def __on_got_subsystem(self, handle: client._RegisteredSubsystemHandle):
        # state has to be named state as it will get passed as a KW argument, stop complaining Pylint!!
        def fail(state, reason): #pylint: disable=unused-argument
            print(f"Failed: {reason}")
            self.__run = False

        def __setup_kv(s_uuid):
            handle.get_subsystem(s_uuid).then(lambda subsystem: subsystem.get_kv(self.__key).then(self.__on_got_kv).catch(fail)).catch(fail)

        #pylint: disable=pointless-string-statement

        self.__subsystem = handle

        if self.__target is not None:
            __setup_kv(self.__target)
        else:
            self.__client.resolve(self.__t_name).then(__setup_kv).catch(fail)

    def __on_got_kv(self, value):
        self.__remote_kv = value
        self.__remote_kv.on_new_data_received(lambda v: self.__nd_event.call())

    def get_value(self):
        return self.__remote_kv.value if self.__remote_kv is not None else None
    
    def ok(self):
        return self.__run and self.__client.ok()
    
    def on_new_data(self, c : mt_events.EventConsumer):
        return self.__nd_event.bind(c)

    def close(self):
        self.__client.close()
        self.__logger_sock.close()

        self.__run = False

    def is_cached(self):
        return self.__remote_kv.is_cached() if self.__remote_kv is not None else True

def main(args: argparse.Namespace):
    m_client = EchoClient(args.name, args.sys, args.key)

    m_awaiter = mt_events.EventConsumer()
    nd_e = m_client.on_new_data(m_awaiter)
    hz = args.hz if args.hz is not None else None

    try:
        while m_client.ok():
            if m_client.is_cached():
                if hz is not None and m_client.get_value() is not None:
                    print("--hz set, but property is published. Values will be displayed whenever the originator sends them regardless of desired rate.")
                    hz = None

                e = m_awaiter.get(timeout=0.1)
                if e == nd_e:
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