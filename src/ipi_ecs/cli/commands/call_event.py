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

class CallEventClient:
    def __init__(self, e_name: str, data: bytes):
        self.__e_name = e_name.encode("utf-8")
        self.__data = data
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

        self.__event_handle = None
        self.__failure_reason = None
        self.__failure_state = None

        self.__on_data = mt_events.Event()

    def __on_got_subsystem(self, handle: client._RegisteredSubsystemHandle):
        self.__subsystem = handle
        self.__event_provider = self.__subsystem.add_event_provider(self.__e_name)
        self.__event_handle = self.__event_provider.call(self.__data, [])
        self.__event_handle.after().then(self.__event_return).catch(self.__event_fail)
        self.__event_handle.on_data().chain(self.__on_data)

    def __event_return(self, handle: client._InProgressEvent._Handle):
        self.__nd_event.call()

    def __event_fail(self, reason: str, state: int):
        self.__failure_reason = reason
        self.__failure_state = state
        self.__nd_event.call()

    def get_event_handle(self) -> client._InProgressEvent._Handle:
        return self.__event_handle
    
    def get_event_state(self):
        if self.__event_handle is None:
            return magics.EVENT_PENDING
        
        return self.__event_handle.get_event_state()
    
    def get_failure_reason(self):
        return self.__failure_reason if self.__failure_reason is not None else ""

    def ok(self):
        return self.__run and self.__client.ok()
    
    def on_new_data(self):
        return self.__nd_event

    def close(self):
        self.__client.close()
        self.__logger_sock.close()

        self.__run = False

    def on_data(self):
        return self.__on_data

def main(args: argparse.Namespace):
    m_client = CallEventClient(args.event, args.data.encode("utf-8"))

    m_awaiter = mt_events.EventConsumer()
    nd_e = m_client.on_new_data().bind(m_awaiter)
    ret_e = m_client.on_data().bind(m_awaiter)

    last_states = dict()
    last_feedback = dict()

    try:
        while m_client.ok():
            e = m_awaiter.get()

            if m_client.get_event_state() == magics.EVENT_REJ:
                print(f"Event failed with state {m_client.get_event_state()} and reason: {m_client.get_failure_reason()}")

            if e == ret_e:
                states = m_client.get_event_handle().get_states().items()
                for s_uuid, (state, reason) in states:
                    if (s_uuid in last_states and last_states[s_uuid] == state) and (s_uuid in last_feedback and last_feedback[s_uuid] == reason):
                        continue

                    if state == magics.EVENT_IN_PROGRESS:
                        if s_uuid not in last_feedback and reason is not None:
                            print(f"Subsystem {s_uuid} has begun processing event: {reason.decode('utf-8')}")
                        elif last_feedback.get(s_uuid) != reason and reason is not None:
                            print(f"Subsystem {s_uuid} is still in progress: {reason.decode('utf-8')}")
                    elif state == magics.EVENT_OK:
                        print(f"Subsystem {s_uuid} completed successfully.")
                    elif state == magics.EVENT_REJ:
                        if reason != magics.E_DOES_NOT_HANDLE_EVENT and reason != magics.E_SUBSYSTEM_DISCONNECTED:
                            print(f"Subsystem {s_uuid} failed: {reason.decode('utf-8')}")
                        else:
                            pass# print(f"Subsystem {s_uuid} does not handle this event.")

                    last_states[s_uuid] = state
                    last_feedback[s_uuid] = reason
            if e == nd_e and not m_client.get_event_handle().is_in_progress():
                print("Event has concluded.")
                states = m_client.get_event_handle().get_states().items()
                for s_uuid, (state, reason) in states:
                    if state == magics.EVENT_OK:
                        print(f"Subsystem {s_uuid}: {m_client.get_event_handle().get_result(s_uuid)}")
                    elif reason != magics.E_DOES_NOT_HANDLE_EVENT and reason != magics.E_SUBSYSTEM_DISCONNECTED:
                        print(f"Subsystem {s_uuid} did not complete successfully: {state} - {reason.decode('utf-8')}")

                break

    except KeyboardInterrupt:
        pass
    finally:
        m_client.close()

    return 0