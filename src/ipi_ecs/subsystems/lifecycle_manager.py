import time
import uuid
import multiprocessing
import traceback

import segment_bytes
import mt_events

# This is stupid, all of these are "third-party"
# Why are "mt_events" and "segment_bytes" being detected as first party?!
# pylint:disable=wrong-import-order
import ipi_ecs.dds.client as client
import ipi_ecs.core.tcp as tcp
import ipi_ecs.core.daemon as daemon
import ipi_ecs.dds.subsystem as subsystem
import ipi_ecs.dds.types as types

from ipi_ecs.logging.client import LogClient
from ipi_ecs.dds.magics import OP_OK

class magics:
    E_START_FAILED = b"Subsystem failed to start."
    E_STARTS_FAILED = b"One or more subsystems failed to start."
    E_NXUUID = b"Specified uuid is invalid, not found, or not a managed subsystem."
    E_ALREADY_IN_PROGRESS = b"Operation already in progress."

class SubsystemRuntimeState:
    started = False
    initializing = False
    connected = False
    warn = False
    error = False
    process_running = False
    managed = False

    name = None

    def encode(self):
        s_bytes = bytes()
        s_bytes += self.started.to_bytes(length=1, byteorder="big")
        s_bytes += self.initializing.to_bytes(length=1, byteorder="big")
        s_bytes += self.connected.to_bytes(length=1, byteorder="big")
        s_bytes += self.warn.to_bytes(length=1, byteorder="big")
        s_bytes += self.error.to_bytes(length=1, byteorder="big")
        s_bytes += self.process_running.to_bytes(length=1, byteorder="big")
        s_bytes += self.managed.to_bytes(length=1, byteorder="big")

        if self.name is not None:
            s_bytes += self.name.encode("utf-8")

        return s_bytes
    
    @staticmethod
    def decode(s_bytes: bytes):
        ret = SubsystemRuntimeState()

        ret.started = bool.from_bytes(s_bytes[0], byteorder="big")
        ret.initializing = bool.from_bytes(s_bytes[1], byteorder="big")
        ret.connected = bool.from_bytes(s_bytes[2], byteorder="big")
        ret.warn = bool.from_bytes(s_bytes[3], byteorder="big")
        ret.error = bool.from_bytes(s_bytes[4], byteorder="big")
        ret.process_running = bool.from_bytes(s_bytes[5], byteorder="big")
        ret.managed = bool.from_bytes(s_bytes[6], byteorder="big")

        if len(s_bytes > 7):
            ret.name = s_bytes[7:].decode("utf-8")

        return ret
    
    def __str__(self):
        ret_str = ""
        ret_str += f"Subsystem {'?' if self.name is None else self.name}:\n"
        ret_str += f"Connected: {self.connected}\n"
        ret_str += f"Initializing: {self.initializing}\n"
        ret_str += f"Error: {self.error}\n"
        ret_str += f"Warning: {self.warn}\n"
        ret_str += f"Running: {self.process_running}\n"
        ret_str += f"Managed: {self.managed}\n"
        ret_str += f"Started: {self.started}\n"

        return ret_str
"""
class SubsystemRuntimeStatesType(types.PropertyTypeSpecifier):
    def __init__(self):
        pass

    

    def encode_type(self):
        return bytes()

    @staticmethod
    def decode_type(data: bytes):
        return SubsystemRuntimeStatesType()
    
types.types.define_type(SubsystemRuntimeStatesType)
"""


class LifecycleManager:
    START_TIMEOUT = 10.0
    DC_TIMEOUT = 10.0

    def __init__(self, s_uuid: uuid.UUID):
        self.__s_uuid = s_uuid

        self.__logger_sock = tcp.TCPClientSocket()
        self.__logger_sock.connect(("127.0.0.1", 11751))
        self.__logger_sock.start()

        self.__logger = LogClient(self.__logger_sock, origin_uuid=s_uuid)

        self.__did_config = False

        self.__subsystems = dict()
        self.__processes = dict()
        self.__remote_handles = dict()
        self.__states = dict()
        self.__runtime_states = dict()
        self.__subsystem = None
        self.__out_kv = None

        self.__start_handles = []
        self.__stop_handles = []
        self.__restart_handles = []

        self.__start_all_handle = None
        self.__stop_all_handle = None

        self.__sent_status_request = False

        self.__auto_restart = True

        self.__event_consumer = mt_events.EventConsumer()

        def _on_ready():
            if self.__did_config:
                return

            self.__did_config = True
            sh = self.__client.register_subsystem("Lifecycle Manager", self.__s_uuid)

            self.__on_got_subsystem(sh)

        # print("Registering subsystem...")
        self.__client = client.DDSClient(uuid.uuid4(), logger=self.__logger)
        self.__client.when_ready().then(_on_ready)
        self.__E_SYSTEM_UPDATE = self.__client.on_remote_system_update().bind(
            self.__event_consumer
        )

        self.__daemon = daemon.Daemon(exception_handler=self.__handle_exception)
        self.__daemon.add(self.__thread)
        self.__daemon.start()

    def __thread(self, stop_flag: daemon.StopFlag):
        while stop_flag.run():
            e = self.__event_consumer.get(timeout=1)

            if not self.__client.ok() or self.__subsystem is None:
                time.sleep(1)
                continue

            if e == self.__E_SYSTEM_UPDATE:
                self.__on_system_update()

            if not self.__sent_status_request:
                self.__fetch_states()

            self.__update_states()
            self.__update_processes()
            self.__update_handles()

            if self.__out_kv is not None:
                self.__out_kv.value = self.__encode_runtime_states()

    def close(self):
        self.__logger.log(
            "Shutting down...", event="LC", level="INFO", subsystem="Lifecycle Manager"
        )
        self.stop_all()

        while len(self.__processes) > 0:
            time.sleep(0.1)

        self.__client.close()
        time.sleep(0.1)
        self.__daemon.stop()
        self.__logger.log(
            "Stopped cleanly.", event="LC", level="DEBUG", subsystem="Lifecycle Manager"
        )

        time.sleep(0.1)
        self.__logger_sock.close()

    def add_subsystem(self, s_uuid: uuid.UUID, run_target):
        self.__subsystems[s_uuid] = {
            "uuid": s_uuid,
            "target": run_target,
            "last_start": 0,
            "last_alive": 0,
            "should_stop": False,
            "stop_event_set": 0,
            "stop_event": None,
            "should_start": False,
            "restart_attempts": [],
            "can_restart": True,
            "commanded_run": True, 
        }

    def __on_system_update(self):
        if self.__subsystem is None:
            return

        s = self.__subsystem.get_all()

        self.__states.clear()
        for handle, state in s:
            self.__states[handle.get_info().get_uuid()] = state
            self.__remote_handles[handle.get_info().get_uuid()] = handle

    def __fetch_states(self):
        def __on_got_state(state: subsystem.SubsystemStatus, s_uuid: uuid.UUID):
            self.__states[s_uuid] = state
            self.__sent_status_request = False

            if state.get_status() == subsystem.SubsystemStatus.STATE_ALIVE:
                self.__subsystems[s_uuid]["last_alive"] = time.time()

        def __on_failed(state, reason):
            self.__sent_status_request = False

        for s in self.__subsystems.values():
            s_uuid = s["uuid"]

            self.__client.get_status(s_uuid).then(
                __on_got_state, kwargs={"s_uuid": s_uuid}
            ).catch(__on_failed)
            self.__sent_status_request = True

    def __update_states(self) -> dict[uuid.UUID, "SubsystemRuntimeState"]:
        if self.__subsystem is None:
            return

        self.__runtime_states.clear()

        for s in self.__subsystems.values():
            r = SubsystemRuntimeState()
            r.managed = True

            if s["last_start"] != 0:
                r.started = True

                if time.time() - s["last_start"] < self.START_TIMEOUT:
                    r.initializing = True

            for s in self.__subsystems.values():
                proc = self.__processes.get(s["uuid"])
                if proc is not None:
                    r.process_running = True

            state = self.__states.get(s["uuid"])

            if (
                state is not None
                and state.get_status() == subsystem.SubsystemStatus.STATE_ALIVE
            ):
                r.connected = True
                r.initializing = False
                for item in state.get_status_items():
                    sev = item.get_severity()

                    if sev == subsystem.StatusItem.STATE_ALARM:
                        r.error = True
                    if sev == subsystem.StatusItem.STATE_WARN:
                        r.warn = True

            if self.__remote_handles.get(s["uuid"]) is not None:
                r.name = self.__remote_handles.get(s["uuid"]).get_info().get_name()
            self.__runtime_states[s["uuid"]] = r

        for s_uuid, state in self.__states.items():
            if self.__runtime_states.get(s_uuid) is not None:
                continue

            r = SubsystemRuntimeState()
            r.process_running = False

            if state.get_status() == subsystem.SubsystemStatus.STATE_ALIVE:
                r.connected = True
                for item in state.get_status_items():
                    sev = item.get_severity()
                    if sev == subsystem.StatusItem.STATE_ALARM:
                        r.error = True
                    if sev == subsystem.StatusItem.STATE_WARN:
                        r.warn = True

            if self.__remote_handles.get(s_uuid) is not None:
                r.name = self.__remote_handles.get(s_uuid).get_info().get_name()

            self.__runtime_states[s_uuid] = r

    def __update_processes(self):
        for s_uuid, s in self.__subsystems.items():
            state = self.__runtime_states[s_uuid]

            for attempt in s["restart_attempts"]:
                if time.time() - attempt > 300.0:
                    s["restart_attempts"].remove(attempt)
                    break

            if s["can_restart"] and state.connected and not state.process_running:
                self.__logger.log(
                    f"Subsystem {s_uuid} has been launched externally, will not be launched automatically.",
                    event="LC",
                    level="INFO",
                    subsystem="Lifecycle Manager",
                    s_uuid=str(s_uuid),
                )

                s["can_restart"] = False

            if not s["commanded_run"]:
                continue

            if not s["can_restart"]:
                continue

            if state.connected:
                continue

            if state.initializing:
                continue

            if time.time() - s["last_alive"] < self.DC_TIMEOUT:
                continue

            if not self.__auto_restart:
                continue

            if s["should_stop"] or s["should_start"]:
                continue

            for attempt in s["restart_attempts"]:
                if time.time() - attempt < 30.0:
                    continue
            
            if state.started:
                if self.__processes.get(s_uuid) is not None:
                    self.__logger.log(
                        f"Subsystem has timed out: {s_uuid}",
                        event="LC",
                        level="WARN",
                        subsystem="Lifecycle Manager",
                        s_uuid=str(s_uuid),
                        action="TIMEOUT",
                    )
                else:
                    self.__logger.log(
                        f"Subsystem has died: {s_uuid}",
                        event="LC",
                        level="WARN",
                        subsystem="Lifecycle Manager",
                        s_uuid=str(s_uuid),
                        action="DIED",
                    )

            attempts = len(s["restart_attempts"])

            s["should_stop"] = True

            if attempts > 0:
                self.__logger.log(
                    f"Subsystem has been restarted {attempts} times already in the last 5 minutes!",
                    event="LC",
                    level="WARN",
                    subsystem="Lifecycle Manager",
                    s_uuid=str(s_uuid),
                )

                if attempts > 4:
                    self.__logger.log(
                        "TOO SOON! Subsystem has entered a crash loop! Will not restart subsystem automatically any longer.",
                        event="LC",
                        level="ERROR",
                        subsystem="Lifecycle Manager",
                        s_uuid=str(s_uuid),
                    )
                    s["can_restart"] = False
                    continue

            s["restart_attempts"].append(time.time())
            s["should_start"] = True

        for s_uuid, s in self.__subsystems.items():
            if s["should_stop"]:
                self.__stop_subsystem(s_uuid)

            if s["should_start"]:
                self.__start_subsystem(s_uuid)

        removed = True
        while removed:
            removed = False
            for s_uuid, process in self.__processes.items():
                if process.is_alive():
                    continue

                process.join()
                process.close()
                self.__processes.pop(s_uuid)
                self.__logger.log(
                    f"Subsystem process has exited: {s_uuid}",
                    event="LC",
                    level="DEBUG",
                    subsystem="Lifecycle Manager",
                    s_uuid=str(s_uuid),
                    action="FIN",
                )
                removed = True
                break

    def __update_handles(self):
        for s_uuid, handle in self.__start_handles:
            s = self.__subsystems[s_uuid]
            state = self.__runtime_states[s_uuid]

            if s["should_start"]:
                continue

            if state.initializing:
                continue

            if not state.connected:
                handle.fail(magics.E_START_FAILED)
            else:
                handle.ret(OP_OK)

            self.__start_handles.remove((s_uuid, handle))

        for s_uuid, handle in self.__stop_handles:
            s = self.__subsystems[s_uuid]
            p = self.__processes.get(s_uuid)

            if s["should_stop"]:
                continue

            if p is not None:
                continue

            handle.ret(OP_OK)
            self.__stop_handles.remove((s_uuid, handle))

        for s_uuid, handle in self.__restart_handles:
            s = self.__subsystems[s_uuid]
            p = self.__processes.get(s_uuid)

            if s["should_start"]:
                continue

            if state.initializing:
                continue

            if not state.connected:
                handle.fail(magics.E_START_FAILED)
            else:
                handle.ret(OP_OK)

            self.__restart_handles.remove((s_uuid, handle))

        if self.__start_all_handle is not None:
            failed = False
            not_started = False
            for s_uuid, s in self.__subsystems.items():
                state = self.__runtime_states[s_uuid]

                if state.initializing:
                    not_started = True
                    continue

                if s["should_start"] and not state.process_running:
                    not_started = True
                    continue

                if not s["should_start"]:
                    continue

                if not state.connected:
                    self.__start_all_handle.fail(magics.E_STARTS_FAILED)
                    self.__start_all_handle = None
                    failed = True
                    break
            
            if not failed and not not_started:
                self.__start_all_handle.ret(OP_OK)
                self.__start_all_handle = None

        if self.__stop_all_handle is not None:
            running = False
            for s_uuid, s in self.__subsystems.items():
                state = self.__runtime_states[s_uuid]

                if state.process_running:
                    running = True
                    break
            
            if not running:
                self.__stop_all_handle.ret(OP_OK)
                self.__stop_all_handle = None

    def __stop_subsystem(self, s_uuid):
        p = self.__processes.get(s_uuid)
        s = self.__subsystems[s_uuid]

        if p is None:
            s["should_stop"] = False
            return

        if not s["stop_flag"].is_set():
            self.__logger.log(
                f"Stopping subsystem: {s_uuid}",
                event="LC",
                subsystem="Lifecycle Manager",
                s_uuid=str(s_uuid),
                action="STOP",
            )
            s["stop_flag"].set()
            s["stop_event_set"] = time.time()

        if time.time() - s["stop_event_set"] > 5.0:
            self.__logger.log(
                f"Subsystem {s_uuid} process has taken too long to exit, killing!",
                event="LC",
                level="ERROR",
                subsystem="Lifecycle Manager",
                s_uuid=str(s_uuid),
                action="KILL",
            )
            s["stop_event_set"] = time.time()
            p.terminate()

    def __start_subsystem(self, s_uuid):
        s = self.__subsystems[s_uuid]

        if self.__processes.get(s_uuid) is not None or s["should_stop"]:
            return

        self.__logger.log(
            f"Starting subsystem: {s_uuid}",
            event="LC",
            subsystem="Lifecycle Manager",
            s_uuid=str(s_uuid),
            action="START",
        )

        e = multiprocessing.Event()

        s["stop_flag"] = e
        s["should_start"] = False
        s["last_start"] = time.time()

        p = multiprocessing.Process(target=s["target"], daemon=True, args=(e,))
        self.__processes[s_uuid] = p
        p.start()

    def get_states(self):
        return self.__runtime_states

    def __on_got_subsystem(self, handle: client._RegisteredSubsystemHandle):
        self.__subsystem = handle

        handle.add_event_handler(b"start_subsystem").on_called(self.__start_event)
        handle.add_event_handler(b"stop_subsystem").on_called(self.__stop_event)
        handle.add_event_handler(b"restart_subsystem").on_called(self.__restart_event)
        handle.add_event_handler(b"start_all_subsystems").on_called(self.__start_all_event)
        handle.add_event_handler(b"stop_all_subsystems").on_called(self.__stop_all_event)

        self.__out_kv = handle.get_kv_property(b"lifecycle_manager_runtime_states", False, True, True)

    def __get_subsystem_uuid(self, param: bytes):
        if len(param) != 16:
            return None
        
        s_uuid = uuid.UUID(bytes=param)
        
        s = self.__subsystems.get(s_uuid)

        if s is None:
            return None
        
        return s_uuid

    def __start_event(self, s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
        s_uuid = self.__get_subsystem_uuid(param)

        if s_uuid is None:
            handle.fail(magics.E_NXUUID)
            return
        
        self.start_subsystem(s_uuid)

        self.__start_handles.append((s_uuid, handle))

    def __stop_event(self, s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
        s_uuid = self.__get_subsystem_uuid(param)

        if s_uuid is None:
            handle.fail(magics.E_NXUUID)
            return
        
        self.stop_subsystem(s_uuid)

        self.__stop_handles.append((s_uuid, handle))

    def __restart_event(self, s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
        s_uuid = self.__get_subsystem_uuid(param)

        if s_uuid is None:
            handle.fail(magics.E_NXUUID)
            return
        
        self.restart_subsystem(s_uuid)

        self.__restart_handles.append((s_uuid, handle))

    def __start_all_event(self, s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
        if self.__start_all_handle is not None:
            handle.fail(magics.E_ALREADY_IN_PROGRESS)
            return
        
        self.start_all()
        
        self.__start_all_handle = handle

    def __stop_all_event(self, s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
        if self.__stop_all_handle is not None:
            handle.fail(magics.E_ALREADY_IN_PROGRESS)
            return
        
        self.stop_all()

        self.__stop_all_handle = handle

    def ok(self):
        return self.__client.ok()

    def start_subsystem(self, s_uuid):
        s = self.__subsystems[s_uuid]

        s["should_start"] = True
        s["commanded_run"] = True

    def stop_subsystem(self, s_uuid):
        s = self.__subsystems[s_uuid]

        s["should_stop"] = True
        s["can_restart"] = True
        s["restart_attempts"].clear()
        s["commanded_run"] = False

    def restart_subsystem(self, s_uuid):
        self.stop_subsystem(s_uuid)
        self.start_subsystem(s_uuid)

    def stop_all(self):
        self.__logger.log(
            "Stopping all subsystems.",
            event="LC",
            level="DEBUG",
            subsystem="Lifecycle Manager",
        )
        for s in self.__subsystems.keys():
            self.stop_subsystem(s)

    def start_all(self):
        self.__logger.log(
            "Starting all subsystems.",
            event="LC",
            level="DEBUG",
            subsystem="Lifecycle Manager",
        )

        for s in self.__subsystems.keys():
            self.start_subsystem(s)

    def __handle_exception(self, e: Exception):
        self.__logger.log(
            "Caught exception on lifecycle manager daemon thread!",
            level="ERROR",
            subsystem="Lifecycle Manager",
        )
        for line in traceback.format_exception(None, e, e.__traceback__):
            for split in line.split("\n"):
                self.__logger.log(split, level="ERROR", subsystem="Lifecycle Manager")

    def __encode_runtime_states(self):
        d_bytes = []

        for s_uuid, s in self.__runtime_states.items():
            d_bytes.append(segment_bytes.encode([s_uuid.bytes, s.encode()]))

        return segment_bytes.encode(d_bytes)
    
def decode_runtime_states(self, data: bytes):
    d_bytes = segment_bytes.decode(data)

    ret = dict()

    for item in d_bytes:
        s_uuid_bytes, s_state_bytes = segment_bytes.decode(item)
        s_uuid = uuid.UUID(bytes=s_uuid_bytes)
        s_state = SubsystemRuntimeState.decode(s_state_bytes)

        ret[s_uuid] = s_state
    
    return ret
