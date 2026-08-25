import multiprocessing
import math
import os
import queue
import time
import traceback
import uuid
import segment_bytes
import json

from dataclasses import replace

from ipi_ecs.core import daemon
import ipi_ecs.dds.subsystem as subsystem
import ipi_ecs.dds.client as client
import ipi_ecs.dds.magics as magics
import ipi_ecs.core.tcp as tcp
from ipi_ecs.dds.magics import EVENT_IN_PROGRESS, EVENT_OK, OP_IN_PROGRESS, OP_OK

from ipi_ecs.logging.client import LogClient
from ipi_ecs.db.db_library import Entry, Library
from ipi_ecs.subsystems.run_events import (
    RUN_EVENT_RESOURCE,
    RUN_EVENT_RESOURCE_TYPE,
    STREAM_END_KIND,
    STREAM_EXPECTED_KIND,
    STREAM_START_KIND,
    RunEvent,
    RunEventStream,
    append_run_event,
    load_run_event_timeline,
    run_event_stream_id,
)

PREPARE_RUN_TAGS_VERSION = 1
AUTOMATION_LEASE_SCHEMA_VERSION = 1
_RESERVED_RUN_TAGS = frozenset({
    "experiment",
    "run",
    "version",
    "status",
    "abort_reason",
})


def _normalize_run_tags(tags, reserved_keys=()) -> dict[str, str | int | float]:
    if not isinstance(tags, dict):
        raise ValueError("Run tags must be a JSON object.")

    reserved = _RESERVED_RUN_TAGS | frozenset(reserved_keys)
    normalized = {}
    for key, value in tags.items():
        if not isinstance(key, str) or not key or key != key.strip() or len(key) > 128:
            raise ValueError("Run tag keys must be non-empty trimmed strings of at most 128 characters.")
        if key in reserved or key.startswith("state_"):
            raise ValueError(f"Run tag {key!r} is reserved.")
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise ValueError(f"Run tag {key!r} must contain a string or finite number.")
        if isinstance(value, (int, float)) and not math.isfinite(float(value)):
            raise ValueError(f"Run tag {key!r} must contain a finite number.")
        normalized[key] = value
    return normalized


def encode_prepare_run_tags(tags: dict[str, str | int | float]) -> bytes:
    normalized = _normalize_run_tags(tags)
    return json.dumps(
        {"version": PREPARE_RUN_TAGS_VERSION, "tags": normalized},
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def decode_prepare_run_tags(payload: bytes, reserved_keys=()) -> dict[str, str | int | float]:
    if payload is None or len(payload) == 0:
        return {}
    if not isinstance(payload, bytes) or len(payload) > 64 * 1024:
        raise ValueError("Prepare-run tag payload must be at most 64 KiB of bytes.")
    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Prepare-run tag payload is not valid UTF-8 JSON.") from exc
    if not isinstance(decoded, dict) or set(decoded) != {"version", "tags"}:
        raise ValueError("Prepare-run tag payload must contain only version and tags.")
    if decoded["version"] != PREPARE_RUN_TAGS_VERSION:
        raise ValueError(f"Unsupported prepare-run tag payload version {decoded['version']!r}.")
    return _normalize_run_tags(decoded["tags"], reserved_keys)


class AutomationLease:
    def __init__(self):
        self.owner_uuid: uuid.UUID | None = None
        self.owner_name = ""
        self.acquired_at: float | None = None

    def acquire(self, requester: uuid.UUID, owner_name: str, now: float) -> tuple[bool, str]:
        normalized_name = owner_name.strip()
        if not normalized_name or len(normalized_name) > 128:
            return False, "Automation owner name must contain 1-128 characters."
        if self.owner_uuid not in (None, requester):
            return False, f"Exposure automation is owned by {self.owner_name} ({self.owner_uuid})."
        if self.owner_uuid is None:
            self.owner_uuid = requester
            self.owner_name = normalized_name
            self.acquired_at = float(now)
        return True, f"Exposure automation lease held by {self.owner_name}."

    def release(self, requester: uuid.UUID) -> tuple[bool, str]:
        if self.owner_uuid is None:
            return True, "Exposure automation lease is already free."
        if self.owner_uuid != requester:
            return False, f"Exposure automation is owned by {self.owner_name} ({self.owner_uuid})."
        self.clear()
        return True, "Exposure automation lease released."

    def can_start(self, requester: uuid.UUID) -> tuple[bool, str]:
        if self.owner_uuid in (None, requester):
            return True, ""
        return False, f"Exposure automation is owned by {self.owner_name} ({self.owner_uuid})."

    def can_update_settings(self, requester: uuid.UUID, run_active: bool) -> tuple[bool, str]:
        if run_active:
            return False, "Exposure settings cannot change while a run is active."
        return self.can_start(requester)

    def clear(self) -> None:
        self.owner_uuid = None
        self.owner_name = ""
        self.acquired_at = None

    def encode(self) -> bytes:
        return json.dumps(
            {
                "schema_version": AUTOMATION_LEASE_SCHEMA_VERSION,
                "owner_uuid": str(self.owner_uuid) if self.owner_uuid is not None else None,
                "owner_name": self.owner_name or None,
                "acquired_at": self.acquired_at,
            },
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")

class RunSettings:
    data = {
        "name": "",
        "description": "",
    }

    def encode(self) -> str:
        return json.dumps(self.data)
    
    @staticmethod
    def decode(data: str):
        load = json.loads(data)
        obj = RunSettings()
        obj.data = load

        return obj
    
    def get_dict(self):
        return self.data.copy()
    
    def set_attr(self, key: str, value):
        if key not in self.data:
            raise ValueError(f"Key '{key}' not found in RunSettings.")
        
        #print(f"Setting {key} to {value} of type {type(value)} which is currently of type {type(self.data[key])}")
        
        self.data[key] = type(self.data[key])(value) if key in self.data else value

    def get_attr(self, key: str, default=None):
        return self.data.get(key, default)
    
    def get_keys(self):
        return self.data.keys()
    
    def get_types(self):
        return {k: type(v) for k, v in self.data.items()}
    
class RunState:
    def __init__(self, e_type: str, experiment_config: RunSettings, s_uuid: uuid.UUID = None):
        self.__type = e_type
        self.__experiment_config = experiment_config
        self.__uuid = uuid.uuid4() if s_uuid is None else s_uuid

        self.__name = experiment_config.get_dict().get("name", None)
        self.__description = experiment_config.get_dict().get("description", None)

    def set_name(self, name: str):
        self.__name = name

    def set_description(self, description: str):
        self.__description = description

    def encode(self) -> str:
        return json.dumps({
            "type": self.__type,
            "config": self.__experiment_config.encode(),
            "uuid": str(self.__uuid),
            "name": self.__name,
            "description": self.__description,
        })
    
    def get_name(self):
        return self.__name
    
    def get_description(self):
        return self.__description
    
    @staticmethod
    def decode(data: str):
        load = json.loads(data)
        obj = RunState(load["type"], RunSettings.decode(load["config"]), uuid.UUID(load["uuid"]))
        obj.set_name(load.get("name", None))
        obj.set_description(load.get("description", None))
        return obj
    
    def get_uuid(self):
        return self.__uuid
    
    def get_settings(self):
        return self.__experiment_config
    
    def get_dict(self):
        return {
            "uuid": str(self.__uuid),
            "config": self.__experiment_config.get_dict(),
            "type": self.__type,
            "name": self.__name,
            "description": self.__description,
        }
    
    def get_type(self):
        return self.__type

class RunRecord:
    CURRENT_DATA_VERSION = 2

    def __init__(self, logger: LogClient, library: Library, controller: "ExperimentController", r_uuid: uuid.UUID, entry: Entry = None, event_uuid: uuid.UUID = None):
        self.__entry = entry
        self.__event_uuid = event_uuid

        self.__logger = logger
        self.__library = library
        self.__run_uuid = r_uuid

        self.__metadata = None
        self.__end_metadata = None

        self.__state = None
        self.__controller = controller

        if self.__entry is None:
            self.read(self.__run_uuid)

    @staticmethod
    def create(
        logger: LogClient,
        library: Library,
        state: RunState,
        settings: RunSettings,
        controller: "ExperimentController",
        run_tags: dict[str, str | int | float] | None = None,
    ):
        print("Creating run record for run with UUID:", state.get_uuid(), "and type:", state.get_type())
        name = state.get_name() if state.get_name() is not None else f"Run {str(state.get_uuid())[-8:]}"
        desc = state.get_description() if state.get_description() is not None else f"Run on {time.ctime()}"
        entry = library.create_entry(name, desc)

        for k, v in settings.get_dict().items():
            print("Adding tag for setting:", k, v, "of type", type(v))
            entry.set_tag(k, v)

        entry.set_tag("experiment", state.get_type())
        entry.set_tag("run", state.get_uuid().hex)
        entry.set_tag("version", RunRecord.CURRENT_DATA_VERSION)
        for key, value in _normalize_run_tags(run_tags or {}, settings.get_keys()).items():
            entry.set_tag(key, value)

        print("Beginning event for run with UUID:", state.get_uuid(), "and entry UUID:", entry.get_uuid())
        event_uuid = logger.begin_event("RUN", name, event_id=str(state.get_uuid()), subsystem=controller.name, run=state.get_dict(), exp_type=state.get_type())

        res = entry.resource("run.json", "run_state", "w")
        res.write(state.encode())
        res.close()

        metadata = {
            "event_uuid": event_uuid,
            "created_at": time.time(),
            "version": RunRecord.CURRENT_DATA_VERSION,
        }

        md_res = entry.resource("metadata.json", "metadata", "w")
        json.dump(metadata, md_res)
        md_res.close()

        with entry.resource(RUN_EVENT_RESOURCE, RUN_EVENT_RESOURCE_TYPE, "wb"):
            pass

        return RunRecord(logger, library, controller, state.get_uuid(), entry=entry, event_uuid=event_uuid)
    
    def read(self, s_uuid: uuid.UUID):
        #print("Reading run record for run with UUID:", s_uuid)
        entry = self.__library.query({"tags": {"run": s_uuid.hex}}, limit=1)

        if entry is None or len(entry) == 0:
            raise ValueError(f"Run with UUID {str(s_uuid)} not found in library.")
        
        entry = entry[0]
        
        self.__entry = entry # Defer reading until accessing state or metadata to avoid unnecessary reads
    
    def __read_from_entry(self, entry: Entry):
        #print("Reading run record from entry with UUID:", entry.get_uuid(), "and tags:", entry.get_tags())
        res = entry.resource("run.json", "run_state", "r")
        run = RunState.decode(res.read())
        res.close()

        md_res = entry.resource("metadata.json", "metadata", "r")
        metadata = json.load(md_res)
        md_res.close()

        try:
            md_end_res = entry.resource("end_metadata.json", "metadata", "r")
            end_metadata = json.load(md_end_res)
            md_end_res.close()
        except FileNotFoundError:
            end_metadata = None

        self.__entry = entry
        self.__state = run
        self.__metadata = metadata
        self.__end_metadata = end_metadata
        self.__event_uuid = metadata.get("event_uuid", None)

        #print("Read run record from entry with UUID:", entry.get_uuid(), "and tags:", entry.get_tags())

    def set_name(self, name: str):
        if self.__entry is not None:
            self.__entry.set_name(name)

    def set_description(self, description: str):
        if self.__entry is not None:
            self.__entry.set_desc(description)

    def get_name(self) -> str:
        if self.__entry is not None:
            return self.__entry.get_name()
        return ""
    
    def get_description(self) -> str:
        if self.__entry is not None:
            return self.__entry.get_description()
        return ""
    
    def add_tag(self, key: str, value: str | int | float):
        if self.__entry is not None:
            self.__entry.set_tag(key, value)

    def get_tags(self) -> dict:
        if self.__entry is not None:
            return self.__entry.get_tags()
        return {}

    def append_event(self, event: RunEvent) -> bool:
        if self.__entry is None:
            raise RuntimeError("Run record has no database entry.")
        if event.run_id != self.__run_uuid:
            raise ValueError("Run event does not belong to this run record.")
        return append_run_event(self.__entry, event)

    def get_event_timeline(self):
        if self.__entry is None:
            raise RuntimeError("Run record has no database entry.")
        return load_run_event_timeline(self.__entry)

    def write_end(self, state: RunState, status: str, reason: str):
        print("Writing end metadata for run with UUID:", state.get_uuid(), "status:", status, "reason:", reason)
        if self.__entry is not None:
            md_res = self.__entry.resource("end_metadata.json", "metadata", "w")
            json.dump({
                "end_time": time.time(),
                "end_reason": reason,
                "status": status,
            }, md_res)
            md_res.close()

            for k, v in state.get_dict().items():
                if isinstance(v, (str, int, float)):
                    self.__entry.set_tag(f"state_{k}", v)

            self.__entry.set_tag("status", status)
            self.__entry.set_tag("abort_reason", reason)

        print("Ending event for run with UUID:", state.get_uuid(), "status:", status, "reason:", reason, "event UUID:", self.__event_uuid)
        self.__logger.end_event(self.__event_uuid, status=status, reason=reason)

    def get_record(self):
        return self.__entry
    
    def get_state(self) -> RunState:
        if self.__state is None and self.__entry is not None:
            print("State is None, reading from entry...")
            self.__read_from_entry(self.__entry)
        
        return self.__state
    
    def get_metadata(self):
        if self.__metadata is None and self.__entry is not None:
            #print("Metadata is None, reading from entry...")
            self.__read_from_entry(self.__entry)
        return self.__metadata
    
    def get_end_metadata(self):
        if self.__end_metadata is None and self.__entry is not None:
            #print("End metadata is None, reading from entry...")
            self.__read_from_entry(self.__entry)
        return self.__end_metadata

class ExperimentController:
    RUN_OK = 0
    RUN_ABORT = 1

    RUN_STATE_CAN_START = 5
    RUN_STATE_PREINIT = 0
    RUN_STATE_INIT = 1
    RUN_STATE_RUNNING = 2
    RUN_STATE_STOPPING = 3
    RUN_STATE_STOPPED = 4
    RUN_STATE_NAMES = {
        RUN_STATE_CAN_START: "CAN_START",
        RUN_STATE_PREINIT: "PREINIT",
        RUN_STATE_INIT: "INIT",
        RUN_STATE_RUNNING: "RUNNING",
        RUN_STATE_STOPPING: "STOPPING",
        RUN_STATE_STOPPED: "STOPPED",
    }
    RUN_EVENT_RETRY_SECONDS = 5.0
    RUN_EVENT_RETRY_INTERVAL_SECONDS = 0.1
    MAX_RUN_EVENT_PAYLOAD_BYTES = 256 * 1024

    name = "ExperimentController"
    exp_type = "my_experiment"

    def __init__(self, name: str, s_uuid: uuid.UUID, exp_type: str, data_path: str):
        self.__run = True

        self.name = name
        self.uuid = s_uuid
        self.exp_type = exp_type
        self.data_path = data_path

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
            sh = self.__client.register_subsystem(self.name, s_uuid)

            self.__on_got_subsystem(sh)

        #print("Registering subsystem...")
        self.__client = client.DDSClient(c_uuid, logger=self.__logger)
        self.__client.when_ready().then(_on_ready)

        self.__can_start_event_handle = None
        self.__can_start_event_provider = None
        
        self.__preinit_handle = None
        self.__preinit_provider = None

        self.__init_handle = None
        self.__init_provider = None

        self.__start_run_handle = None
        self.__preinit_handle = None

        self.__stop_provider = None
        self.__stop_handle = None

        self.__stop_request_handle = None

        self.__automation_lease = AutomationLease()
        self.__automation_lease_kv = None
        self.__automation_owner_missing_since = None

        self.__state_kv = None
        self.__run_kv = None
        self.__reasons_kv = None

        self.__run_record = None
        self.__event_uuid = None
        self.__current_phase = self.RUN_STATE_STOPPED
        self.__lifecycle_stream = None
        self.__expected_event_streams: dict[str, tuple[uuid.UUID, uuid.UUID]] = {}
        self.__pending_run_events: list[tuple[RunRecord, RunEvent]] = []
        self.__pending_run_events_lock = multiprocessing.Lock()
        self.__last_pending_event_warning = 0.0

        self.__settings_type = RunSettings
        self.__settings = self.__settings_type()
        self.__current_run = None

        self.__next_run_uuid = None

        self.__require_subsystems = {
            # uuids.UUID_TARGET_CONTROLLER: "Target Controller",
        }

        self.__library = None

        self.__states = dict()

        self.__data_thread_queue = queue.Queue()

        self.__daemon = daemon.Daemon(exception_handler=self.handle_exception)
        self.__daemon.add(self.__data_thread)
        self.__daemon.add(self.__thread)
        self.__daemon.add(self.__check_thread)

        self.__daemon.start()

    def handle_exception(self, e: Exception):
        self.__log("Caught exception on daemon thread!", level="ERROR")
        for line in traceback.format_exception(None, e, e.__traceback__):
            for split in line.split('\n'):
                self.__log(split, level="ERROR")
    
    def __log(self, msg, level = "INFO", **data):
        if self.__logger is None:
            print(level, msg)
            return
        
        self.__logger.log(msg, level=level, l_type="SW", subsystem="Experiment Controller", **data)

    def __thread(self, stop_flag: daemon.StopFlag):
        while stop_flag.run() and self.__run:
            self.__retry_pending_run_event()
            if self.__can_start_event_handle is not None and not self.__can_start_event_handle.is_in_progress():
                self.__on_can_start_returned()

            if self.__has_timed_out(self.__can_start_event_handle, 30):
                self.__abort_run("Run start request timed out.")
                self.__can_start_event_handle = None
            
            if self.__preinit_handle is not None and not self.__preinit_handle.is_in_progress():
                self.__on_preinit_returned()

            if self.__has_timed_out(self.__preinit_handle, 30):
                self.__abort_run("Preinit request timed out.")
                self.__preinit_handle = None

            if self.__init_handle is not None and not self.__init_handle.is_in_progress():
                self.__on_init_returned()

            if self.__has_timed_out(self.__init_handle, 30):
                self.__abort_run("Init request timed out.")
                self.__init_handle = None

            if self.__stop_handle is not None and not self.__stop_handle.is_in_progress():
                self.__on_stop_returned()

            if self.__has_timed_out(self.__stop_handle, 30):
                self.__set_run_phase(
                    self.RUN_STATE_STOPPED,
                    outcome="ABORTED",
                    reason="Stop run request timed out.",
                )
                self.__finalize_run("ABORTED", "Stop run request timed out.")
                self.__run_record = None
                self.__stop_handle = None

                if self.__stop_request_handle is not None:
                    self.__stop_request_handle.fail(b"Stop run request timed out.")
                    self.__stop_request_handle = None

            self.__update_state()

            time.sleep(0.1)

    def __check_thread(self, stop_flag: daemon.StopFlag):
        while stop_flag.run() and self.__run:
            if self.__current_run is not None and self.__start_run_handle is None and self.__stop_handle is None:
                should_continue, reason = self.__should_continue()
                if not should_continue:
                    self.__abort_run(reason)
            self.__check_automation_owner()
            time.sleep(1)

    def __publish_automation_lease(self):
        if self.__automation_lease_kv is not None:
            self.__automation_lease_kv.value = self.__automation_lease.encode()

    def __check_automation_owner(self):
        owner_uuid = self.__automation_lease.owner_uuid
        if owner_uuid is None or self.__subsystem is None:
            self.__automation_owner_missing_since = None
            return
        alive = any(
            handle.get_info().get_uuid() == owner_uuid
            and state.get_status() == subsystem.SubsystemStatus.STATE_ALIVE
            for handle, state in self.__subsystem.get_all()
        )
        if alive:
            self.__automation_owner_missing_since = None
            return
        if self.__automation_owner_missing_since is None:
            self.__automation_owner_missing_since = time.monotonic()
            return
        if self.__current_run is None and time.monotonic() - self.__automation_owner_missing_since >= 10.0:
            self.__log(
                f"Releasing automation lease for disconnected owner {owner_uuid}.",
                level="WARN",
                event="automation_lease_release",
            )
            self.__automation_lease.clear()
            self.__automation_owner_missing_since = None
            self.__publish_automation_lease()

    def __status_str_get_for_event(self, event_handle: client._InProgressEvent._Handle):
        states = []

        for subsystem_uuid, state in event_handle.get_states().items():
            code, reason = state

            s_name = self.__require_subsystems.get(subsystem_uuid, str(subsystem_uuid))
            status_code = "Ongoing" if code == EVENT_IN_PROGRESS else "Done"

            if reason == magics.E_DOES_NOT_HANDLE_EVENT:
                continue

            states.append(segment_bytes.encode([s_name.encode("utf-8"), status_code.encode("utf-8"), reason if reason is not None else b""]))

        return states

    def __update_state(self):
        if self.__state_kv is None:
            return
        
        statestr = []
        
        if self.__current_run is not None:
            self.__state_kv.value = segment_bytes.encode([
                self.__current_phase.to_bytes(1, "big"),
                self.__current_run.encode().encode("utf-8"),
            ])
            event_handle = {
                self.RUN_STATE_CAN_START: self.__can_start_event_handle,
                self.RUN_STATE_PREINIT: self.__preinit_handle,
                self.RUN_STATE_INIT: self.__init_handle,
                self.RUN_STATE_STOPPING: self.__stop_handle,
            }.get(self.__current_phase)
            if event_handle is not None:
                statestr = self.__status_str_get_for_event(event_handle)
        else:
            self.__state_kv.value = segment_bytes.encode([self.RUN_STATE_STOPPED.to_bytes(1, "big"), bytes()])

        self.__reasons_kv.value = segment_bytes.encode(statestr)

    def __has_timed_out(self, event_handle: client._InProgressEvent._Handle, timeout: float) -> bool:
        if event_handle is None:
            return False
        
        t_initiated = event_handle.get_time_initiated()
        last_update, l_uuid = event_handle.get_last_update()

        now = time.time()

        if now - t_initiated < timeout:
            return False
        
        if now - last_update < timeout:
            return False
        
        self.__log(f"Event handle {event_handle} has timed out. Now: {now}, Initiated: {t_initiated}, Last update: {last_update}, Last update UUID: {l_uuid}", level="WARN")
        
        for r_uuid, state in event_handle.get_states().items():
            code, reason = state
            s_name = self.__require_subsystems.get(r_uuid, str(r_uuid))
            result = event_handle.get_result(r_uuid)
            self.__log(f"Subsystem {s_name} has status code {code} and reason {reason} with result {result}", level="WARN")


        return True

    def __required_subsystem_name(self, s_uuid: uuid.UUID) -> str:
        return self.__require_subsystems.get(s_uuid, str(s_uuid))
    
    def __should_continue(self):
        s = self.__subsystem.get_all()
        for _handle, _state in s:
            if _handle.get_info().get_uuid() not in self.__require_subsystems:
                continue

            if _state.get_status() != subsystem.SubsystemStatus.STATE_ALIVE:
                dead_uuid = _handle.get_info().get_uuid()
                return False, f"Required subsystem {self.__required_subsystem_name(dead_uuid)} has died."

        state_vs = self.__request_states()
        for s_uuid, s_name in self.__require_subsystems.items():
            if s_uuid not in state_vs:
                return False, f"Required subsystem {s_name} did not provide state KV."

            state, v = state_vs[s_uuid]

            if state == OP_IN_PROGRESS:
                return False, f"Attempt to fetch status of {s_name} has timed out."

            if state != OP_OK:
                return False, f"Attempt to fetch status of {s_name} returned non-OK({state}): {v if v is not None else 'No reason provided'}."
            
            b_ok, state = segment_bytes.decode(v)
            ok = bool.from_bytes(b_ok, "big")

            if not ok:
                return False, f"Subsystem {s_name} reported not OK status: {state.decode('utf-8') if state is not None else 'No reason provided'}."
            
            self.__states[s_uuid] = state
            
        
        return True, None
                

    def __data_thread(self, stop_flag: daemon.StopFlag):
        self.__library = Library(self.data_path)
        try:
            while stop_flag.run() and self.__run:
                try:
                    fn, pargs, kwargs, result_queue = self.__data_thread_queue.get(timeout=0.1)
                except queue.Empty:
                    continue
                try:
                    result_queue.put(("ok", fn(*pargs, **kwargs)))
                except Exception as exc:
                    result_queue.put(("err", exc))
        finally:
            self.__library.close()

    def __data_thread_enqueue(self, fn, *pargs, **kwargs):
        result_queue = queue.Queue(maxsize=1)
        self.__data_thread_queue.put((fn, pargs, kwargs, result_queue))
        status, result = result_queue.get()
        if status == "err":
            raise result
        return result

    def __persist_run_event(self, record: RunRecord, event: RunEvent) -> bool:
        deadline = time.monotonic() + self.RUN_EVENT_RETRY_SECONDS
        last_error = None
        while True:
            try:
                return bool(self.__data_thread_enqueue(record.append_event, event))
            except Exception as exc:
                last_error = exc
                if time.monotonic() >= deadline:
                    break
                time.sleep(self.RUN_EVENT_RETRY_INTERVAL_SECONDS)
        with self.__pending_run_events_lock:
            if not any(pending.event_id == event.event_id for _record, pending in self.__pending_run_events):
                self.__pending_run_events.append((record, event))
        self.__log(
            f"Run event {event.event_id} could not be persisted immediately and remains queued: "
            f"{type(last_error).__name__}: {last_error}",
            level="ERROR",
            event="run_event_persistence_deferred",
            run_id=str(event.run_id),
            event_id=str(event.event_id),
        )
        return False

    def __retry_pending_run_event(self) -> None:
        with self.__pending_run_events_lock:
            pending = self.__pending_run_events[0] if self.__pending_run_events else None
        if pending is None:
            return
        record, event = pending
        try:
            self.__data_thread_enqueue(record.append_event, event)
        except Exception as exc:
            now = time.monotonic()
            if now - self.__last_pending_event_warning >= 5.0:
                self.__last_pending_event_warning = now
                self.__log(
                    f"Run event {event.event_id} is still awaiting persistence: {type(exc).__name__}: {exc}",
                    level="ERROR",
                    event="run_event_persistence_pending",
                    run_id=str(event.run_id),
                    event_id=str(event.event_id),
                )
            return
        with self.__pending_run_events_lock:
            self.__pending_run_events = [
                item for item in self.__pending_run_events if item[1].event_id != event.event_id
            ]

    def __start_lifecycle_stream(self) -> None:
        if self.__current_run is None or self.__run_record is None:
            raise RuntimeError("Cannot start a lifecycle event stream without an active run record.")
        run_id = self.__current_run.get_uuid()
        self.__lifecycle_stream = RunEventStream(
            run_id,
            run_event_stream_id(run_id, self.uuid, "controller.lifecycle"),
            "controller.lifecycle",
            self.uuid,
        )
        timestamp_ns = time.time_ns()
        event = self.__lifecycle_stream.event(
            STREAM_START_KIND,
            {"controller": self.name, "experiment_type": self.exp_type},
            producer_unix_ns=timestamp_ns,
            producer_monotonic_ns=time.monotonic_ns(),
            ingest_unix_ns=timestamp_ns,
        )
        self.__persist_run_event(self.__run_record, event)

    def __declare_expected_event_streams(self) -> None:
        if self.__lifecycle_stream is None or self.__run_record is None or self.__current_run is None:
            return
        run_id = self.__current_run.get_uuid()
        for stream_name, (submitter_id, producer_id) in self.__expected_event_streams.items():
            timestamp_ns = time.time_ns()
            event = self.__lifecycle_stream.event(
                STREAM_EXPECTED_KIND,
                {
                    "stream_id": str(run_event_stream_id(run_id, submitter_id, stream_name)),
                    "stream_name": stream_name,
                    "submitter_id": str(submitter_id),
                    "producer_id": str(producer_id),
                },
                producer_unix_ns=timestamp_ns,
                producer_monotonic_ns=time.monotonic_ns(),
                ingest_unix_ns=timestamp_ns,
            )
            self.__persist_run_event(self.__run_record, event)

    def __set_run_phase(self, phase: int, *, outcome: str | None = None, reason: str | None = None) -> None:
        if phase not in self.RUN_STATE_NAMES:
            raise ValueError(f"Unknown run phase {phase}.")
        if self.__current_run is None:
            self.__current_phase = self.RUN_STATE_STOPPED
            return
        if self.__current_phase == phase and phase != self.RUN_STATE_STOPPED:
            return
        self.__current_phase = phase
        if self.__lifecycle_stream is None or self.__run_record is None:
            return
        timestamp_ns = time.time_ns()
        payload = {"phase": self.RUN_STATE_NAMES[phase], "phase_code": phase}
        if outcome is not None:
            payload["outcome"] = outcome
        if reason is not None:
            payload["reason"] = reason
        event = self.__lifecycle_stream.event(
            "lifecycle.phase",
            payload,
            producer_unix_ns=timestamp_ns,
            producer_monotonic_ns=time.monotonic_ns(),
            ingest_unix_ns=timestamp_ns,
        )
        self.__persist_run_event(self.__run_record, event)
        if phase == self.RUN_STATE_STOPPED:
            end_event = self.__lifecycle_stream.event(
                STREAM_END_KIND,
                {"outcome": outcome, "reason": reason},
                producer_unix_ns=timestamp_ns,
                producer_monotonic_ns=time.monotonic_ns(),
                ingest_unix_ns=timestamp_ns,
            )
            self.__persist_run_event(self.__run_record, end_event)
            self.__lifecycle_stream = None

    def __create_run(self, run_tags: dict[str, str | int | float] | None = None):
        self.__current_run = RunState(self.exp_type, self.__settings, s_uuid=self.__next_run_uuid)
        print("Creating run with state:", self.__current_run.get_dict())
        self.__run_record = self.__data_thread_enqueue(
            RunRecord.create,
            self.__logger,
            self.__library,
            self.__current_run,
            self.__settings,
            self,
            run_tags,
        )
        self.__start_lifecycle_stream()
        self.__set_run_phase(self.RUN_STATE_CAN_START)
        print(f"Created run record with UUID {str(self.__current_run.get_uuid())} and entry UUID {str(self.__run_record.get_record().get_uuid())}")

    def __abort_run(self, reason: str):
        print("Aborting run:", reason)
        print(f"Run record: {self.__run_record}")
        if self.__start_run_handle is not None:
            self.__start_run_handle.fail(reason.encode("utf-8"))
            self.__start_run_handle = None

        if self.__can_start_event_handle is not None:
            self.__can_start_event_handle.abort()
            self.__can_start_event_handle = None
        
        if self.__preinit_handle is None and self.__init_handle is None and self.__run_record is None:
            self.__logger.log(f"Cannot start run: {reason}", level="WARN", l_type="EXP", subsystem=self.name)
        else:
            self.__logger.log(f"Aborting run: {reason}", level="ERROR", l_type="EXP", subsystem=self.name)

        if self.__preinit_handle is not None:
            self.__preinit_handle.abort()
            self.__preinit_handle = None

        if self.__init_handle is not None:
            self.__init_handle.abort()
            self.__init_handle = None

        if self.__current_run is not None:
            self.__set_run_phase(self.RUN_STATE_STOPPED, outcome="ABORTED", reason=reason)
            self.__stop_provider.call(segment_bytes.encode([self.RUN_ABORT.to_bytes(1, "big"), self.__current_run.get_uuid().bytes, reason.encode("utf-8")]), target=[])
        else:
            self.__stop_provider.call(segment_bytes.encode([self.RUN_ABORT.to_bytes(1, "big"), bytes(), reason.encode("utf-8")]), target=[])

        if self.__run_record is not None:
            self.__finalize_run("ABORTED", reason)
            self.__run_record = None

    def __on_start_run_event(self, s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
        print("Start run event called by:", s_uuid, param)
        allowed, reason = self.__automation_lease.can_start(s_uuid)
        if not allowed:
            handle.fail(reason.encode("utf-8"))
            return
        if param:
            handle.fail(b"Legacy prepare event does not accept run tags; use the tagged prepare event.")
            return
        self.__begin_start_run(handle, {})

    def __on_start_tagged_run_event(self, s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
        print("Tagged start run event called by:", s_uuid, param)
        allowed, reason = self.__automation_lease.can_start(s_uuid)
        if not allowed:
            handle.fail(reason.encode("utf-8"))
            return
        try:
            run_tags = decode_prepare_run_tags(param, self.__settings.get_keys())
        except ValueError as exc:
            handle.fail(f"Invalid prepare-run tags: {exc}".encode("utf-8"))
            return

        self.__begin_start_run(handle, run_tags)

    def __on_acquire_automation_event(self, s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
        try:
            owner_name = param.decode("utf-8")
        except UnicodeDecodeError:
            handle.fail(b"Automation owner name must be UTF-8.")
            return
        ok, message = self.__automation_lease.acquire(s_uuid, owner_name, time.time())
        if not ok:
            handle.fail(message.encode("utf-8"))
            return
        self.__automation_owner_missing_since = None
        self.__publish_automation_lease()
        handle.ret((magics.OP_OK + b": " + message.encode("utf-8")))

    def __on_release_automation_event(self, s_uuid, _param, handle: client._EventHandler._IncomingEventHandle):
        if self.__current_run is not None and self.__automation_lease.owner_uuid == s_uuid:
            handle.fail(b"Cannot release the automation lease while an exposure is active.")
            return
        ok, message = self.__automation_lease.release(s_uuid)
        if not ok:
            handle.fail(message.encode("utf-8"))
            return
        self.__automation_owner_missing_since = None
        self.__publish_automation_lease()
        handle.ret((magics.OP_OK + b": " + message.encode("utf-8")))

    def __begin_start_run(self, handle: client._EventHandler._IncomingEventHandle, run_tags):
        self.__start_run_handle = handle
        s, r = self.__try_start_run(run_tags)
        if s:
            handle.feedback(r)
        else:
            self.__start_run_handle = None
            handle.fail(r)

    def __on_stop_run_event(self, s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
        print("Stop run event called by:", s_uuid, param)
        if self.__current_run is None:
            handle.fail(b"No run to stop!")
            return

        self.__stop_request_handle = handle
        self.stop_run(param.decode("utf-8"))

    def __try_start_run(self, run_tags: dict[str, str | int | float] | None = None):
        if self.__preinit_handle is not None or self.__init_handle is not None or self.__stop_handle is not None or self.__current_run is not None:
            self.__logger.log("Cannot start new run while another is in progress!", level="WARN", l_type="EXP", subsystem=self.name)
            return False, b"Cannot start new run while another is in progress!"
        
        self.__next_run_uuid = uuid.uuid4()
        self.__create_run(run_tags)

        self.__logger.log("Attempting to begin new run: " + str(self.__next_run_uuid) + "...", level="DEBUG", l_type="EXP", subsystem=self.name)


        b_s_data = self.__settings.encode().encode("utf-8")
        b_state_data = self.__current_run.encode().encode("utf-8")
        e_h = self.__can_start_event_provider.call(segment_bytes.encode([b_s_data, b_state_data]), target=[])
        self.__can_start_event_handle = e_h

        return True, magics.OP_OK

    def stop_run(self, reason: str):
        if self.__run_record is None or self.__current_run is None:
            return False, "No run to stop!"
        
        if self.__preinit_handle is not None or self.__init_handle is not None:
            print("Stopping run during preinit!")
            self.__abort_run(reason)
            return True, magics.OP_OK
        
        self.__logger.log(f"Stopping run {str(self.__current_run.get_uuid())[-8:]} : " + reason, level="INFO", l_type="EXP", subsystem=self.name, run=self.__current_run.get_dict(), reason=reason, exp_type=self.__current_run.get_type())

        self.__set_run_phase(self.RUN_STATE_STOPPING, reason=reason)
        self.__stop_reason = reason
        self.__stop_handle = self.__stop_provider.call(segment_bytes.encode([self.RUN_OK.to_bytes(1, "big"), self.__current_run.get_uuid().bytes, reason.encode("utf-8")]), target=[])

    def __finalize_run(self, code: str, reason: str):
        record = self.__run_record
        state = self.__current_run
        self.__run_record = None

        if record is None or state is None:
            self.__logger.log(
                f"Skipping duplicate finalization with code {code}: run record or state is unavailable.",
                level="WARN",
                l_type="EXP",
                subsystem=self.name,
            )
            if state is not None:
                try:
                    self.__stop_kv.value = segment_bytes.encode([state.get_uuid().bytes, code.encode("utf-8"), reason.encode("utf-8")])
                except Exception as exc:
                    self.__logger.log(
                        f"Failed to set terminal stop KV for run {str(state.get_uuid())[-8:]}: {exc}",
                        level="ERROR",
                        l_type="EXP",
                        subsystem=self.name,
                    )
            self.__current_run = None
            return
        self.__data_thread_enqueue(record.write_end, state, code, reason)

        self.__logger.log(f"Run {str(state.get_uuid())[-8:]} has been finalized with code " + code + ": " + reason, level="DEBUG", l_type="EXP", subsystem=self.name, run=state.get_dict(), reason=reason, exp_type=state.get_type())
        try:
            print(f"Setting stop KV for run {str(state.get_uuid())[-8:]} with code '{code}' and reason '{reason}'")
            self.__stop_kv.value = segment_bytes.encode([state.get_uuid().bytes, code.encode("utf-8"), reason.encode("utf-8")])
        except Exception as e:
            self.__logger.log(f"Failed to set stop KV for run {str(state.get_uuid())[-8:]}: {e}", level="ERROR", l_type="EXP", subsystem=self.name)

        self.__current_run = None

    def __on_stop_returned(self):
        self.__logger.log(f"Run {str(self.__current_run.get_uuid())[-8:]} stopped: " + self.__stop_reason, level="INFO", l_type="EXP", subsystem=self.name, run=self.__current_run.get_dict(), reason=self.__stop_reason, event="stop_run", exp_type=self.__current_run.get_type())
        self.__set_run_phase(self.RUN_STATE_STOPPED, outcome="STOPPED", reason=self.__stop_reason)
        self.__finalize_run("STOPPED", self.__stop_reason)
        self.__run_record = None

        self.__current_run = None

        self.__stop_handle = None

        if self.__stop_request_handle is not None:
            self.__stop_request_handle.ret(b"Run successfully stopped.")
            self.__stop_request_handle = None
        
        return True, magics.OP_OK


    def __on_can_start_returned(self):
        if self.__can_start_event_handle.is_in_progress():
            return
        
        if self.__can_start_event_handle.get_event_state() != EVENT_OK:
            self.__abort_run("Run start request failed.")
            return
        
        states = self.__can_start_event_handle.get_states()

        log_responses = {}

        for s_uuid, (state, reason) in states.items():
            s_name = self.__required_subsystem_name(s_uuid)
            log_responses[str(s_uuid)] = {
                "state": state,
                "reason": reason.decode() if reason is not None else None,
                "name": s_name,
            }

            if state == magics.EVENT_PENDING or state == magics.EVENT_IN_PROGRESS:
                self.__abort_run(f"Subsystem {s_name} has timed out.")
                return
            
            if state != magics.EVENT_OK and reason != magics.E_DOES_NOT_HANDLE_EVENT and reason != magics.E_SUBSYSTEM_DISCONNECTED:
                self.__abort_run(f"Run start rejected by subsystem {s_name} due to {reason.decode('utf-8')}.")
                return
            
        for required, required_name in self.__require_subsystems.items():
            if required not in states:
                self.__abort_run(f"Required subsystem {required_name} did not respond to run start request.")
                return
            state, reason = states[required]


            if state != magics.EVENT_OK:
                if reason == magics.E_DOES_NOT_HANDLE_EVENT or reason == magics.E_SUBSYSTEM_DISCONNECTED:
                    self.__abort_run(f"Required subsystem {required_name} is disconnected, aborting run start.")
                else:
                    self.__abort_run(f"Required subsystem {required_name} responded with {reason.decode()}, aborting run start.")
                return
            
        self.__can_start_event_handle = None
        self.__set_run_phase(self.RUN_STATE_PREINIT)
        self.__declare_expected_event_streams()

        self.__logger.log("All subsystems OK, starting run preparation.", level="DEBUG", l_type="EXP", subsystem=self.name, responses=log_responses, event="can_begin_run_ok", exp_type=self.exp_type)
        self.__start_run_handle.feedback(b"Preinitiation started.")
        b_s_data = self.__settings.encode().encode("utf-8")
        b_state_data = self.__current_run.encode().encode("utf-8")
        self.__preinit_handle = self.__preinit_provider.call(segment_bytes.encode([b_s_data, b_state_data]), target=[])

    def __on_preinit_returned(self):
        if self.__preinit_handle.is_in_progress():
            return
        
        if self.__preinit_handle.get_event_state() != EVENT_OK:
            self.__abort_run("Run preinitialization failed.")
            return
        
        states = self.__preinit_handle.get_states()

        log_responses = {}

        for s_uuid, (state, reason) in states.items():
            s_name = self.__required_subsystem_name(s_uuid)
            log_responses[str(s_uuid)] = {
                "state": state,
                "reason": reason.decode() if reason is not None else None,
                "name": s_name,
            }
            if state == magics.EVENT_PENDING or state == magics.EVENT_IN_PROGRESS:
                self.__abort_run(f"Subsystem {s_name} has timed out.")
                return
            
            if state != magics.EVENT_OK and reason != magics.E_DOES_NOT_HANDLE_EVENT and reason != magics.E_SUBSYSTEM_DISCONNECTED:
                self.__abort_run(f"Run preinitialization rejected by subsystem {s_name} due to {reason.decode('utf-8')}.")
                return
        
        for required, required_name in self.__require_subsystems.items():
            if required not in states:
                self.__abort_run(f"Required subsystem {required_name} did not respond to run preinitialization.")
                return
            
            state, reason = states[required]
            if state != magics.EVENT_OK:
                if reason == magics.E_DOES_NOT_HANDLE_EVENT or reason == magics.E_SUBSYSTEM_DISCONNECTED:
                    self.__abort_run(f"Required subsystem {required_name} is disconnected or does not handle event, aborting run start.")
                else:
                    self.__abort_run(f"Required subsystem {required_name} responded with {reason.decode()}, aborting run start.")
                return
            
        self.__logger.log("All subsystems preinit OK, starting init.", level="DEBUG", l_type="EXP", subsystem=self.name, event="preinit_run", responses=log_responses, exp_type=self.exp_type)
        self.__start_run_handle.feedback(b"Preinit complete, starting init.")

        self.__preinit_handle = None
        self.__set_run_phase(self.RUN_STATE_INIT)

        b_s_data = self.__settings.encode().encode("utf-8")
        b_state_data = self.__current_run.encode().encode("utf-8")

        self.__init_handle = self.__init_provider.call(segment_bytes.encode([b_s_data, b_state_data]), target=[])

    def __on_init_returned(self):
        if self.__init_handle.is_in_progress():
            return
        
        if self.__init_handle.get_event_state() != EVENT_OK:
            self.__abort_run("Run initiation failed.")
            return
        
        states = self.__init_handle.get_states()

        log_responses = {}

        for s_uuid, (state, reason) in states.items():
            s_name = self.__required_subsystem_name(s_uuid)
            log_responses[str(s_uuid)] = {
                "state": state,
                "reason": reason.decode() if reason is not None else None,
                "name": s_name,
            }

            if state == magics.EVENT_PENDING or state == magics.EVENT_IN_PROGRESS:
                print("Subsystem", s_name, "still pending/in progress, aborting run start.")
                return
            
            if state != magics.EVENT_OK and reason != magics.E_DOES_NOT_HANDLE_EVENT and reason != magics.E_SUBSYSTEM_DISCONNECTED:
                self.__abort_run(f"Run initiation rejected by subsystem {s_name} due to {reason.decode('utf-8')}.")
                return
        
        for required, required_name in self.__require_subsystems.items():
            if required not in states:
                self.__abort_run(f"Required subsystem {required_name} did not respond to run initiation.")
                return
            
            state, reason = states[required]
            if state != magics.EVENT_OK:
                if reason == magics.E_DOES_NOT_HANDLE_EVENT or reason == magics.E_SUBSYSTEM_DISCONNECTED:
                    self.__abort_run(f"Required subsystem {required_name} is disconnected or does not handle event, aborting run start.")
                else:
                    self.__abort_run(f"Required subsystem {required_name} responded with {reason.decode()}, aborting run start.")
                return
            
        self.__logger.log("All subsystems init OK, run started.", level="DEBUG", l_type="EXP", subsystem=self.name, exp_type=self.exp_type)
        self.__init_handle = None
        self.__set_run_phase(self.RUN_STATE_RUNNING)

        self.__logger.log(f"Began run {str(self.__current_run.get_uuid())[-8:]}.", level="INFO", l_type="EXP", subsystem=self.name, run=self.__current_run.get_dict(), event="begin_run", responses=log_responses, exp_type=self.exp_type)

        self.__start_run_handle.ret(b"Run successfully started with UUID: " + str(self.__run_record.get_state().get_uuid()).encode("utf-8"))
        self.__start_run_handle = None

    def __handle_set(self, h, requester, v):
        allowed, reason = self.__automation_lease.can_update_settings(
            requester,
            self.__current_run is not None,
        )
        if not allowed:
            return (magics.TRANSOP_STATE_REJ, reason.encode("utf-8"))

        bytes_d = segment_bytes.decode(v)

        if len(bytes_d) != 2:
            return (magics.TRANSOP_STATE_REJ, b"Invalid data format for settings update.")
        try:
            self.__settings.set_attr(bytes_d[0].decode("utf-8"), bytes_d[1].decode("utf-8"))
        except (ValueError, AssertionError) as e:
            return (magics.TRANSOP_STATE_REJ, f"Invalid value for setting: {e}".encode("utf-8"))
        return (magics.TRANSOP_STATE_OK, bytes())

    def __handle_get(self, requester):
        val = self.__settings.encode().encode("utf-8")
        return (magics.TRANSOP_STATE_OK, val)

    def __append_external_run_event(self, event: RunEvent) -> bool:
        record = RunRecord(self.__logger, self.__library, self, event.run_id)
        state = record.get_state()
        if state.get_type() != self.exp_type:
            raise ValueError("Run event targets another experiment type.")
        try:
            version = int(record.get_tags().get("version", 0))
        except (TypeError, ValueError):
            version = 0
        if version < RunRecord.CURRENT_DATA_VERSION:
            raise ValueError("Historical run records do not accept external run events.")
        return record.append_event(event)

    def __on_append_run_event(self, sender_uuid, payload, handle) -> None:
        if not isinstance(payload, bytes) or len(payload) > self.MAX_RUN_EVENT_PAYLOAD_BYTES:
            handle.fail(b"Run event payload is too large or invalid.")
            return
        try:
            event = RunEvent.decode(payload)
            expected = self.__expected_event_streams.get(event.stream_name)
            if expected is None:
                raise ValueError(f"Run event stream {event.stream_name!r} is not configured.")
            submitter_id, producer_id = expected
            if sender_uuid != submitter_id or event.submitter_id != submitter_id:
                raise ValueError("Run event submitter does not match the configured subsystem.")
            if event.producer_id != producer_id:
                raise ValueError("Run event producer does not match the configured source.")
            expected_stream_id = run_event_stream_id(event.run_id, submitter_id, event.stream_name)
            if event.stream_id != expected_stream_id:
                raise ValueError("Run event stream ID is not canonical for this run and submitter.")
            persisted = replace(event, ingest_unix_ns=time.time_ns())
            self.__data_thread_enqueue(self.__append_external_run_event, persisted)
        except Exception as exc:
            handle.fail(f"Run event was not persisted: {type(exc).__name__}: {exc}".encode("utf-8"))
            return
        handle.ret(persisted.event_id.bytes)


    def __on_got_subsystem(self, handle: client._RegisteredSubsystemHandle):
        self.__subsystem = handle

        self.__can_start_event_provider = handle.add_event_provider(b"can_begin_" + self.exp_type.encode("utf-8"))

        self.__preinit_provider = handle.add_event_provider(b"preinit_" + self.exp_type.encode("utf-8"))
        self.__init_provider = handle.add_event_provider(b"init_" + self.exp_type.encode("utf-8"))

        self.__stop_provider = handle.add_event_provider(b"stopped_" + self.exp_type.encode("utf-8"))

        handle.add_event_handler(b"prepare_" + self.exp_type.encode("utf-8")).on_called(self.__on_start_run_event)
        handle.add_event_handler(
            b"prepare_" + self.exp_type.encode("utf-8") + b"_with_tags"
        ).on_called(self.__on_start_tagged_run_event)
        handle.add_event_handler(b"acquire_" + self.exp_type.encode("utf-8") + b"_automation").on_called(
            self.__on_acquire_automation_event
        )
        handle.add_event_handler(b"release_" + self.exp_type.encode("utf-8") + b"_automation").on_called(
            self.__on_release_automation_event
        )
        handle.add_event_handler(b"stop_" + self.exp_type.encode("utf-8")).on_called(self.__on_stop_run_event)
        handle.add_event_handler(b"append_" + self.exp_type.encode("utf-8") + b"_run_event").on_called(
            self.__on_append_run_event
        )

        self.__state_kv = self.__subsystem.get_kv_property(b"experiment_state", False, True, True)
        self.__reasons_kv = self.__subsystem.get_kv_property(b"experiment_reasons", False, True, True)

        self.__stop_kv = self.__subsystem.get_kv_property(b"run_finalized", False, True, True)

        self.__automation_lease_kv = self.__subsystem.get_kv_property(
            b"automation_lease", False, True, True
        )
        self.__publish_automation_lease()

        set_kv_h = self.__subsystem.add_kv_handler(b"settings")
        set_kv_h.on_set(self.__handle_set)
        set_kv_h.on_get(self.__handle_get)

    def __request_states(self):
        rets = dict()

        for req in self.__require_subsystems.keys():
            rets[req] = (OP_IN_PROGRESS, None)
            self.__subsystem.get_kv(req, b"exp_state").then(lambda v, req=req: rets.update({req: (OP_OK, v)})).catch(lambda state, reason, req=req: rets.update({req: (state, reason)}))

        timeout = time.time() + 5.0
        while time.time() < timeout:
            all_done = True
            for req in self.__require_subsystems.keys():
                state, reason = rets[req]
                if state == OP_IN_PROGRESS:
                    all_done = False
                    break
            
            if all_done:
                break
            
            time.sleep(0.1)
        
        return rets

    def add_required_subsystem(self, s_uuid: uuid.UUID, name: str):
        self.__require_subsystems[s_uuid] = name

    def add_expected_run_event_stream(
        self,
        submitter_uuid: uuid.UUID,
        stream_name: str,
        *,
        producer_uuid: uuid.UUID | None = None,
    ) -> None:
        if not isinstance(submitter_uuid, uuid.UUID):
            raise ValueError("Expected run event submitter must be a UUID.")
        if not isinstance(stream_name, str) or not stream_name.strip() or stream_name != stream_name.strip():
            raise ValueError("Expected run event stream name must be non-empty trimmed text.")
        producer = submitter_uuid if producer_uuid is None else producer_uuid
        if not isinstance(producer, uuid.UUID):
            raise ValueError("Expected run event producer must be a UUID.")
        self.__expected_event_streams[stream_name] = (submitter_uuid, producer)

    def ok(self):
        return self.__run and self.__client.ok()
    
    def close(self):
        if self.__current_run is not None:
            try:
                self.__logger.log(f"Shutting down while run is running: {str(self.__current_run.get_uuid())[-8:]}!", level="ERROR", l_type="EXP", subsystem=self.name, run=self.__current_run.get_dict(), exp_type=self.__current_run.get_type())
                self.__abort_run("Run controller shutting down.")
            except Exception as e:
                self.__logger.log(f"Error aborting run while shutting down: {e}", level="ERROR", l_type="EXP", subsystem=self.name)
                self.__finalize_run("ABORTED", "Run controller shutting down due to error.")
                raise
        
        self.__daemon.stop()
        self.__client.close()
        self.__logger_sock.close()

        self.__run = False

    def register_experiment_settings_type(self, settings_type: type[RunSettings]):
        self.__settings_type = settings_type
        self.__settings = self.__settings_type()

class ExperimentReader:
    def __init__(self, data_path: str, exp_name: str, *, read_only: bool = False):
        self.__library = Library(data_path, read_only=read_only)
        self.__exp_name = exp_name

    def __build_query(self, query: dict | None = None, tags: dict | None = None) -> dict:
        query_args = {} if query is None else dict(query)
        query_tags = dict(query_args.get("tags", {}))
        if tags is not None:
            query_tags.update(tags)
        query_tags["experiment"] = self.__exp_name
        query_args["tags"] = query_tags
        return query_args

    def locate_runs_by_name(self, name: str) -> list[RunRecord]:
        q_tags = {
            "experiment": self.__exp_name,
        }
        q_args = {
            "name": name,
            "tags": q_tags,
        }

        entries = self.__library.query(q_args, limit=None)
        runs = []

        for entry in entries:
            try:
                data_manager = RunRecord(None, self.__library, None, uuid.UUID(entry.get_tags().get("run")))
                runs.append(data_manager)
            except Exception as e:
                print(f"Error loading run record for entry {entry.get_uuid()}: {e}")
        
        return runs
    
    def locate_runs_by_timestamp(self, date_min: float = None, date_max: float = None) -> list[RunRecord]:
        q_tags = {
            "experiment": self.__exp_name,
        }
        q_args = {
            "created_min": date_min,
            "created_max": date_max,
            "tags": q_tags,
        }

        entries = self.__library.query(q_args, limit=None)
        runs = []

        for entry in entries:
            try:
                data_manager = RunRecord(None, self.__library, None, uuid.UUID(entry.get_tags().get("run")))
                runs.append(data_manager)
            except Exception as e:
                print(f"Error loading run record for entry {entry.get_uuid()}: {e}")
        
        return runs
    
    def query(
        self,
        query: dict,
        limit: int = None,
        *,
        offset: int = 0,
        cursor=None,
    ) -> list[RunRecord]:
        q_args = self.__build_query(query)
        entries = self.__library.query(q_args, limit=limit, offset=offset, cursor=cursor)
        runs = []

        for entry in entries:
            try:
                data_manager = RunRecord(
                    None,
                    self.__library,
                    None,
                    uuid.UUID(entry.get_tags().get("run")),
                    entry=entry,
                )
                runs.append(data_manager)
            except Exception as e:
                print(f"Error loading run record for entry {entry.get_uuid()}: {e}")
        
        return runs
    
    def locate_run_by_uuid(self, r_uuid: uuid.UUID) -> RunRecord | None:
        q_tags = {
            "experiment": self.__exp_name,
            "run": r_uuid.hex,
        }
        q_args = {
            "tags": q_tags,
        }

        entries = self.__library.query(q_args, limit=1)

        if len(entries) == 0:
            return None
        
        entry = entries[0]
        try:
            data_manager = RunRecord(None, self.__library, None, uuid.UUID(entry.get_tags().get("run")))
            return data_manager
        except Exception as e:
            print(f"Error loading run record for entry {entry.get_uuid()}: {e}")
            return None

    def list_runs(
        self,
        q_tags: dict = None,
        q_args: dict = None,
        limit: int = None,
        *,
        offset: int = 0,
        cursor=None,
    ) -> list[RunRecord]:
        """
        Query function for runs.
        
        filters: dict with optional keys:
        - 'name': str (substring match, case-insensitive)
        - 'description': str (substring match, case-insensitive)
        - 'created_min': int (timestamp >=)
        - 'created_max': int (timestamp <=)
        - 'tags': dict[str, any] where each value can be:
          - str: exact string match
          - dict with 'min' and/or 'max': numeric range
          - None: check if tag key exists (regardless of value)
        limit: optional int, maximum number of results, ordered by creation date (most recent first)
        """

        q_args = self.__build_query(q_args, q_tags)
        
        print("Querying runs with args:", q_args, "and limit:", limit)
        entries = self.__library.query(q_args, limit=limit, offset=offset, cursor=cursor)
        print(f"Found {len(entries)} entries matching query.")
        runs = []

        failed = 0

        for entry in entries:
            try:
                #print(f"Loading run record for entry {entry.get_uuid()} with tags {entry.get_tags()}")
                data_manager = RunRecord(None, self.__library, None, uuid.UUID(entry.get_tags().get("run")), entry=entry)
                #print(f"Loaded run record for entry {entry.get_uuid()}: name='{data_manager.get_name()}', description='{data_manager.get_description()}', tags={data_manager.get_tags()}")
                runs.append(data_manager)
            except Exception as e:
                print(f"Error loading run record for entry {entry.get_uuid()}: {e}")
                for line in traceback.format_exception(None, e, e.__traceback__):
                    for split in line.split('\n'):
                        print(split)

                failed += 1
        
        print("Done loading runs, total loaded:", len(runs), "failed to load:", failed)
        return runs

    def count(self, query: dict | None = None) -> int:
        return self.__library.count(self.__build_query(query))

    def close(self) -> None:
        self.__library.close()
    
    def get_run(self, r_uuid: uuid.UUID) -> RunRecord:
        return RunRecord(None, self.__library, None, r_uuid)

def demo_main(stop_event: "multiprocessing.Event"):
    __SAVE_PATH = os.path.join(os.environ["EUVL_PATH"], "datasets")
    m_run_controller = ExperimentController("ExperimentController", uuid.uuid3(uuid.NAMESPACE_OID, "ExperimentController"), "my_experiment", __SAVE_PATH)

    try:
        while m_run_controller.ok() and not (stop_event is not None and stop_event.is_set()):
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        m_run_controller.close()



def print_seqs(stop_event: "multiprocessing.Event"):
    __SAVE_PATH = os.path.join(os.environ["EUVL_PATH"], "datasets")
    m_exp_reader = ExperimentReader(__SAVE_PATH, "my_experiment")
    runs = m_exp_reader.list_runs()
    print("Found runs:", runs)

    for run in runs:
        print("Run UUID:", run.get_state().get_uuid())
        print("Run Name:", run.get_name())
        print("Run Description:", run.get_description())
        print("Run Tags:", run.get_tags())
        print("Run Metadata:", run.get_metadata())
        print("Run End Metadata:", run.get_end_metadata())

    if len(runs) > 0:
        runs[0].set_name("Updated Run Name")
        runs[0].set_description("Updated Run Description")
        time.sleep(0.1)

if __name__ == "__main__":
    demo_main(None)