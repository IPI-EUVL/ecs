import multiprocessing
import os
import pickle
import queue
import time
import uuid
import mt_events
import segment_bytes
import json

from enum import Enum

from ipi_ecs.core import daemon
import ipi_ecs.dds.subsystem as subsystem
import ipi_ecs.dds.types as types
import ipi_ecs.dds.client as client
import ipi_ecs.dds.magics as magics
import ipi_ecs.core.tcp as tcp
from ipi_ecs.dds.magics import *

from ipi_ecs.logging.client import LogClient
from ipi_ecs.db.db_library import Library

class RunSettings:
    data = {}

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
    CURRENT_DATA_VERSION = 1

    def __init__(self, logger: LogClient, library: Library, controller: "ExperimentController", r_uuid: uuid.UUID):
        self.__entry = None
        self.__event_uuid = None

        self.__logger = logger
        self.__library = library
        self.__run_uuid = r_uuid

        self.__state = None
        self.__controller = controller

        self.read(self.__run_uuid)

    @staticmethod
    def create(logger: LogClient, library: Library, state: RunState, settings: RunSettings, controller: "ExperimentController"):
        name = state.get_name() if state.get_name() is not None else f"Run {str(state.get_uuid())[-8:]}"
        desc = state.get_description() if state.get_description() is not None else f"Run on {time.ctime()}"
        entry = library.create_entry(name, desc)

        for k, v in settings.get_dict().items():
            entry.set_tag(k, v)

        entry.set_tag("experiment", state.get_type())
        entry.set_tag("run", state.get_uuid().hex)
        entry.set_tag("version", RunRecord.CURRENT_DATA_VERSION)

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

        return RunRecord(logger, library, controller, state.get_uuid())
    
    def read(self, s_uuid: uuid.UUID):
        entry = self.__library.query({"tags": {"run": s_uuid.hex}}, limit=1)

        if entry is None or len(entry) == 0:
            raise ValueError(f"Run with UUID {str(s_uuid)} not found in library.")
        
        entry = entry[0]
        
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

    def write_end(self, state: RunState, status: str, reason: str):
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

            self.__logger.end_event(self.__event_uuid, status=status, reason=reason)

    def get_record(self):
        return self.__entry
    
    def get_state(self) -> RunState:
        return self.__state
    
    def get_metadata(self):
        return self.__metadata
    
    def get_end_metadata(self):
        return self.__end_metadata

class ExperimentController:
    RUN_OK = 0
    RUN_ABORT = 1

    RUN_STATE_PREINIT = 0
    RUN_STATE_INIT = 1
    RUN_STATE_RUNNING = 2
    RUN_STATE_STOPPING = 3
    RUN_STATE_STOPPED = 4

    name = "ExperimentController"
    exp_type = "my_experiment"

    def __init__(self, name: str, s_uuid: uuid.UUID, exp_type: str, data_path: str):
        self.__run = True

        self.name = name
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

        self.__state_kv = None

        self.__run_record = None
        self.__event_uuid = None

        self.__settings_type = RunSettings
        self.__settings = self.__settings_type()
        self.__current_run = None

        self.__next_run_uuid = None

        self.__require_subsystems = [
           # uuids.UUID_TARGET_CONTROLLER,
        ]

        self.__library = None

        self.__states = dict()

        self.__data_thread_queue = queue.Queue()
        self.__data_thread_out_queue = queue.Queue()

        self.__last_should_continue_check = 0

        self.__daemon = daemon.Daemon()
        self.__daemon.add(self.__data_thread)
        self.__daemon.add(self.__thread)

        self.__daemon.start()

    def __thread(self, stop_flag: daemon.StopFlag):
        while stop_flag.run() and self.__run:
            if self.__can_start_event_handle is not None and not self.__can_start_event_handle.is_in_progress():
                self.__on_can_start_returned()

            if self.__has_timed_out(self.__can_start_event_handle, 30):
                self.__abort_run("Run start request timed out.")
            
            if self.__preinit_handle is not None and not self.__preinit_handle.is_in_progress():
                self.__on_preinit_returned()

            if self.__has_timed_out(self.__preinit_handle, 30):
                self.__abort_run("Preinit request timed out.")

            if self.__init_handle is not None and not self.__init_handle.is_in_progress():
                self.__on_init_returned()

            if self.__has_timed_out(self.__init_handle, 30):
                self.__abort_run("Init request timed out.")

            if self.__stop_handle is not None and not self.__stop_handle.is_in_progress():
                self.__on_stop_returned()

            if self.__has_timed_out(self.__stop_handle, 30):
                self.__abort_run("Stop request timed out.")

            self.__update_state()

            if self.__current_run is not None and self.__start_run_handle is None:
                if time.time() - self.__last_should_continue_check < 1.0:
                    continue

                self.__last_should_continue_check = time.time()
                should_continue, reason = self.__should_continue()
                if not should_continue:
                    self.__abort_run(reason)

            time.sleep(0.1)

    def __update_state(self):
        if self.__state_kv is None:
            return
        
        if self.__current_run is not None:
            if self.__preinit_handle is not None:
                self.__state_kv.value = segment_bytes.encode([self.RUN_STATE_PREINIT.to_bytes(1, "big"), self.__current_run.encode().encode("utf-8")])
            elif self.__init_handle is not None:
                self.__state_kv.value = segment_bytes.encode([self.RUN_STATE_INIT.to_bytes(1, "big"), self.__current_run.encode().encode("utf-8")])
            elif self.__stop_handle is not None:
                self.__state_kv.value = segment_bytes.encode([self.RUN_STATE_STOPPING.to_bytes(1, "big"), self.__current_run.encode().encode("utf-8")])
            else:
                self.__state_kv.value = segment_bytes.encode([self.RUN_STATE_RUNNING.to_bytes(1, "big"), self.__current_run.encode().encode("utf-8")])
        else:
            self.__state_kv.value = segment_bytes.encode([self.RUN_STATE_STOPPED.to_bytes(1, "big"), bytes()])

    def __has_timed_out(self, event_handle: client._InProgressEvent._Handle, timeout: float) -> bool:
        if event_handle is None:
            return False
        
        t_initiated = event_handle.get_time_initiated()
        last_update = event_handle.get_last_update()

        now = time.time()

        if now - t_initiated < timeout:
            return False
        
        if now - last_update < timeout:
            return False
        
        return True
    
    def __should_continue(self):
        s = self.__subsystem.get_all()
        for _handle, _state in s:
            if _handle.get_info().get_uuid() not in self.__require_subsystems:
                continue

            if _state.get_status() != subsystem.SubsystemStatus.STATE_ALIVE:
                return False, f"Required subsystem {_handle.get_info().get_uuid()} has died."

        state_vs = self.__request_states()
        for s_uuid in self.__require_subsystems:
            if s_uuid not in state_vs:
                return False, f"Required subsystem {s_uuid} did not provide state KV."

            state, v = state_vs[s_uuid]

            if state == OP_IN_PROGRESS:
                return False, f"Attempt to fetch status of {s_uuid} has timed out."

            if state != OP_OK:
                return False, f"Attempt to fetch status of {s_uuid} returned non-OK({state}): {v if v is not None else 'No reason provided'}."
            
            b_ok, state = segment_bytes.decode(v)
            ok = bool.from_bytes(b_ok, "big")

            if not ok:
                return False, f"Subsystem {s_uuid} reported not OK status: {state.decode('utf-8') if state is not None else 'No reason provided'}."
            
            self.__states[s_uuid] = state
            
        
        return True, None
                

    def __data_thread(self, stop_flag: daemon.StopFlag):
        self.__library = Library(self.data_path)
    
        while stop_flag.run() and self.__run:
            try:
                fn, pargs, kwargs = self.__data_thread_queue.get(timeout=1)
            except queue.Empty:
                time.sleep(0.01)
                continue

            r = fn(*pargs, **kwargs)
            self.__data_thread_out_queue.put(r)

    def __data_thread_enqueue(self, fn, *pargs, **kwargs):
        self.__data_thread_queue.put((fn, pargs, kwargs))

        return self.__data_thread_out_queue.get()

    def __create_run(self):
        self.__current_run = RunState(self.exp_type, self.__settings, s_uuid=self.__next_run_uuid)
        self.__run_record = self.__data_thread_enqueue(RunRecord.create, self.__logger, self.__library, self.__current_run, self.__settings, self)

    def __abort_run(self, reason: str):
        print("Aborting run:", reason)
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
            self.__stop_provider.call(segment_bytes.encode([self.RUN_ABORT.to_bytes(1, "big"), self.__current_run.get_uuid().bytes, reason.encode("utf-8")]), target=[])
        else:
            self.__stop_provider.call(segment_bytes.encode([self.RUN_ABORT.to_bytes(1, "big"), bytes(), reason.encode("utf-8")]), target=[])

        if self.__run_record is not None:
            self.__finalize_run("ABORTED", reason)
            self.__run_record = None

    def __on_start_run_event(self, s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
        print("Start run event called by:", s_uuid, param)
        self.__start_run_handle = handle
        s, r = self.__try_start_run()
        if s:
            handle.feedback(r)
        else:
            handle.fail(r)

    def __on_stop_run_event(self, s_uuid, param, handle: client._EventHandler._IncomingEventHandle):
        print("Stop run event called by:", s_uuid, param)
        self.__stop_request_handle = handle
        self.stop_run(param.decode("utf-8"))

    def __try_start_run(self):
        self.__next_run_uuid = uuid.uuid4()
        self.__create_run()

        self.__logger.log("Attempting to begin new run: " + str(self.__next_run_uuid) + "...", level="DEBUG", l_type="EXP", subsystem=self.name)

        if self.__preinit_handle is not None or self.__init_handle is not None or self.__stop_handle is not None:
            self.__logger.log("Cannot start new run while another is in progress!", level="WARN", l_type="EXP", subsystem=self.name)
            return False, b"Cannot start new run while another is in progress!"

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

        self.__stop_reason = reason
        self.__stop_handle = self.__stop_provider.call(segment_bytes.encode([self.RUN_OK.to_bytes(1, "big"), self.__current_run.get_uuid().bytes, reason.encode("utf-8")]), target=[])

    def __finalize_run(self, code: str, reason: str):
        self.__data_thread_enqueue(self.__run_record.write_end, self.__current_run, code, reason)
        self.__run_record = None

        self.__logger.log(f"Run {str(self.__current_run.get_uuid())[-8:]} has been finalized with code " + code + ": " + reason, level="DEBUG", l_type="EXP", subsystem=self.name, run=self.__current_run.get_dict(), reason=reason, exp_type=self.__current_run.get_type())
        self.__current_run = None

    def __on_stop_returned(self):
        self.__logger.log(f"Run {str(self.__current_run.get_uuid())[-8:]} stopped: " + self.__stop_reason, level="INFO", l_type="EXP", subsystem=self.name, run=self.__current_run.get_dict(), reason=self.__stop_reason, event="stop_run", exp_type=self.__current_run.get_type())
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
            log_responses[str(s_uuid)] = {
                "state": state,
                "reason": reason.decode() if reason is not None else None,
            }

            if state == magics.EVENT_PENDING or state == magics.EVENT_IN_PROGRESS:
                self.__abort_run(f"Subsystem {s_uuid} has timed out.")
                return
            
            if state != magics.EVENT_OK and reason != magics.E_DOES_NOT_HANDLE_EVENT and reason != magics.E_SUBSYSTEM_DISCONNECTED:
                self.__abort_run(f"Run start rejected by subsystem {s_uuid} due to {reason.decode("utf-8")}.")
                return
            
        for required in self.__require_subsystems:
            if required not in states:
                self.__abort_run(f"Required subsystem {required} did not respond to run start request.")
                return
            state, reason = states[required]


            if state != magics.EVENT_OK:
                if reason == magics.E_DOES_NOT_HANDLE_EVENT or reason == magics.E_SUBSYSTEM_DISCONNECTED:
                    self.__abort_run(f"Required subsystem {required} is disconnected, aborting run start.")
                else:
                    self.__abort_run(f"Required subsystem {required} responded with {reason.decode()}, aborting run start.")
                return
            
        self.__can_start_event_handle = None
            
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
            log_responses[str(s_uuid)] = {
                "state": state,
                "reason": reason.decode() if reason is not None else None,
            }
            if state == magics.EVENT_PENDING or state == magics.EVENT_IN_PROGRESS:
                self.__abort_run(f"Subsystem {s_uuid} has timed out.")
                return
            
            if state != magics.EVENT_OK and reason != magics.E_DOES_NOT_HANDLE_EVENT and reason != magics.E_SUBSYSTEM_DISCONNECTED:
                self.__abort_run(f"Run preinitialization rejected by subsystem {s_uuid} due to {reason.decode("utf-8")}.")
                return
        
        for required in self.__require_subsystems:
            if required not in states:
                self.__abort_run(f"Required subsystem {required} did not respond to run preinitialization.")
                return
            
            state, reason = states[required]
            if state != magics.EVENT_OK:
                if reason == magics.E_DOES_NOT_HANDLE_EVENT or reason == magics.E_SUBSYSTEM_DISCONNECTED:
                    self.__abort_run(f"Required subsystem {required} is disconnected or does not handle event, aborting run start.")
                else:
                    self.__abort_run(f"Required subsystem {required} responded with {reason.decode()}, aborting run start.")
                return
            
        self.__logger.log("All subsystems preinit OK, starting init.", level="DEBUG", l_type="EXP", subsystem=self.name, event="preinit_run", responses=log_responses, exp_type=self.exp_type)
        self.__start_run_handle.feedback(b"Preinit complete, starting init.")

        self.__preinit_handle = None

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
            log_responses[str(s_uuid)] = {
                "state": state,
                "reason": reason.decode() if reason is not None else None,
            }

            if state == magics.EVENT_PENDING or state == magics.EVENT_IN_PROGRESS:
                print("Subsystem", s_uuid, "still pending/in progress, aborting run start.")
                return
            
            if state != magics.EVENT_OK and reason != magics.E_DOES_NOT_HANDLE_EVENT and reason != magics.E_SUBSYSTEM_DISCONNECTED:
                self.__abort_run(f"Run initiation rejected by subsystem {s_uuid} due to {reason.decode('utf-8')}.")
                return
        
        for required in self.__require_subsystems:
            if required not in states:
                self.__abort_run(f"Required subsystem {required} did not respond to run initiation.")
                return
            
            state, reason = states[required]
            if state != magics.EVENT_OK:
                if reason == magics.E_DOES_NOT_HANDLE_EVENT or reason == magics.E_SUBSYSTEM_DISCONNECTED:
                    self.__abort_run(f"Required subsystem {required} is disconnected or does not handle event, aborting run start.")
                else:
                    self.__abort_run(f"Required subsystem {required} responded with {reason.decode()}, aborting run start.")
                return
            
        self.__logger.log("All subsystems init OK, run started.", level="DEBUG", l_type="EXP", subsystem=self.name, exp_type=self.exp_type)
        self.__init_handle = None

        self.__logger.log(f"Began run {str(self.__current_run.get_uuid())[-8:]}.", level="INFO", l_type="EXP", subsystem=self.name, run=self.__current_run.get_dict(), event="begin_run", responses=log_responses, exp_type=self.exp_type)

        self.__start_run_handle.ret(b"Run successfully started with UUID: " + str(self.__run_record.get_state().get_uuid()).encode("utf-8"))
        self.__start_run_handle = None


    def __on_got_subsystem(self, handle: client._RegisteredSubsystemHandle):
        self.__subsystem = handle

        self.__can_start_event_provider = handle.add_event_provider(b"can_begin_" + self.exp_type.encode("utf-8"))

        self.__preinit_provider = handle.add_event_provider(b"preinit_" + self.exp_type.encode("utf-8"))
        self.__init_provider = handle.add_event_provider(b"init_" + self.exp_type.encode("utf-8"))

        self.__stop_provider = handle.add_event_provider(b"stopped_" + self.exp_type.encode("utf-8"))

        handle.add_event_handler(b"prepare_" + self.exp_type.encode("utf-8")).on_called(self.__on_start_run_event)
        handle.add_event_handler(b"stop_" + self.exp_type.encode("utf-8")).on_called(self.__on_stop_run_event)

        self.__state_kv = self.__subsystem.get_kv_property(b"experiment_state", False, True, True)

    def __request_states(self):
        rets = dict()

        for req in self.__require_subsystems:
            rets[req] = (OP_IN_PROGRESS, None)
            self.__subsystem.get_kv(req, b"exp_state").then(lambda v, req=req: rets.update({req: (OP_OK, v)})).catch(lambda state, reason, req=req: rets.update({req: (state, reason)}))

        timeout = time.time() + 5.0
        while time.time() < timeout:
            all_done = True
            for req in self.__require_subsystems:
                state, reason = rets[req]
                if state == OP_IN_PROGRESS:
                    all_done = False
                    break
            
            if all_done:
                break
            
            time.sleep(0.1)
        
        return rets

    def add_required_subsystem(self, s_uuid: uuid.UUID):
        if s_uuid not in self.__require_subsystems:
            self.__require_subsystems.append(s_uuid)

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
    def __init__(self, data_path: str, exp_name: str):
        self.__library = Library(data_path)
        self.__exp_name = exp_name

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
    
    def query(self, query: dict) -> list[RunRecord]:
        q_tags = {
            "experiment": self.__exp_name,
        }
        q_args = {
            "tags": q_tags,
        }
        q_args.update(query)

        entries = self.__library.query(q_args, limit=None)
        runs = []

        for entry in entries:
            try:
                data_manager = RunRecord(None, self.__library, None, uuid.UUID(entry.get_tags().get("run")))
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

    def list_runs(self, q_tags: dict = None, q_args: dict = None, limit: int = None) -> list[RunRecord]:
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

        q_tags = {} if q_tags is None else q_tags
        q_tags["experiment"] = self.__exp_name

        q_args = {} if q_args is None else q_args
        q_args["tags"] = q_tags
        
        entries = self.__library.query(q_args, limit=limit)
        runs = []

        for entry in entries:
            try:
                data_manager = RunRecord(None, self.__library, None, uuid.UUID(entry.get_tags().get("run")))
                runs.append(data_manager)
            except Exception as e:
                print(f"Error loading run record for entry {entry.get_uuid()}: {e}")
        
        return runs
    
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