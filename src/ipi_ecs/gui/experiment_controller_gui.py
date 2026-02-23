import tkinter as tk
from tkinter import ttk
import time
import math
import threading
import uuid
import mt_events
import segment_bytes
from queue import Empty, Queue

from ipi_ecs.core import daemon
from ipi_ecs.core.tcp import TCPClientSocket
from ipi_ecs.dds import client, subsystem, types, magics
from ipi_ecs.logging.client import LogClient
from ipi_ecs.subsystems.experiment_controller import RunState, RunSettings, ExperimentController

class ExperimentInterface:
    def __init__(self, exp_type: str, ctl_uuid: uuid.UUID, exp_settings_type = RunSettings):
        self.exp_type = exp_type
        self.ctl_uuid = ctl_uuid
        self.exp_settings_type = exp_settings_type

        c_uuid = uuid.uuid4()
        s_uuid = uuid.uuid4()

        self.__logger_sock = TCPClientSocket()

        self.__logger_sock.connect(("127.0.0.1", 11751))
        self.__logger_sock.start()

        self.__logger = LogClient(self.__logger_sock, origin_uuid=c_uuid)

        self.__did_config = False
        self.__subsystem = None

        self.__current_experiment = None
        self.__current_state = None

        self.__status_kv = None
        self.__settings_kv = None

        def _on_ready():
            if self.__did_config:
                return
            
            self.__did_config = True
            sh = self.__client.register_subsystem(f"__cli_{s_uuid}", s_uuid, temporary=True)

            self.__on_got_subsystem(sh)

        #print("Registering subsystem...")
        self.__client = client.DDSClient(c_uuid, logger=self.__logger)
        self.__client.when_ready().then(_on_ready)

    def __on_got_subsystem(self, handle: client._RegisteredSubsystemHandle):
        self.__subsystem = handle

        self.__status_kv = handle.add_remote_kv(self.ctl_uuid, subsystem.KVDescriptor(types.ByteTypeSpecifier(), b"experiment_state", True, True, False))
        self.__status_kv.on_new_data_received(self.__on_status_update)

        self.__settings_kv = handle.add_remote_kv(self.ctl_uuid, subsystem.KVDescriptor(types.ByteTypeSpecifier(), b"settings", False, True, True))

        self.__start_experiment_event_sender = handle.add_event_provider(f"prepare_{self.exp_type}".encode("utf-8"))
        self.__stop_experiment_event_sender = handle.add_event_provider(f"stop_{self.exp_type}".encode("utf-8"))

    def __on_status_update(self, n_status: bytes):
        b_status = segment_bytes.decode(n_status)
        if len(b_status) != 2:
            print("Invalid status update received: ", b_status)
            return
        
        self.__current_state = int.from_bytes(b_status[0], byteorder="big")

        if self.__current_state != ExperimentController.RUN_STATE_STOPPED:
            self.__current_experiment = RunState.decode(b_status[1].decode("utf-8"))
        else:
            self.__current_experiment = None

    def set_name(self, name: str, ret_type = client.KVP_RET_AWAIT):
        if self.__settings_kv is None:
            print("Settings KV not available yet.")
            return
        
        return self.__settings_kv.try_set(segment_bytes.encode([b"name", name.encode("utf-8")]), ret_type)
    
    def set_description(self, description: str, ret_type = client.KVP_RET_AWAIT):
        if self.__settings_kv is None:
            print("Settings KV not available yet.")
            return
        
        return self.__settings_kv.try_set(segment_bytes.encode([b"description", description.encode("utf-8")]), ret_type)
    
    def start_experiment(self):
        if self.__start_experiment_event_sender is None:
            print("Start event sender not available yet.")
            return
        
        return self.__start_experiment_event_sender.call(bytes(), [])
    
    def stop_experiment(self, reason: str = "Stopped by user."):
        if self.__stop_experiment_event_sender is None:
            print("Stop event sender not available yet.")
            return
        
        return self.__stop_experiment_event_sender.call(reason.encode("utf-8"), [])
    
    def get_state(self):
        return self.__current_state
    
    def get_experiment(self):
        return self.__current_experiment
    
    def close(self):
        self.__client.close()
        self.__logger_sock.close()

    def get_controller_uuid(self):
        return self.ctl_uuid
    
    def get_experiment_uuid(self):
        if self.__current_experiment is not None:
            return self.__current_experiment.get_uuid()
        return None
    
    def set_kw(self, key: str, value: str, ret_type = client.KVP_RET_AWAIT):
        if self.__settings_kv is None:
            print("Settings KV not available yet.")
            return
        
        return self.__settings_kv.try_set(segment_bytes.encode([key.encode("utf-8"), value.encode("utf-8")]), ret_type)
    
    def get_exp_settings_type(self):
        return self.exp_settings_type

class ExperimentControllerGUI:
    def __init__(self, root, itf : ExperimentInterface):
        self.root = root
        root.title("Experiment Controller GUI")

        self.__itf = itf

        self.__settings_entries = []
        self.__update_queue = Queue()
        
        #GUI setup 
        self.__initialize_component()
        self.handle_window()

        self.__op_event_handle = None
        self.__op_transop_handle = None
        self.__current_op = None

        self.__status_style = ttk.Style()
        self.__status_style.configure("Status.TLabel", font=("Arial", 30), width=20, anchor=tk.CENTER)

        #threading.Thread(target=self.__update_thread, daemon=True).start()
        self.__updater()

        self.__daemon = daemon.Daemon()
        self.__daemon.add(self.__update_thread)
        self.__daemon.start()

    def __initialize_component(self):
        main_frame = ttk.Frame(self.root, padding=10)
        main_frame.pack(fill=tk.BOTH, expand=True)

        self.__status_frame = ttk.LabelFrame(main_frame, text="Current Experiment Status", padding=10)
        self.__status_frame.pack(fill=tk.BOTH, expand=True)
        self.__status_label = ttk.Label(self.__status_frame, text="No experiment running.", style="Status.TLabel")
        self.__status_label.pack(side=tk.TOP, pady=5)

        self.__uuid_label = ttk.Label(self.__status_frame, text="", font=("Arial", 12))
        self.__uuid_label.pack(side=tk.TOP, pady=5)

        self.__control_frame = ttk.LabelFrame(main_frame, text="Controls", padding=10)
        self.__control_frame.pack(fill=tk.BOTH, expand=True)

        start_button = ttk.Button(self.__control_frame, text="Start Experiment", command=self.__on_start_experiment)
        start_button.pack(side=tk.LEFT, padx=5)
        stop_button = ttk.Button(self.__control_frame, text="Stop Experiment", command=self.__on_stop_experiment)
        stop_button.pack(side=tk.LEFT, padx=5)

        self.__data_frame = ttk.LabelFrame(main_frame, text="Experiment Settings", padding=10)
        self.__data_frame.pack(fill=tk.BOTH, expand=True)
        self.__data_frame.columnconfigure(1, weight=1)

        keys_types = self.__itf.get_exp_settings_type()().get_types()
        for key, type_ in keys_types.items():
            self.__add_settings_row(key, type_, self.__data_frame)

        set_button = ttk.Button(self.__data_frame, text="Update Settings", command=self.__on_update_settings)
        set_button.grid(row=len(self.__settings_entries) + 1, column=0, columnspan=2, pady=5)

    def friendly_type_name(self, key):
        underscore_replaced = key.replace("_", " ")
        return underscore_replaced.capitalize()

    def __add_settings_row(self, key: str, type: type, root: ttk.Frame):
        label = ttk.Label(root, text=f"{self.friendly_type_name(key)}:")
        label.grid(row=len(self.__settings_entries), column=0, sticky=tk.EW, pady=2)
        entry = ttk.Entry(root)
        entry.grid(row=len(self.__settings_entries), column=1, sticky=tk.EW, pady=2)

        self.__settings_entries.append((key, entry, type))

    def __update_thread(self, stop_flag: daemon.StopFlag):
        while stop_flag.run():
            try:
                time.sleep(0.1) # small sleep to prevent busy waiting
                if self.__op_transop_handle is not None:
                    continue

                key, value_str = self.__update_queue.get(timeout=1)
                self.__do_update_setting(key, value_str)
            except Empty:
                pass
        

    def __updater(self):
        self.__update_values()
        self.root.after(500, self.__updater)

    def __update_values(self):
        if self.__op_event_handle is None and self.__op_transop_handle is None:
            if self.__itf.get_experiment() is not None:
                exp = self.__itf.get_experiment()
                state_str = {ExperimentController.RUN_STATE_PREINIT: "Preinitialization", ExperimentController.RUN_STATE_INIT: "Initialization", ExperimentController.RUN_STATE_RUNNING: "Running", ExperimentController.RUN_STATE_STOPPED: "Stopped"}.get(self.__itf.get_state(), "Unknown")
                self.__status_label.config(text=f"Current state: {state_str}")

                self.__status_style.configure("Status.TLabel", background={"Preinitialization": "yellow", "Initialization": "orange", "Running": "green", "Stopped": "red"}.get(state_str, "gray"))
            else:
                self.__status_label.config(text="No experiment running.")
                self.__status_style.configure("Status.TLabel", background="lightgray")
        else:
            self.__status_label.config(text=f"{self.__current_op}...")

        if self.__itf.get_experiment() is not None:
            exp = self.__itf.get_experiment().get_settings()
            exp_dict = exp.get_dict()

            for key, entry, type_ in self.__settings_entries:
                value = exp_dict.get(key)
                entry.delete(0, tk.END)
                time.sleep(0.01) # small delay to ensure GUI updates
                entry.insert(0, str(value))
                time.sleep(0.01) # small delay to ensure GUI updates

                entry.config(state=tk.DISABLED)

            self.__uuid_label.config(text=f"Run UUID: ...{str(self.__itf.get_experiment_uuid())[-8:]}")
        else:
            for key, entry, type_ in self.__settings_entries:
                entry.config(state=tk.NORMAL)

            self.__uuid_label.config(text="Run UUID: None")

        self.__update_gui_enabled()

        if self.__op_event_handle is not None:
            if not self.__op_event_handle.is_in_progress():
                result = self.__op_event_handle.get_result(self.__itf.get_controller_uuid())
                state = self.__op_event_handle.get_state(self.__itf.get_controller_uuid())

                if state != magics.EVENT_OK:
                    self.__alert(f"Operation '{self.__current_op}' failed: {result.decode('utf-8')}")

                self.__op_event_handle = None

        if self.__op_transop_handle is not None:
            if self.__op_transop_handle.get_state() != client.TRANSOP_STATE_PENDING:
                if self.__op_transop_handle.get_state() == client.TRANSOP_STATE_REJ:
                    result = self.__op_transop_handle.get_reason()
                    self.__alert(f"Operation '{self.__current_op}' failed: {result}")

                self.__op_transop_handle = None

    def __alert(self, message: str):
        alert_window = tk.Toplevel(self.root)
        alert_window.title("Alert")
        alert_label = ttk.Label(alert_window, text=message, padding=10)
        alert_label.pack()
        ok_button = ttk.Button(alert_window, text="OK", command=alert_window.destroy)
        ok_button.pack(pady=5)

    def __update_gui_enabled(self):
        should_enable_controls = self.__op_event_handle is None and self.__op_transop_handle is None
        should_enable_data = self.__itf.get_experiment() is None and should_enable_controls

        self.__set_controls_enabled(should_enable_controls)
        self.__set_data_controls_enabled(should_enable_data)

    def __set_controls_enabled(self, enabled: bool):
        state = tk.NORMAL if enabled else tk.DISABLED
        for child in self.__control_frame.winfo_children():
            child.config(state=state)

    def __set_data_controls_enabled(self, enabled: bool):
        state = tk.NORMAL if enabled else tk.DISABLED
        for child in self.__data_frame.winfo_children():
            child.config(state=state)

    def __on_update_settings(self):
        for key, entry, type_ in self.__settings_entries:
            value_str = entry.get()
            self.__update_queue.put((key, value_str))

    def __do_update_setting(self, key: str, value_str: str):
        print(f"Updating setting {key} to {value_str}")
        self.__op_transop_handle = self.__itf.set_kw(key, value_str, client.KVP_RET_HANDLE)
        self.__current_op = "Updating setting " + key

    def handle_window(self):
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def on_close(self):
        self.root.destroy()
        self.__daemon.stop()

    def __on_start_experiment(self):
        self.__op_event_handle = self.__itf.start_experiment()
        self.__current_op = "Starting"

    def __on_stop_experiment(self):
        self.__op_event_handle = self.__itf.stop_experiment("Stopped by user.")
        self.__current_op = "Stopping"

UUID_EXPOSURE_CONTROLLER = uuid.uuid3(uuid.NAMESPACE_OID, "Exposure Controller")
if __name__ == "__main__":
    itf = ExperimentInterface("exposure", UUID_EXPOSURE_CONTROLLER)
    
    root = tk.Tk()
    app = ExperimentControllerGUI(root, itf)
    root.mainloop()

    itf.close()