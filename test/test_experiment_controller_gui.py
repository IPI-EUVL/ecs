import json
import threading
import tkinter as tk

import segment_bytes
import pytest

from ipi_ecs.dds import magics
from ipi_ecs.gui.experiment_controller_gui import ExperimentControllerGUI, ExperimentInterface
from ipi_ecs.subsystems.experiment_controller import ExperimentController, decode_prepare_run_tags


class _Widget:
    def __init__(self):
        self.options = {}

    def config(self, **kwargs):
        self.options.update(kwargs)


class _Style:
    def __init__(self):
        self.options = {}

    def configure(self, name, **kwargs):
        self.options[name] = kwargs


class _Root:
    def __init__(self):
        self.after_calls = []

    def after(self, delay, callback):
        self.after_calls.append((delay, callback))


class _Settings:
    def get_dict(self):
        return {}


class _Run:
    def get_settings(self):
        return _Settings()

    def get_uuid(self):
        return "00000000-0000-0000-0000-000000001234"


class _RacingInterface:
    def __init__(self, reasons):
        self._run = _Run()
        self._reasons = reasons
        self.get_experiment_calls = 0

    def get_status_snapshot(self):
        return ExperimentController.RUN_STATE_STOPPING, self._run, self._reasons

    def get_experiment(self):
        self.get_experiment_calls += 1
        if self.get_experiment_calls < 4:
            return self._run
        return None

    def get_state(self):
        return ExperimentController.RUN_STATE_STOPPING

    def get_experiment_reasons(self):
        return self._reasons

    def get_experiment_uuid(self):
        return "00000000-0000-0000-0000-000000001234"

    def get_stopped_run(self):
        return None


class _ControlInterface:
    def get_automation_owner(self):
        return None

    def get_experiment(self):
        return None


class _EventSender:
    def __init__(self) -> None:
        self.calls = []

    def call(self, payload, targets):
        self.calls.append((payload, targets))
        return "handle"


def _gui(interface):
    gui = ExperimentControllerGUI.__new__(ExperimentControllerGUI)
    gui.root = _Root()
    gui._ExperimentControllerGUI__itf = interface
    gui._ExperimentControllerGUI__op_event_handle = None
    gui._ExperimentControllerGUI__op_transop_handle = None
    gui._ExperimentControllerGUI__current_op = None
    gui._ExperimentControllerGUI__status_label = _Widget()
    gui._ExperimentControllerGUI__uuid_label = _Widget()
    gui._ExperimentControllerGUI__reasons_label = _Widget()
    gui._ExperimentControllerGUI__status_style = _Style()
    gui._ExperimentControllerGUI__update_settings_progress_dialog = lambda: None
    gui._ExperimentControllerGUI__update_gui_enabled = lambda: None
    return gui


def test_update_values_uses_one_status_snapshot_during_stop_transition():
    reasons = segment_bytes.encode(
        [segment_bytes.encode([b"Laser", b"Ongoing", magics.OP_IN_PROGRESS + b": Warming"])]
    )
    interface = _RacingInterface(reasons)
    gui = _gui(interface)

    gui._ExperimentControllerGUI__update_values()

    assert gui._ExperimentControllerGUI__status_label.options["text"] == "Current state: Stopping"
    assert gui._ExperimentControllerGUI__reasons_label.options["text"] == "Laser: Ongoing - Ongoing: Warming"
    assert interface.get_experiment_calls == 0


def test_updater_rearms_tk_callback_when_update_raises():
    gui = ExperimentControllerGUI.__new__(ExperimentControllerGUI)
    gui.root = _Root()
    gui._ExperimentControllerGUI__update_values = lambda: (_ for _ in ()).throw(RuntimeError("boom"))

    with pytest.raises(RuntimeError, match="boom"):
        gui._ExperimentControllerGUI__updater()

    assert len(gui.root.after_calls) == 1
    assert gui.root.after_calls[0][0] == 50


@pytest.mark.parametrize("owner", (None, "", "None"))
def test_automation_lease_without_owner_does_not_lock_the_interface(owner):
    interface = ExperimentInterface.__new__(ExperimentInterface)
    interface._ExperimentInterface__status_lock = threading.Lock()
    interface._ExperimentInterface__automation_owner_name = "Previous owner"

    interface._ExperimentInterface__on_automation_lease_update(
        json.dumps({"owner_name": owner}).encode("utf-8")
    )

    assert interface.get_automation_owner() is None


def test_automation_lease_with_owner_preserves_the_owner_name():
    interface = ExperimentInterface.__new__(ExperimentInterface)
    interface._ExperimentInterface__status_lock = threading.Lock()
    interface._ExperimentInterface__automation_owner_name = None

    interface._ExperimentInterface__on_automation_lease_update(b'{"owner_name":"Batch Controller"}')

    assert interface.get_automation_owner() == "Batch Controller"


def test_stop_remains_enabled_during_a_settings_update() -> None:
    gui = ExperimentControllerGUI.__new__(ExperimentControllerGUI)
    gui._ExperimentControllerGUI__itf = _ControlInterface()
    gui._ExperimentControllerGUI__op_event_handle = None
    gui._ExperimentControllerGUI__op_transop_handle = object()
    gui._ExperimentControllerGUI__settings_update_active = True
    gui._ExperimentControllerGUI__current_op = "Updating setting Calibration"
    gui._ExperimentControllerGUI__start_button = _Widget()
    gui._ExperimentControllerGUI__stop_button = _Widget()
    gui._ExperimentControllerGUI__automation_status_label = _Widget()
    gui._ExperimentControllerGUI__set_data_controls_enabled = lambda _enabled: None

    gui._ExperimentControllerGUI__update_gui_enabled()

    assert gui._ExperimentControllerGUI__start_button.options["state"] == tk.DISABLED
    assert gui._ExperimentControllerGUI__stop_button.options["state"] == tk.NORMAL


def test_interface_uses_tagged_prepare_for_scalar_run_tags() -> None:
    interface = ExperimentInterface.__new__(ExperimentInterface)
    plain = _EventSender()
    tagged = _EventSender()
    interface._ExperimentInterface__start_experiment_event_sender = plain
    interface._ExperimentInterface__start_tagged_experiment_event_sender = tagged
    interface._ExperimentInterface__run_tags = {}

    interface.set_run_tags({"source_calibrations": '{"schema_version":1,"bindings":[]}'})
    result = interface.start_experiment()

    assert result == "handle"
    assert plain.calls == []
    assert decode_prepare_run_tags(tagged.calls[0][0]) == {
        "source_calibrations": '{"schema_version":1,"bindings":[]}'
    }
