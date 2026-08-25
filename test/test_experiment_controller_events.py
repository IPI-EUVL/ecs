from __future__ import annotations

import multiprocessing
import uuid

from ipi_ecs.db.db_library import Library
from ipi_ecs.subsystems.experiment_controller import ExperimentController, RunRecord, RunSettings, RunState
from ipi_ecs.subsystems.run_events import STREAM_END_KIND, STREAM_START_KIND, RunEventStream, load_run_event_timeline, run_event_stream_id


class _Logger:
    def begin_event(self, *_args, **_kwargs):
        return str(uuid.uuid4())

    def end_event(self, *_args, **_kwargs):
        pass

    def log(self, *_args, **_kwargs):
        pass


class _EventHandle:
    def __init__(self) -> None:
        self.result = None
        self.error = None

    def ret(self, value: bytes) -> None:
        self.result = value

    def fail(self, value: bytes) -> None:
        self.error = value


def _controller(library: Library) -> ExperimentController:
    controller = ExperimentController.__new__(ExperimentController)
    controller.name = "Test Controller"
    controller.uuid = uuid.uuid4()
    controller.exp_type = "exposure"
    controller._ExperimentController__logger = _Logger()
    controller._ExperimentController__library = library
    controller._ExperimentController__data_thread_enqueue = lambda fn, *args, **kwargs: fn(*args, **kwargs)
    controller._ExperimentController__pending_run_events = []
    controller._ExperimentController__pending_run_events_lock = multiprocessing.Lock()
    controller._ExperimentController__lifecycle_stream = None
    controller._ExperimentController__current_phase = ExperimentController.RUN_STATE_STOPPED
    controller._ExperimentController__expected_event_streams = {}
    controller._ExperimentController__start_run_handle = None
    controller._ExperimentController__can_start_event_handle = None
    controller._ExperimentController__preinit_handle = None
    controller._ExperimentController__init_handle = None
    controller._ExperimentController__stop_kv = None
    controller._ExperimentController__current_run = None
    controller._ExperimentController__run_record = None
    return controller


def _record(controller: ExperimentController, library: Library) -> RunRecord:
    settings = RunSettings()
    state = RunState(controller.exp_type, settings)
    return RunRecord.create(controller._ExperimentController__logger, library, state, settings, controller)


def test_lifecycle_events_are_ordered_and_terminal_stream_is_closed(tmp_path) -> None:
    library = Library(str(tmp_path))
    controller = _controller(library)
    record = _record(controller, library)
    controller._ExperimentController__current_run = record.get_state()
    controller._ExperimentController__run_record = record
    try:
        controller._ExperimentController__start_lifecycle_stream()
        for phase in (
            ExperimentController.RUN_STATE_CAN_START,
            ExperimentController.RUN_STATE_PREINIT,
            ExperimentController.RUN_STATE_INIT,
            ExperimentController.RUN_STATE_RUNNING,
            ExperimentController.RUN_STATE_STOPPING,
        ):
            controller._ExperimentController__set_run_phase(phase)
        controller._ExperimentController__set_run_phase(
            ExperimentController.RUN_STATE_STOPPED,
            outcome="STOPPED",
            reason="Operator request",
        )

        events = load_run_event_timeline(record.get_record()).events
    finally:
        library.close()

    assert [event.kind for event in events] == [
        STREAM_START_KIND,
        "lifecycle.phase",
        "lifecycle.phase",
        "lifecycle.phase",
        "lifecycle.phase",
        "lifecycle.phase",
        "lifecycle.phase",
        STREAM_END_KIND,
    ]
    assert [event.payload["phase"] for event in events if event.kind == "lifecycle.phase"] == [
        "CAN_START",
        "PREINIT",
        "INIT",
        "RUNNING",
        "STOPPING",
        "STOPPED",
    ]
    assert events[-2].payload == {
        "phase": "STOPPED",
        "phase_code": ExperimentController.RUN_STATE_STOPPED,
        "outcome": "STOPPED",
        "reason": "Operator request",
    }
    assert events[-1].payload == {"outcome": "STOPPED", "reason": "Operator request"}
    assert load_run_event_timeline(record.get_record()).complete is True


def test_abort_records_stopped_outcome_without_a_stopping_phase(tmp_path) -> None:
    library = Library(str(tmp_path))
    controller = _controller(library)
    record = _record(controller, library)
    controller._ExperimentController__current_run = record.get_state()
    controller._ExperimentController__run_record = record

    class _StopProvider:
        def __init__(self) -> None:
            self.calls = []

        def call(self, payload: bytes, *, target: list[object]) -> None:
            self.calls.append((payload, target))

    stop_provider = _StopProvider()
    controller._ExperimentController__stop_provider = stop_provider
    try:
        controller._ExperimentController__start_lifecycle_stream()
        controller._ExperimentController__set_run_phase(ExperimentController.RUN_STATE_CAN_START)
        controller._ExperimentController__abort_run("Laser interlock")
        timeline = load_run_event_timeline(record.get_record())
    finally:
        library.close()

    phases = [event.payload["phase"] for event in timeline.events if event.kind == "lifecycle.phase"]
    assert phases == ["CAN_START", "STOPPED"]
    assert timeline.events[-2].payload["outcome"] == "ABORTED"
    assert timeline.events[-2].payload["reason"] == "Laser interlock"
    assert timeline.events[-1].kind == STREAM_END_KIND
    assert timeline.events[-1].payload["outcome"] == "ABORTED"
    assert len(stop_provider.calls) == 1


def test_external_events_validate_sender_and_persist_after_finalization(tmp_path) -> None:
    library = Library(str(tmp_path))
    controller = _controller(library)
    record = _record(controller, library)
    submitter_id = uuid.uuid4()
    producer_id = uuid.uuid4()
    stream_name = "acquisition.timing"
    controller.add_expected_run_event_stream(submitter_id, stream_name, producer_uuid=producer_id)
    stream = RunEventStream(
        record.get_state().get_uuid(),
        run_event_stream_id(record.get_state().get_uuid(), submitter_id, stream_name),
        stream_name,
        producer_id,
        submitter_id,
    )
    start = stream.event(STREAM_START_KIND, {"source": "timing"})
    try:
        rejected = _EventHandle()
        controller._ExperimentController__on_append_run_event(uuid.uuid4(), start.encode(), rejected)
        assert rejected.result is None
        assert rejected.error is not None
        assert b"submitter" in rejected.error

        wrong_producer = RunEventStream(
            record.get_state().get_uuid(),
            run_event_stream_id(record.get_state().get_uuid(), submitter_id, stream_name),
            stream_name,
            uuid.uuid4(),
            submitter_id,
        ).event(STREAM_START_KIND, {"source": "timing"})
        producer_rejected = _EventHandle()
        controller._ExperimentController__on_append_run_event(submitter_id, wrong_producer.encode(), producer_rejected)
        assert producer_rejected.result is None
        assert producer_rejected.error is not None
        assert b"producer" in producer_rejected.error

        record.write_end(record.get_state(), "STOPPED", "Completed")
        delivered = _EventHandle()
        controller._ExperimentController__on_append_run_event(submitter_id, start.encode(), delivered)
        assert delivered.result == start.event_id.bytes
        assert delivered.error is None

        retried = _EventHandle()
        controller._ExperimentController__on_append_run_event(submitter_id, start.encode(), retried)
        assert retried.result == start.event_id.bytes
        assert retried.error is None

        end = stream.event(STREAM_END_KIND, {"outcome": "STOPPED"})
        gapped = type(end).from_dict(end.to_dict() | {"event_id": str(uuid.uuid4()), "sequence": 3})
        gap_rejected = _EventHandle()
        controller._ExperimentController__on_append_run_event(submitter_id, gapped.encode(), gap_rejected)
        assert gap_rejected.result is None
        assert gap_rejected.error is not None
        assert b"expected sequence 1" in gap_rejected.error

        completed = _EventHandle()
        controller._ExperimentController__on_append_run_event(submitter_id, end.encode(), completed)
        assert completed.result == end.event_id.bytes
        assert completed.error is None

        timeline = load_run_event_timeline(record.get_record())
    finally:
        library.close()

    assert [event.event_id for event in timeline.events] == [start.event_id, end.event_id]
    assert [event.payload for event in timeline.events] == [start.payload, end.payload]
    assert timeline.events[0].ingest_unix_ns >= start.ingest_unix_ns
    assert timeline.complete is True