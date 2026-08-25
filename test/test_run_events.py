from __future__ import annotations

import uuid

import pytest

from ipi_ecs.db.db_library import Library
from ipi_ecs.subsystems.run_events import (
    RUN_EVENT_RESOURCE,
    RUN_EVENT_RESOURCE_TYPE,
    STREAM_END_KIND,
    STREAM_START_KIND,
    RunEvent,
    RunEventEmitter,
    RunEventStream,
    append_run_event,
    decode_run_event_journal,
    load_run_event_timeline,
    run_event_stream_id,
)


def _stream(run_id: uuid.UUID | None = None) -> RunEventStream:
    return RunEventStream(
        run_id or uuid.uuid4(),
        uuid.uuid4(),
        "controller.lifecycle",
        uuid.uuid4(),
    )


def test_run_event_codec_is_strict_and_round_trips() -> None:
    stream = _stream()
    event = stream.event(
        "lifecycle.phase",
        {"phase": "PREINIT"},
        producer_unix_ns=10,
        producer_monotonic_ns=20,
        ingest_unix_ns=30,
        runtime_seconds=0.0,
    )

    assert RunEvent.decode(event.encode()) == event
    value = event.to_dict()
    value["unknown"] = True
    with pytest.raises(ValueError, match="unknown or missing"):
        RunEvent.from_dict(value)


def test_stream_identity_is_stable_per_run_submitter_and_name() -> None:
    run_id = uuid.uuid4()
    submitter_id = uuid.uuid4()

    first = run_event_stream_id(run_id, submitter_id, "acquisition.timing")

    assert first == run_event_stream_id(run_id, submitter_id, "acquisition.timing")
    assert first != run_event_stream_id(uuid.uuid4(), submitter_id, "acquisition.timing")


def test_journal_append_is_durable_ordered_and_idempotent(tmp_path) -> None:
    library = Library(str(tmp_path))
    entry = library.create_entry("Run", "Fixture")
    stream = _stream()
    start = stream.event(STREAM_START_KIND, {"source": "controller"})
    phase = stream.event("lifecycle.phase", {"phase": "CAN_START"})
    end = stream.event(STREAM_END_KIND, {"outcome": "ABORTED"})
    try:
        assert append_run_event(entry, start) is True
        assert append_run_event(entry, phase) is True
        retried = RunEvent.from_dict(phase.to_dict() | {"ingest_unix_ns": phase.ingest_unix_ns + 1})
        assert append_run_event(entry, retried) is False
        assert append_run_event(entry, end) is True

        timeline = load_run_event_timeline(entry)
        assert timeline.events == (start, phase, end)
        assert timeline.complete is True
        assert timeline.issues == ()
        assert dict(entry.list_resources())[RUN_EVENT_RESOURCE] == RUN_EVENT_RESOURCE_TYPE
    finally:
        library.close()


def test_journal_rejects_sequence_conflicts_and_gaps(tmp_path) -> None:
    library = Library(str(tmp_path))
    entry = library.create_entry("Run", "Fixture")
    stream = _stream()
    start = stream.event(STREAM_START_KIND)
    try:
        append_run_event(entry, start)
        conflict = RunEvent.from_dict(start.to_dict() | {"event_id": str(uuid.uuid4())})
        with pytest.raises(ValueError, match="sequence conflicts"):
            append_run_event(entry, conflict)
        stream.next_sequence = 3
        with pytest.raises(ValueError, match="expected sequence 1"):
            append_run_event(entry, stream.event("lifecycle.phase", {"phase": "RUNNING"}))
    finally:
        library.close()


def test_reader_reports_truncated_tail_and_open_stream() -> None:
    stream = _stream()
    start = stream.event(STREAM_START_KIND)
    complete = decode_run_event_journal(start.encode() + b"\n" + b'{"partial"')

    assert complete.events == (start,)
    assert complete.complete is False
    assert {issue.code for issue in complete.issues} == {"truncated_tail", "stream_end"}


def test_missing_journal_is_a_valid_historical_empty_timeline(tmp_path) -> None:
    library = Library(str(tmp_path))
    entry = library.create_entry("Historical run", "Fixture")
    try:
        assert load_run_event_timeline(entry).events == ()
        assert load_run_event_timeline(entry).complete is True
    finally:
        library.close()


def test_emitter_retries_then_delivers_in_source_order() -> None:
    controller_id = uuid.uuid4()
    stream = _stream()
    first = stream.event(STREAM_START_KIND)
    second = stream.event(STREAM_END_KIND)
    calls = []

    class _Handle:
        def is_in_progress(self):
            return False

        def get_state(self, _controller_id):
            from ipi_ecs.dds.client import EVENT_OK

            return EVENT_OK

        def get_result(self, _controller_id):
            return first.event_id.bytes

    class _Provider:
        def call(self, payload, _targets):
            event = RunEvent.decode(payload)
            calls.append(event.sequence)
            if len(calls) == 1:
                return None
            return _Handle()

    emitter = RunEventEmitter(
        controller_id,
        immediate_retry_seconds=0.01,
        retry_interval_seconds=0.001,
        background_retry_seconds=0.01,
    )
    emitter.set_provider(_Provider())
    try:
        emitter.emit(first)
        emitter.emit(second)
        assert emitter.flush(1.0)
        assert calls == [0, 0, 1]
        assert emitter.pending_count == 0
        assert emitter.last_error is None
    finally:
        assert emitter.close()