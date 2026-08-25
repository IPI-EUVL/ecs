from __future__ import annotations

import json
import math
import os
import queue
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any


RUN_EVENT_SCHEMA_VERSION = 1
RUN_EVENT_RESOURCE = "run_events.ndjson"
RUN_EVENT_RESOURCE_TYPE = "run_event_journal"
STREAM_START_KIND = "stream.start"
STREAM_END_KIND = "stream.end"
STREAM_EXPECTED_KIND = "stream.expected"
RUN_EVENT_STREAM_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "ipi-ecs/run-event-stream")


def run_event_stream_id(run_id: uuid.UUID, submitter_id: uuid.UUID, stream_name: str) -> uuid.UUID:
    if not isinstance(run_id, uuid.UUID) or not isinstance(submitter_id, uuid.UUID):
        raise ValueError("Run event stream identity requires UUID run and submitter IDs.")
    normalized_name = _required_text("stream_name", stream_name)
    return uuid.uuid5(RUN_EVENT_STREAM_NAMESPACE, f"{run_id}:{submitter_id}:{normalized_name}")


def _required_text(name: str, value: object, *, maximum: int = 128) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > maximum:
        raise ValueError(f"{name} must be non-empty trimmed text of at most {maximum} characters.")
    return value


def _non_negative_integer(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer.")
    return value


@dataclass(frozen=True)
class RunEvent:
    event_id: uuid.UUID
    run_id: uuid.UUID
    stream_id: uuid.UUID
    stream_name: str
    sequence: int
    producer_id: uuid.UUID
    submitter_id: uuid.UUID
    kind: str
    producer_unix_ns: int
    producer_monotonic_ns: int | None
    ingest_unix_ns: int
    payload: dict[str, Any]
    capture_session_id: uuid.UUID | None = None
    next_sequence: int | None = None
    runtime_seconds: float | None = None

    def __post_init__(self) -> None:
        for name in ("event_id", "run_id", "stream_id", "producer_id", "submitter_id"):
            if not isinstance(getattr(self, name), uuid.UUID):
                raise ValueError(f"{name} must be a UUID.")
        _required_text("stream_name", self.stream_name)
        _required_text("kind", self.kind)
        _non_negative_integer("sequence", self.sequence)
        _non_negative_integer("producer_unix_ns", self.producer_unix_ns)
        _non_negative_integer("ingest_unix_ns", self.ingest_unix_ns)
        if self.producer_monotonic_ns is not None:
            _non_negative_integer("producer_monotonic_ns", self.producer_monotonic_ns)
        if not isinstance(self.payload, dict):
            raise ValueError("payload must be an object.")
        try:
            json.dumps(self.payload, allow_nan=False, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            raise ValueError("payload must contain finite JSON values.") from exc
        if self.capture_session_id is not None and not isinstance(self.capture_session_id, uuid.UUID):
            raise ValueError("capture_session_id must be a UUID when present.")
        if self.next_sequence is not None:
            _non_negative_integer("next_sequence", self.next_sequence)
        if self.runtime_seconds is not None:
            if (
                isinstance(self.runtime_seconds, bool)
                or not isinstance(self.runtime_seconds, (int, float))
                or not math.isfinite(float(self.runtime_seconds))
                or float(self.runtime_seconds) < 0
            ):
                raise ValueError("runtime_seconds must be finite and non-negative when present.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": RUN_EVENT_SCHEMA_VERSION,
            "event_id": str(self.event_id),
            "run_id": str(self.run_id),
            "stream_id": str(self.stream_id),
            "stream_name": self.stream_name,
            "sequence": self.sequence,
            "producer_id": str(self.producer_id),
            "submitter_id": str(self.submitter_id),
            "kind": self.kind,
            "producer_unix_ns": self.producer_unix_ns,
            "producer_monotonic_ns": self.producer_monotonic_ns,
            "ingest_unix_ns": self.ingest_unix_ns,
            "payload": self.payload,
            "capture_session_id": None if self.capture_session_id is None else str(self.capture_session_id),
            "next_sequence": self.next_sequence,
            "runtime_seconds": self.runtime_seconds,
        }

    def encode(self) -> bytes:
        return json.dumps(self.to_dict(), allow_nan=False, separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_dict(cls, value: object) -> "RunEvent":
        expected = {
            "schema_version",
            "event_id",
            "run_id",
            "stream_id",
            "stream_name",
            "sequence",
            "producer_id",
            "submitter_id",
            "kind",
            "producer_unix_ns",
            "producer_monotonic_ns",
            "ingest_unix_ns",
            "payload",
            "capture_session_id",
            "next_sequence",
            "runtime_seconds",
        }
        if not isinstance(value, dict) or set(value) != expected:
            raise ValueError("Run event contains unknown or missing fields.")
        if value["schema_version"] != RUN_EVENT_SCHEMA_VERSION:
            raise ValueError("Unsupported run event schema version.")
        return cls(
            event_id=uuid.UUID(str(value["event_id"])),
            run_id=uuid.UUID(str(value["run_id"])),
            stream_id=uuid.UUID(str(value["stream_id"])),
            stream_name=value["stream_name"],
            sequence=value["sequence"],
            producer_id=uuid.UUID(str(value["producer_id"])),
            submitter_id=uuid.UUID(str(value["submitter_id"])),
            kind=value["kind"],
            producer_unix_ns=value["producer_unix_ns"],
            producer_monotonic_ns=value["producer_monotonic_ns"],
            ingest_unix_ns=value["ingest_unix_ns"],
            payload=value["payload"],
            capture_session_id=(
                None
                if value["capture_session_id"] is None
                else uuid.UUID(str(value["capture_session_id"]))
            ),
            next_sequence=value["next_sequence"],
            runtime_seconds=value["runtime_seconds"],
        )

    @classmethod
    def decode(cls, payload: bytes) -> "RunEvent":
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Run event must be valid UTF-8 JSON.") from exc
        return cls.from_dict(value)


@dataclass(frozen=True)
class RunEventIssue:
    code: str
    message: str
    stream_id: uuid.UUID | None = None


@dataclass(frozen=True)
class RunEventTimeline:
    events: tuple[RunEvent, ...]
    complete: bool
    issues: tuple[RunEventIssue, ...]


class RunEventStream:
    def __init__(
        self,
        run_id: uuid.UUID,
        stream_id: uuid.UUID,
        stream_name: str,
        producer_id: uuid.UUID,
        submitter_id: uuid.UUID | None = None,
        *,
        next_sequence: int = 0,
    ) -> None:
        self.run_id = run_id
        self.stream_id = stream_id
        self.stream_name = _required_text("stream_name", stream_name)
        self.producer_id = producer_id
        self.submitter_id = producer_id if submitter_id is None else submitter_id
        self.next_sequence = _non_negative_integer("next_sequence", next_sequence)

    def event(
        self,
        kind: str,
        payload: dict[str, Any] | None = None,
        *,
        producer_unix_ns: int | None = None,
        producer_monotonic_ns: int | None = None,
        ingest_unix_ns: int | None = None,
        capture_session_id: uuid.UUID | None = None,
        next_sequence: int | None = None,
        runtime_seconds: float | None = None,
    ) -> RunEvent:
        event = RunEvent(
            event_id=uuid.uuid4(),
            run_id=self.run_id,
            stream_id=self.stream_id,
            stream_name=self.stream_name,
            sequence=self.next_sequence,
            producer_id=self.producer_id,
            submitter_id=self.submitter_id,
            kind=kind,
            producer_unix_ns=time.time_ns() if producer_unix_ns is None else producer_unix_ns,
            producer_monotonic_ns=producer_monotonic_ns,
            ingest_unix_ns=time.time_ns() if ingest_unix_ns is None else ingest_unix_ns,
            payload={} if payload is None else payload,
            capture_session_id=capture_session_id,
            next_sequence=next_sequence,
            runtime_seconds=runtime_seconds,
        )
        self.next_sequence += 1
        return event


class RunEventEmitter:
    """Deliver run events in source order without coupling producers to record I/O."""

    def __init__(
        self,
        controller_uuid: uuid.UUID,
        *,
        immediate_retry_seconds: float = 5.0,
        retry_interval_seconds: float = 0.1,
        background_retry_seconds: float = 1.0,
    ) -> None:
        if not isinstance(controller_uuid, uuid.UUID):
            raise ValueError("Run event controller UUID must be a UUID.")
        if immediate_retry_seconds <= 0 or retry_interval_seconds <= 0 or background_retry_seconds <= 0:
            raise ValueError("Run event retry intervals must be positive.")
        self.controller_uuid = controller_uuid
        self.immediate_retry_seconds = immediate_retry_seconds
        self.retry_interval_seconds = retry_interval_seconds
        self.background_retry_seconds = background_retry_seconds
        self._provider = None
        self._provider_lock = threading.Lock()
        self._queue: queue.Queue[RunEvent] = queue.Queue()
        self._pending_lock = threading.Lock()
        self._pending_count = 0
        self._last_error: str | None = None
        self._stop = threading.Event()
        self._idle = threading.Event()
        self._idle.set()
        self._thread = threading.Thread(target=self._worker, name="run-event-emitter", daemon=True)
        self._thread.start()

    @property
    def pending_count(self) -> int:
        with self._pending_lock:
            return self._pending_count

    @property
    def last_error(self) -> str | None:
        with self._pending_lock:
            return self._last_error

    def set_provider(self, provider) -> None:
        with self._provider_lock:
            self._provider = provider

    def emit(self, event: RunEvent) -> None:
        if self._stop.is_set():
            raise RuntimeError("Run event emitter is closed.")
        with self._pending_lock:
            self._pending_count += 1
            self._idle.clear()
        self._queue.put(event)

    def flush(self, timeout: float) -> bool:
        if timeout < 0:
            raise ValueError("Run event flush timeout must be non-negative.")
        return self._idle.wait(timeout)

    def close(self, timeout: float = 5.0) -> bool:
        self._stop.set()
        self._thread.join(timeout)
        return not self._thread.is_alive()

    def _provider_snapshot(self):
        with self._provider_lock:
            return self._provider

    def _deliver_once(self, event: RunEvent) -> bool:
        from ipi_ecs.dds.client import EVENT_OK

        provider = self._provider_snapshot()
        if provider is None:
            raise RuntimeError("Run event provider is unavailable.")
        handle = provider.call(event.encode(), [self.controller_uuid])
        if handle is None:
            raise RuntimeError("Run event request could not be sent.")
        deadline = time.monotonic() + self.immediate_retry_seconds
        while handle.is_in_progress() and not self._stop.is_set() and time.monotonic() < deadline:
            time.sleep(self.retry_interval_seconds)
        if handle.is_in_progress():
            raise TimeoutError("Run event acknowledgement timed out.")
        state = handle.get_state(self.controller_uuid)
        if state != EVENT_OK:
            result = handle.get_result(self.controller_uuid)
            if isinstance(result, bytes):
                detail = result.decode("utf-8", errors="replace")
            else:
                detail = str(result or "event rejected")
            raise RuntimeError(detail)
        return True

    def _mark_delivered(self) -> None:
        with self._pending_lock:
            self._pending_count -= 1
            if self._pending_count == 0:
                self._idle.set()
            self._last_error = None

    def _mark_error(self, exc: Exception) -> None:
        with self._pending_lock:
            self._last_error = f"{type(exc).__name__}: {exc}"

    def _worker(self) -> None:
        current: RunEvent | None = None
        next_attempt_at = 0.0
        immediate_deadline = 0.0
        while not self._stop.is_set():
            if current is None:
                try:
                    current = self._queue.get(timeout=0.1)
                    next_attempt_at = 0.0
                    immediate_deadline = time.monotonic() + self.immediate_retry_seconds
                except queue.Empty:
                    continue
            remaining = next_attempt_at - time.monotonic()
            if remaining > 0:
                self._stop.wait(min(remaining, 0.1))
                continue
            try:
                self._deliver_once(current)
            except Exception as exc:
                self._mark_error(exc)
                delay = (
                    self.retry_interval_seconds
                    if time.monotonic() < immediate_deadline
                    else self.background_retry_seconds
                )
                next_attempt_at = time.monotonic() + delay
                continue
            self._mark_delivered()
            current = None


def decode_run_event_journal(payload: bytes) -> RunEventTimeline:
    issues: list[RunEventIssue] = []
    complete = True
    valid_payload = payload
    if payload and not payload.endswith(b"\n"):
        complete = False
        issues.append(RunEventIssue("truncated_tail", "Run event journal ends with an incomplete record."))
        last_newline = payload.rfind(b"\n")
        valid_payload = b"" if last_newline < 0 else payload[: last_newline + 1]

    events: list[RunEvent] = []
    for line_number, line in enumerate(valid_payload.splitlines(), start=1):
        if not line:
            raise ValueError(f"Run event journal contains an empty record on line {line_number}.")
        try:
            event = RunEvent.decode(line)
        except ValueError as exc:
            raise ValueError(f"Invalid run event journal line {line_number}: {exc}") from exc
        events.append(event)

    event_ids: dict[uuid.UUID, RunEvent] = {}
    stream_events: dict[uuid.UUID, list[RunEvent]] = {}
    expected_streams: set[uuid.UUID] = set()
    for event in events:
        existing = event_ids.get(event.event_id)
        if existing is not None:
            if existing != event:
                raise ValueError(f"Run event ID {event.event_id} has conflicting records.")
            continue
        event_ids[event.event_id] = event
        stream_events.setdefault(event.stream_id, []).append(event)
        if event.kind == STREAM_EXPECTED_KIND:
            try:
                expected_streams.add(uuid.UUID(str(event.payload["stream_id"])))
            except (KeyError, TypeError, ValueError, AttributeError) as exc:
                raise ValueError("Expected-stream event has an invalid stream_id payload.") from exc

    for stream_id, items in stream_events.items():
        sequences = [event.sequence for event in items]
        expected_sequences = list(range(len(items)))
        if sequences != expected_sequences:
            complete = False
            issues.append(
                RunEventIssue(
                    "sequence_gap",
                    f"Run event stream {stream_id} has sequences {sequences}, expected {expected_sequences}.",
                    stream_id,
                )
            )
        starts = sum(event.kind == STREAM_START_KIND for event in items)
        ends = sum(event.kind == STREAM_END_KIND for event in items)
        if starts != 1:
            complete = False
            issues.append(RunEventIssue("stream_start", f"Run event stream {stream_id} has {starts} start records.", stream_id))
        if ends != 1:
            complete = False
            issues.append(RunEventIssue("stream_end", f"Run event stream {stream_id} has {ends} end records.", stream_id))

    for stream_id in expected_streams - stream_events.keys():
        complete = False
        issues.append(RunEventIssue("missing_stream", f"Expected run event stream {stream_id} is missing.", stream_id))

    deduplicated = tuple(event_ids.values())
    return RunEventTimeline(deduplicated, complete, tuple(issues))


def load_run_event_timeline(entry) -> RunEventTimeline:
    resources = dict(entry.list_resources())
    if RUN_EVENT_RESOURCE not in resources:
        return RunEventTimeline((), True, ())
    if resources[RUN_EVENT_RESOURCE] != RUN_EVENT_RESOURCE_TYPE:
        raise ValueError("Run event journal resource has an unexpected type.")
    with entry.resource(RUN_EVENT_RESOURCE, RUN_EVENT_RESOURCE_TYPE, "rb") as resource:
        return decode_run_event_journal(resource.read())


def append_run_event(entry, event: RunEvent) -> bool:
    timeline = load_run_event_timeline(entry)
    if any(issue.code == "truncated_tail" for issue in timeline.issues):
        raise ValueError("Cannot append after an incomplete run event journal record.")
    if timeline.events and any(item.run_id != event.run_id for item in timeline.events):
        raise ValueError("Run event journal contains another run ID.")
    for existing in timeline.events:
        if existing.event_id == event.event_id:
            if existing != event and existing != RunEvent.from_dict(event.to_dict() | {"ingest_unix_ns": existing.ingest_unix_ns}):
                raise ValueError("Run event ID conflicts with its persisted record.")
            return False
        if existing.stream_id == event.stream_id and existing.sequence == event.sequence:
            raise ValueError("Run event stream sequence conflicts with its persisted record.")
    stream_sequences = [item.sequence for item in timeline.events if item.stream_id == event.stream_id]
    expected_sequence = 0 if not stream_sequences else max(stream_sequences) + 1
    if event.sequence != expected_sequence:
        raise ValueError(
            f"Run event stream {event.stream_id} expected sequence {expected_sequence}, received {event.sequence}."
        )
    with entry.resource(RUN_EVENT_RESOURCE, RUN_EVENT_RESOURCE_TYPE, "ab") as resource:
        resource.write(event.encode() + b"\n")
        resource.flush()
        os.fsync(resource.fileno())
    return True