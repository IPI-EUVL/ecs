from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from typing import Any, Iterator

from ipi_ecs.logging.protocol import (
    encode_log_record,
    encode_event_begin,
    encode_event_end,
    encode_event_end_last,
)


class LogClient:
    def __init__(self, sock, *, origin_uuid: uuid.UUID | None = None):
        self._sock = sock
        self._origin_uuid = origin_uuid or uuid.uuid4()
        self._seq = 0

    @property
    def origin_uuid(self) -> str:
        # Always expose as a string UUID for log records.
        return str(self._origin_uuid)

    def log(
        self,
        msg: str,
        *,
        level: str = "INFO",
        l_type: str = "SW",
        event: str | None = None,
        origin_ts_ns: int | None = None,
        origin_uuid: uuid.UUID | None = None,
        **data: Any,
    ) -> None:
        """
        Send one structured record. All extra fields go into record["data"].

        Args:
            msg (str): Message to log
            level (str): Log level (can be arbitrary)
            l_type (str): Log type (can be arbitrary) such as: SOFTW, EXP, REC
            event (str): Optional event tag (legacy / free-form)
            origin_ts_ns (int): Origin timestamp override (ns)
            data (Any): Extra data to add to log. Intended usage is for subsystems to store event-related data
                to enable replay of experiment events.
        """

        self._seq += 1

        # The following is Schema v1
        # MODIFYING THESE KEYS WILL GIVE ME A HEADACHE. DO NOT TOUCH
        record: dict[str, Any] = {
            "v": 1,  # record schema version
            "origin": {
                "uuid": origin_uuid if origin_uuid is not None else str(self._origin_uuid),
                "ts_ns": origin_ts_ns if origin_ts_ns is not None else time.time_ns(),
            },
            "seq": self._seq,
            "level": level,
            "msg": msg,
            "l_type": l_type,
            "data": {
                **({"event": event} if event else {}),
                **data,
            },
        }

        self._sock.put(encode_log_record(record))

    # ----- Event markers (no line numbers on the client) -----

    def begin_event(
        self,
        e_type: str,
        message: str,
        *,
        level: str = "INFO",
        event_id: str | None = None,
        **data_start: Any,
    ) -> str:
        """
        Begin an event marker. The server will assign start_line/start_ts_ns.

        Returns:
            event_id (str): UUID string identifying this event.
        """
        eid = event_id or str(uuid.uuid4())
        payload = {
            "event_id": eid,
            "e_type": e_type,
            "level": level,
            "message": message,
            "data_start": data_start,
        }
        self._sock.put(encode_event_begin(payload))
        return eid

    def end_event(self, event_id: str, **data_end: Any) -> None:
        """End an event marker by id. The server will assign end_line/end_ts_ns."""
        payload = {
            "event_id": str(event_id),
            "data_end": data_end,
        }
        self._sock.put(encode_event_end(payload))

    def end_last_event(self, *, e_type: str | None = None, **data_end: Any) -> None:
        """
        Recovery helper: end the most recent OPEN event (optionally restricted by e_type).
        """
        payload = {
            **({"e_type": e_type} if e_type else {}),
            "data_end": data_end,
        }
        self._sock.put(encode_event_end_last(payload))

    @contextmanager
    def event(
        self,
        e_type: str,
        message: str,
        *,
        level: str = "INFO",
        event_id: str | None = None,
        **data_start: Any,
    ) -> Iterator[str]:
        """
        Convenience context manager:

            with client.event("RUN", "Expose sample 12", run_id=..., sample_id=...) as eid:
                ...

        Automatically ends the event on exit. If an exception is raised, it still ends the event and
        attaches exception info to data_end.
        """
        eid = self.begin_event(e_type, message, level=level, event_id=event_id, **data_start)
        try:
            yield eid
        except Exception as e:
            # Don't swallow; just annotate.
            self.end_event(eid, exception=str(e), exception_type=type(e).__name__)
            raise
        else:
            self.end_event(eid)
