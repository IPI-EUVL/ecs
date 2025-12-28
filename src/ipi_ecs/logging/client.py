from __future__ import annotations

import time
import uuid

from typing import Any

from ipi_ecs.logging.protocol import encode_log_record


class LogClient:
    def __init__(self, sock, *, origin_uuid: str | None = None):
        self._sock = sock
        self._origin_uuid = origin_uuid or str(uuid.uuid4())
        self._seq = 0

    @property
    def origin_uuid(self) -> str:
        return self._origin_uuid

    def log(
        self,
        msg: str,
        *,
        level: str = "INFO",  # free-form string (your preference)
        origin_ts_ns: int | None = None,
        subsystem: str | None = None,
        event: str | None = None,
        **data: Any,
    ) -> None:
        """
        Send one structured record. All extra fields go into record["data"].
        """
        self._seq += 1

        record: dict[str, Any] = {
            "v": 1,  # record schema version
            "origin": {
                "uuid": self._origin_uuid,
                "ts_ns": origin_ts_ns if origin_ts_ns is not None else time.time_ns(),
            },
            "seq": self._seq,
            "level": level,
            "msg": msg,
            "data": {
                **({"subsystem": subsystem} if subsystem else {}),
                **({"event": event} if event else {}),
                **data,
            },
        }

        self._sock.put(encode_log_record(record))
