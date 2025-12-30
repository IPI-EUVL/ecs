from __future__ import annotations

import time
import uuid

from typing import Any

from ipi_ecs.logging.protocol import encode_log_record


class LogClient:
    def __init__(self, sock, *, origin_uuid: uuid.UUID | None = None):
        self._sock = sock
        self._origin_uuid = origin_uuid or uuid.uuid4()
        self._seq = 0

    @property
    def origin_uuid(self) -> str:
        return self._origin_uuid
    
    def log(self, msg: str, *, level: str = "INFO", l_type: str = "SW", event: str | None = None, origin_ts_ns: int | None = None, **data: Any,) -> None:
        """
        Send one structured record. All extra fields go into record["data"].

        Args:
            msg (str): Message to log
            level (str): Log level (can be arbitrary)
            l_type (str): Log type (can be arbitrary) such as: SOFTW, EXP. Used to distinguish debugging-related messages from experiment-related messages
            event (str): Event type (can be arbitrary)
            origin_ts_ns (int): Origin timestamp (if desired)
            data (Any): Extra data to add to log. Intended usage is for subsystems to store event-related data to enable replay of experiment events.
        """

        self._seq += 1

        # The following is Schema v1
        # MODIFYING THESE KEYS WILL GIVE ME A HEADACHE. DO NOT TOUCH
        record: dict[str, Any] = {
            "v": 1,  # record schema version
            "origin": {
                "uuid": str(self._origin_uuid),
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
