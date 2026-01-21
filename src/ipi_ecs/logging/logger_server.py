from __future__ import annotations

import base64
import queue
import time
from pathlib import Path
import traceback
from typing import Any

from ipi_ecs.core.daemon import StopFlag

from ipi_ecs.core import tcp  # your wrapper should be here
from ipi_ecs.logging.journal import JournalWriter, resolve_log_dir
from ipi_ecs.logging.protocol import (
    TYPE_LOG,
    TYPE_EVT_BEGIN,
    TYPE_EVT_END,
    TYPE_EVT_END_LAST,
    PROTO_V1,
    ProtocolError,
    decode_message,
    decode_log_record,
    decode_json_payload,
)

ECS_LOG_PORT = 11751
ENV_LOG_DIR = "IPI_ECS_LOG_DIR"


def _wrap_unknown(payload: bytes, reason: str) -> dict[str, Any]:
    return {
        "v": 1,
        "level": "WARN",
        "msg": "Unparsed/unknown log payload",
        "data": {"reason": reason, "raw_b64": base64.b64encode(payload).decode("ascii")},
    }


def _softw_warn(msg: str, *, data: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "v": 1,
        "l_type": "SOFTW",
        "level": "WARNING",
        "msg": msg,
        "data": data or {},
        "origin": {"uuid": "logger_server", "ts_ns": int(time.time_ns())},
    }

def _resolve_logger_archive_dir(cli_log_dir: Path | None) -> Path:
    """Resolve the archive directory the logger server should write into."""
    root = resolve_log_dir(cli_log_dir, ENV_LOG_DIR)

    # If the user points directly at an archive, honor it (even if it's fresh).
    if root.name == "current" or root.parent.name == "archives":
        return root
    if (root / "index.sqlite3").exists():
        return root

    return root / "current"


def run_logger_server(
    bind: tuple[str, int],
    log_dir: Path,
    *,
    rotate_max_bytes: int = 256 * 1024 * 1024,
    rotate_max_seconds: int = 60 * 60,
    stop_flag: StopFlag | None = None,
) -> None:
    addr, port = bind
    if port is None:
        port = ECS_LOG_PORT

    bind = (addr, port)

    log_dir = _resolve_logger_archive_dir(log_dir)
    
    print("Using log dir", log_dir)
    print("Using bind address", bind)
    client_q: queue.Queue = queue.Queue()
    srv = tcp.TCPServer(bind, client_q)
    srv.start()

    writer = JournalWriter(
        log_dir,
        rotate_max_bytes=rotate_max_bytes,
        rotate_max_seconds=rotate_max_seconds,
        service_name="logger",
    )

    clients: list[Any] = []

    try:
        while srv.ok() and not (stop_flag is not None and not stop_flag.run()):
            while not client_q.empty():
                clients.append(client_q.get())

            for c in list(clients):
                if c.is_closed():
                    clients.remove(c)
                    continue

                while not c.empty():
                    msg = c.get(block=False)
                    if msg is None:
                        break

                    try:
                        msg_type, ver, payload = decode_message(msg)
                    except ProtocolError:
                        writer.append(_wrap_unknown(msg, "bad magic/header"))
                        continue

                    if ver != PROTO_V1:
                        writer.append(_wrap_unknown(payload, f"unsupported ver {ver}"))
                        continue

                    if msg_type == TYPE_LOG:
                        try:
                            rec = decode_log_record(payload)
                        except Exception as e:
                            writer.append(_wrap_unknown(payload, f"json decode error: {e}"))
                            continue

                        writer.append(rec)
                        continue

                    if msg_type in (TYPE_EVT_BEGIN, TYPE_EVT_END, TYPE_EVT_END_LAST):
                        try:
                            obj = decode_json_payload(payload)
                        except Exception as e:
                            writer.append(_wrap_unknown(payload, f"event json decode error: {e}"))
                            continue

                        if msg_type == TYPE_EVT_BEGIN:
                            event_id = obj.get("event_id")
                            e_type = obj.get("e_type")
                            level = obj.get("level")
                            message = obj.get("message")
                            data_start = obj.get("data_start") or obj.get("data") or {}

                            if not (isinstance(event_id, str) and isinstance(e_type, str) and isinstance(level, str) and isinstance(message, str)):
                                writer.append(_softw_warn("Bad EVT_BEGIN payload (missing fields).", data={"obj": obj}))
                                continue

                            try:
                                writer.begin_event(
                                    event_id=event_id,
                                    e_type=e_type,
                                    level=level,
                                    message=message,
                                    data_start=(data_start if isinstance(data_start, dict) else {}),
                                )
                            except Exception as e:
                                writer.append(_softw_warn("Failed to begin event.", data={"error": str(e), "event_id": event_id}))
                            continue

                        if msg_type == TYPE_EVT_END:
                            event_id = obj.get("event_id")
                            data_end = obj.get("data_end") or obj.get("data") or {}

                            if not isinstance(event_id, str):
                                writer.append(_softw_warn("Bad EVT_END payload (missing event_id).", data={"obj": obj}))
                                continue

                            try:
                                writer.end_event(
                                    event_id=event_id,
                                    data_end=(data_end if isinstance(data_end, dict) else {}),
                                )
                            except Exception as e:
                                writer.append(_softw_warn("Failed to end event.", data={"error": str(e), "event_id": event_id}))
                            continue

                        # TYPE_EVT_END_LAST
                        e_type = obj.get("e_type")
                        data_end = obj.get("data_end") or obj.get("data") or {}

                        try:
                            writer.end_last_event(
                                e_type=(e_type if isinstance(e_type, str) else None),
                                data_end=(data_end if isinstance(data_end, dict) else {}),
                            )
                        except Exception as e:
                            writer.append(_softw_warn("Failed to end last event.", data={"error": str(e), "e_type": e_type}))
                        continue

                    writer.append(_wrap_unknown(payload, f"unsupported type {msg_type}"))

            time.sleep(0.01)

    except KeyboardInterrupt:
        pass
    except Exception as e:
        print("Logger server exception:\n", traceback.format_exc())
        raise
    finally:
        print("Closing logger server...")
        writer.close()
        srv.close()
        print("Logger server stopped.")
