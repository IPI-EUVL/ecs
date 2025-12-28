from __future__ import annotations

import base64
import queue
import time
from pathlib import Path
from typing import Any

from ipi_ecs.core import tcp  # your wrapper should be here
from ipi_ecs.logging.journal import JournalWriter
from ipi_ecs.logging.protocol import (
    TYPE_LOG,
    PROTO_V1,
    ProtocolError,
    decode_message,
    decode_log_record,
)


def _wrap_unknown(payload: bytes, reason: str) -> dict[str, Any]:
    return {
        "v": 1,
        "level": "WARN",
        "msg": "Unparsed/unknown log payload",
        "data": {"reason": reason, "raw_b64": base64.b64encode(payload).decode("ascii")},
    }


def run_logger_server(
    bind: tuple[str, int],
    log_dir: Path,
    *,
    rotate_max_bytes: int = 256 * 1024 * 1024,
    rotate_max_seconds: int = 60 * 60,
) -> None:
    print("Using log dir", log_dir)
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
        while srv.ok():
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

                    if msg_type != TYPE_LOG or ver != PROTO_V1:
                        writer.append(_wrap_unknown(payload, f"unsupported type/ver {msg_type}/{ver}"))
                        continue

                    try:
                        rec = decode_log_record(payload)
                    except Exception as e:
                        writer.append(_wrap_unknown(payload, f"json decode error: {e}"))
                        continue

                    # Optionally validate required keys here; if missing, still store
                    writer.append(rec)

            time.sleep(0.01)

    except KeyboardInterrupt:
        pass
    finally:
        writer.close()
        srv.close()
