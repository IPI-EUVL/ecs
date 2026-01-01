from __future__ import annotations

import json

from typing import Any

MAGIC = b"IECS"      # 4 bytes
TYPE_LOG = 0x01      # 1 byte
TYPE_EVT_BEGIN = 0x02  # 1 byte
TYPE_EVT_END = 0x03    # 1 byte
TYPE_EVT_END_LAST = 0x04  # 1 byte
PROTO_V1 = 0x01      # 1 byte
HEADER_LEN = 6       # MAGIC(4) + TYPE(1) + VER(1)


class ProtocolError(Exception):
    pass


def encode_log_record(record: dict[str, Any]) -> bytes:
    payload = json.dumps(record, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return MAGIC + bytes([TYPE_LOG, PROTO_V1]) + payload


def decode_message(msg: bytes) -> tuple[int, int, bytes]:
    if len(msg) < HEADER_LEN:
        raise ProtocolError("message too short")
    if msg[:4] != MAGIC:
        raise ProtocolError("bad magic")
    msg_type = msg[4]
    ver = msg[5]
    return msg_type, ver, msg[6:]


def decode_log_record(payload: bytes) -> dict[str, Any]:
    return json.loads(payload.decode("utf-8"))

def _encode_json_payload(obj: dict[str, Any]) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def encode_event_begin(payload: dict[str, Any]) -> bytes:
    """Begin an event (marker). Server will assign start_line/start_ts_ns."""
    return MAGIC + bytes([TYPE_EVT_BEGIN, PROTO_V1]) + _encode_json_payload(payload)


def encode_event_end(payload: dict[str, Any]) -> bytes:
    """End an event by event_id. Server will assign end_line/end_ts_ns."""
    return MAGIC + bytes([TYPE_EVT_END, PROTO_V1]) + _encode_json_payload(payload)


def encode_event_end_last(payload: dict[str, Any]) -> bytes:
    """End the most recent OPEN event (optionally restricted by e_type)."""
    return MAGIC + bytes([TYPE_EVT_END_LAST, PROTO_V1]) + _encode_json_payload(payload)


def decode_json_payload(payload: bytes) -> dict[str, Any]:
    return json.loads(payload.decode("utf-8"))
