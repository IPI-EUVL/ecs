from __future__ import annotations

from datetime import datetime
from dateutil import parser as dtparser


def _local_tzinfo():
    # local tzinfo for “assume local”
    return datetime.now().astimezone().tzinfo


def parse_time_to_ns(s: str) -> int:
    """
    Parses human time strings.
    - If string has tz info, respects it.
    - If not, assumes LOCAL timezone.
    Returns ns since epoch (UTC-based).
    """
    dt = dtparser.parse(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_local_tzinfo())
    return int(dt.timestamp() * 1_000_000_000)


def fmt_ns_local(ns: int | None) -> str:
    if ns is None:
        return "?"
    dt = datetime.fromtimestamp(ns / 1e9).astimezone(_local_tzinfo())
    base = dt.strftime("%Y-%m-%d %H:%M:%S")
    ms = f"{dt.microsecond // 1000:03d}"
    return f"{base}.{ms}"
