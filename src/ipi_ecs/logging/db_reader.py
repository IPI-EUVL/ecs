# src/ipi_ecs/logging/db_reader.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ipi_ecs.logging.index import SQLiteIndex


class DBJournalReader:
    """
    Reads log records using the SQLite index for fast lookup.

    `root` should be an *archive directory* containing:
      - index.sqlite3
      - manifest.json
      - segment_*.ndjson
    """

    def __init__(self, root: Path):
        self.root = root
        self.index = SQLiteIndex(root / "index.sqlite3")

    def close(self) -> None:
        self.index.close()

    def read_line(self, line: int) -> dict[str, Any] | None:
        rows = self.index.query_lines(line_min=line, line_max=line + 1, limit=1)
        if not rows:
            return None
        _, seg, off = rows[0]
        p = self.root / seg
        with p.open("rb") as f:
            f.seek(off)
            raw = f.readline()
        return json.loads(raw.decode("utf-8"))

    def query(self, **kwargs) -> list[tuple[int, dict[str, Any]]]:
        """
        Query logs and return a list of (global_line_number, record_dict).

        Ordering is preserved exactly as returned by SQLiteIndex.query_lines()
        (i.e. respects order_by/desc).
        """
        rows = self.index.query_lines(**kwargs)  # [(line, segment_path, byte_off), ...]

        out: list[tuple[int, dict[str, Any]]] = []
        handles: dict[str, Any] = {}
        try:
            for line, seg, off in rows:
                f = handles.get(seg)
                if f is None:
                    p = self.root / seg
                    f = p.open("rb")
                    handles[seg] = f
                f.seek(off)
                raw = f.readline()
                out.append((line, json.loads(raw.decode("utf-8"))))
        finally:
            for f in handles.values():
                try:
                    f.close()
                except Exception:
                    pass

        return out
