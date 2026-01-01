from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ipi_ecs.logging.index import SQLiteIndex
from ipi_ecs.logging.journal import SegmentInfo


class JournalReader:
    """
    Backwards-compatible reader that *does not* rely on manifest.json.

    Uses the archive's SQLite index (index.sqlite3) to locate records quickly.

    `root` should be an archive directory containing:
      - index.sqlite3
      - segment_*.ndjson (or similar segment files referenced by the index)
    """

    def __init__(self, root: Path):
        self.root = root
        self.index = SQLiteIndex(root / "index.sqlite3")
        self.segments: list[SegmentInfo] = []
        self.next_line: int = 0
        self.refresh()

    def close(self) -> None:
        self.index.close()

    def refresh(self) -> None:
        # next global line (exclusive)
        self.next_line = int(self.index.get_next_line())

        # segments table is maintained by the writer (periodically updated during active segment)
        rows = self.index.conn.execute(
            "SELECT path, start_line, end_line, start_ts_ns, end_ts_ns FROM segments ORDER BY start_line ASC"
        ).fetchall()

        segs: list[SegmentInfo] = []
        for (path, start_line, end_line, start_ts_ns, end_ts_ns) in rows:
            segs.append(
                SegmentInfo(
                    path=str(path),
                    start_line=int(start_line),
                    end_line=(int(end_line) if end_line is not None else None),
                    start_ts_ns=int(start_ts_ns),
                    end_ts_ns=(int(end_ts_ns) if end_ts_ns is not None else None),
                    idx_path=None,  # idx files are optional; SQLite already stores byte offsets
                )
            )
        self.segments = segs

    def read_between(self, start_linenum: int, end_linenum: int) -> list[dict[str, Any]]:
        """
        Returns records with global line numbers in [start_linenum, end_linenum).

        This is compatible with the old manifest-based reader, but uses SQLite.
        """
        # Refresh so rollovers are visible (segments + next_line)
        self.refresh()

        if end_linenum <= start_linenum:
            return []

        rows = self.index.query_lines(
            line_min=int(start_linenum),
            line_max=int(end_linenum),
            order_by="line",
            descending=False,
            limit=None,
        )

        out: list[dict[str, Any]] = []
        handles: dict[str, Any] = {}
        try:
            for _line, seg, off, ts_ns, level, level_num, uuid, l_type in rows:
                f = handles.get(seg)
                if f is None:
                    f = (self.root / seg).open("rb")
                    handles[seg] = f
                f.seek(off)
                raw = f.readline()
                out.append((_line, json.loads(raw.decode("utf-8"))))
        finally:
            for f in handles.values():
                try:
                    f.close()
                except Exception:
                    pass
        
        return out
