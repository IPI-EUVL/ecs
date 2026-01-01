# src/ipi_ecs/logging/db_reader.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from ipi_ecs.logging.index import SQLiteIndex

class DBJournalReader:
    def __init__(self, root: Path):
        self.root = root
        self.index = SQLiteIndex(root / "index.sqlite3")

    def close(self) -> None:
        self.index.close()

    def read_line(self, line: int) -> dict[str, Any] | None:
        rows = self.index.query_lines(line_min=line, line_max=line+1, limit=1)
        if not rows:
            return None
        _, seg, off = rows[0]
        p = self.root / seg
        with p.open("rb") as f:
            f.seek(off)
            raw = f.readline()
        return json.loads(raw.decode("utf-8"))

    def query(
        self,
        **kwargs,
    ) -> list[tuple[int, dict[str, Any]]]:
        rows = self.index.query_lines(**kwargs)
        out: list[dict[str, Any]] = []
        # group by segment to reduce open/close churn (optional optimization later)
        for line, seg, off, ts_ns, level, level_num, uuid, l_type in rows:
            p = self.root / seg
            with p.open("rb") as f:
                f.seek(off)
                raw = f.readline()
            out.append((line, json.loads(raw.decode("utf-8"))))
        return out

    # ----------------------
    # Events (markers)
    # ----------------------

    @staticmethod
    def _row_to_event(row: tuple[Any, ...]) -> dict[str, Any]:
        (
            event_id,
            e_type,
            level,
            message,
            start_line,
            end_line,
            start_ts_ns,
            end_ts_ns,
            data_start_json,
            data_end_json,
        ) = row

        def _loads(x: Any) -> dict[str, Any]:
            if x is None:
                return {}
            if isinstance(x, (bytes, bytearray)):
                x = x.decode("utf-8")
            if isinstance(x, str) and x.strip():
                try:
                    v = json.loads(x)
                    return v if isinstance(v, dict) else {}
                except Exception:
                    return {}
            return {}

        return {
            "event_id": str(event_id),
            "e_type": str(e_type),
            "level": str(level),
            "message": str(message),
            "start_line": int(start_line),
            "end_line": (int(end_line) if end_line is not None else None),
            "start_ts_ns": int(start_ts_ns),
            "end_ts_ns": (int(end_ts_ns) if end_ts_ns is not None else None),
            "data_start": _loads(data_start_json),
            "data_end": _loads(data_end_json),
        }

    def get_event(self, event_id: str) -> dict[str, Any] | None:
        """Return one event by id, or None if not found."""
        cur = self.index.conn.execute(
            """
            SELECT id, e_type, level, message,
                start_line, end_line, start_ts_ns, end_ts_ns,
                data_start_json, data_end_json
            FROM events
            WHERE id = ?
            """,
            (str(event_id),),
        )
        row = cur.fetchone()
        return self._row_to_event(row) if row else None

    def list_events(
        self,
        *,
        e_type: str | None = None,
        open_only: bool | None = None,
        line_min: int | None = None,
        line_max: int | None = None,   # exclusive
        ts_min_ns: int | None = None,
        ts_max_ns: int | None = None,
        limit: int | None = None,
        desc: bool = True,
    ) -> list[dict[str, Any]]:
        """
        List events from this archive's SQLite DB.

        Overlap semantics:
        - line window: [line_min, line_max) overlaps [start_line, end_line] (end=open->start)
        - time window: [ts_min, ts_max] overlaps [start_ts, end_ts] (end=open->start)
        """
        clauses: list[str] = []
        params: list[Any] = []

        if e_type is not None:
            clauses.append("e_type = ?")
            params.append(str(e_type))

        if open_only is True:
            clauses.append("end_line IS NULL")
        elif open_only is False:
            clauses.append("end_line IS NOT NULL")

        if line_min is not None or line_max is not None:
            if line_max is not None:
                clauses.append("start_line < ?")
                params.append(int(line_max))
            if line_min is not None:
                clauses.append("COALESCE(end_line, start_line) >= ?")
                params.append(int(line_min))

        if ts_min_ns is not None or ts_max_ns is not None:
            if ts_max_ns is not None:
                clauses.append("start_ts_ns <= ?")
                params.append(int(ts_max_ns))
            if ts_min_ns is not None:
                clauses.append("COALESCE(end_ts_ns, start_ts_ns) >= ?")
                params.append(int(ts_min_ns))

        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        order = "ORDER BY start_line " + ("DESC" if desc else "ASC")
        lim = "LIMIT ?" if limit is not None else ""

        sql = f"""
            SELECT id, e_type, level, message,
                start_line, end_line, start_ts_ns, end_ts_ns,
                data_start_json, data_end_json
            FROM events
            {where}
            {order}
            {lim}
        """

        if limit is not None:
            params.append(int(limit))

        rows = self.index.conn.execute(sql, tuple(params)).fetchall()
        return [self._row_to_event(r) for r in rows]
