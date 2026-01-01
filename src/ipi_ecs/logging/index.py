# src/ipi_ecs/logging/index.py
from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

DEFAULT_LEVEL_MAP = {
    "DEBUG": 10,
    "INFO": 20,
    "WARN": 30,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50,
}

DDL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS segments (
    path TEXT PRIMARY KEY,
    start_line INTEGER NOT NULL,
    end_line INTEGER,
    start_ts_ns INTEGER NOT NULL,
    end_ts_ns INTEGER
);

CREATE INDEX IF NOT EXISTS idx_segments_start_line ON segments(start_line);
CREATE INDEX IF NOT EXISTS idx_segments_end_line ON segments(end_line);

CREATE TABLE IF NOT EXISTS records (
    line INTEGER PRIMARY KEY,
    ts_ns INTEGER NOT NULL,
    ingest_ts_ns INTEGER NOT NULL,
    level_num INTEGER NOT NULL,
    level TEXT NOT NULL,
    uuid TEXT,
    l_type TEXT,
    segment_path TEXT NOT NULL REFERENCES segments(path),
    offset INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_records_ts ON records(ts_ns);
CREATE INDEX IF NOT EXISTS idx_records_levelnum ON records(level_num);
CREATE INDEX IF NOT EXISTS idx_records_uuid ON records(uuid);
CREATE INDEX IF NOT EXISTS idx_records_ltype ON records(l_type);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    e_type TEXT NOT NULL,
    level TEXT NOT NULL,
    level_num INTEGER NOT NULL,
    message TEXT NOT NULL,
    start_line INTEGER NOT NULL,
    end_line INTEGER,
    start_ts_ns INTEGER NOT NULL,
    end_ts_ns INTEGER,
    data_start_json TEXT,
    data_end_json TEXT,
    created_ns INTEGER NOT NULL,
    updated_ns INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_type ON events(e_type);
CREATE INDEX IF NOT EXISTS idx_events_start_line ON events(start_line);
CREATE INDEX IF NOT EXISTS idx_events_end_line ON events(end_line);
CREATE INDEX IF NOT EXISTS idx_events_updated ON events(updated_ns);
"""


def _now_ns() -> int:
    return time.time_ns()


def _json_dumps(v: Any) -> str:
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False)


def _json_loads(s: str | None) -> Any:
    if not s:
        return None
    return json.loads(s)


@dataclass(frozen=True)
class EventRow:
    id: str
    e_type: str
    level: str
    message: str
    start_line: int
    end_line: int | None
    start_ts_ns: int
    end_ts_ns: int | None
    data_start: Any
    data_end: Any

    @staticmethod
    def from_row(r: sqlite3.Row) -> "EventRow":
        return EventRow(
            id=r["id"],
            e_type=r["e_type"],
            level=r["level"],
            message=r["message"],
            start_line=r["start_line"],
            end_line=r["end_line"],
            start_ts_ns=r["start_ts_ns"],
            end_ts_ns=r["end_ts_ns"],
            data_start=_json_loads(r["data_start_json"]),
            data_end=_json_loads(r["data_end_json"]),
        )


class SQLiteIndex:
    """
    Index DB for an *archive directory*.

    Expected archive layout:
      <archive>/
        index.sqlite3
        <segment>.jsonl (or similar)
    """

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(DDL)
        self._ensure_next_line()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def _ensure_next_line(self) -> None:
        row = self.conn.execute("SELECT v FROM meta WHERE k='next_line'").fetchone()
        if row is None:
            self.conn.execute("INSERT INTO meta(k,v) VALUES('next_line','0')")
            self.conn.commit()

    # -------------------------
    # Meta / line counter
    # -------------------------

    def get_next_line(self) -> int:
        row = self.conn.execute("SELECT v FROM meta WHERE k='next_line'").fetchone()
        if row is None:
            return 0
        return int(row["v"])

    def set_next_line(self, next_line: int) -> None:
        self.conn.execute("INSERT INTO meta(k,v) VALUES('next_line',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(int(next_line)),))
        self.conn.commit()

    # -------------------------
    # Segments
    # -------------------------

    def create_segment(self, *, path: str, start_line: int, start_ts_ns: int) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO segments(path,start_line,end_line,start_ts_ns,end_ts_ns) VALUES(?,?,?,?,NULL)",
            (path, int(start_line), None, int(start_ts_ns)),
        )
        self.conn.commit()

    def finalize_segment(self, *, path: str, end_line: int, end_ts_ns: int) -> None:
        self.conn.execute(
            "UPDATE segments SET end_line=?, end_ts_ns=? WHERE path=?",
            (int(end_line), int(end_ts_ns), path),
        )
        self.conn.commit()

    def list_segments(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT path,start_line,end_line,start_ts_ns,end_ts_ns FROM segments ORDER BY start_line ASC"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_segment_for_line(self, line: int) -> dict[str, Any] | None:
        # end_line is exclusive; active segment may have NULL end_line
        row = self.conn.execute(
            """
            SELECT path,start_line,end_line,start_ts_ns,end_ts_ns
            FROM segments
            WHERE start_line <= ?
              AND (end_line IS NULL OR end_line > ?)
            ORDER BY start_line DESC
            LIMIT 1
            """,
            (int(line), int(line)),
        ).fetchone()
        return dict(row) if row else None

    # -------------------------
    # Records
    # -------------------------

    def insert_record(
        self,
        *,
        line: int,
        ts_ns: int,
        ingest_ts_ns: int,
        level: str,
        uuid: str | None,
        l_type: str | None,
        segment_path: str,
        offset: int,
        level_map: dict[str, int] | None = None,
    ) -> None:
        lm = level_map or DEFAULT_LEVEL_MAP
        lvl = (level or "INFO").upper()
        lvl_num = int(lm.get(lvl, 0))
        self.conn.execute(
            """
            INSERT INTO records(line,ts_ns,ingest_ts_ns,level_num,level,uuid,l_type,segment_path,offset)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (int(line), int(ts_ns), int(ingest_ts_ns), lvl_num, lvl, uuid, l_type, segment_path, int(offset)),
        )

    def query_lines(
        self,
        *,
        line_min: int | None = None,
        line_max: int | None = None,
        ts_min_ns: int | None = None,
        ts_max_ns: int | None = None,
        uuid: str | None = None,
        l_type: str | None = None,
        l_type_not: Iterable[str] | None = None,
        level: str | None = None,
        min_level: str | None = None,
        min_level_num: int | None = None,
        limit: int | None = None,
        order_by: str = "line",
        descending: bool = False,
    ) -> list[sqlite3.Row]:
        """
        Returns sqlite3.Row objects containing:
          line, segment_path, offset, ts_ns, level, level_num, uuid, l_type
        """
        where: list[str] = []
        params: list[Any] = []

        if line_min is not None:
            where.append("line >= ?")
            params.append(int(line_min))
        if line_max is not None:
            where.append("line < ?")
            params.append(int(line_max))
        if ts_min_ns is not None:
            where.append("ts_ns >= ?")
            params.append(int(ts_min_ns))
        if ts_max_ns is not None:
            where.append("ts_ns < ?")
            params.append(int(ts_max_ns))
        if uuid:
            where.append("uuid = ?")
            params.append(uuid)
        if l_type:
            where.append("l_type = ?")
            params.append(l_type)

        if l_type_not:
            vals = [v for v in l_type_not if v]
            if vals:
                where.append(f"l_type NOT IN ({','.join(['?']*len(vals))})")
                params.extend(vals)

        if level:
            lvl = level.upper()
            where.append("level = ?")
            params.append(lvl)

        if min_level:
            lvl = min_level.upper()
            min_num = int(DEFAULT_LEVEL_MAP.get(lvl, 0))
            where.append("level_num >= ?")
            params.append(min_num)

        if min_level_num:
            where.append("level_num >= ?")
            params.append(min_level_num)

        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        order_col = order_by if order_by in {"line", "ts_ns", "level_num"} else "line"
        dir_sql = "DESC" if descending else "ASC"

        sql = f"""
            SELECT line, segment_path, offset, ts_ns, level, level_num, uuid, l_type
            FROM records
            {where_sql}
            ORDER BY {order_col} {dir_sql}
        """

        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))

        return list(self.conn.execute(sql, params).fetchall())

    # -------------------------
    # Events
    # -------------------------

    def begin_event(
        self,
        *,
        event_id: str,
        e_type: str,
        level: str,
        message: str,
        start_line: int,
        start_ts_ns: int,
        data_start: Any | None = None,
        level_map: dict[str, int] | None = None,
    ) -> str:
        lm = level_map or DEFAULT_LEVEL_MAP
        lvl = (level or "INFO").upper()
        lvl_num = int(lm.get(lvl, 0))
        now = _now_ns()
        self.conn.execute(
            """
            INSERT INTO events(id,e_type,level,level_num,message,start_line,end_line,start_ts_ns,end_ts_ns,
                               data_start_json,data_end_json,created_ns,updated_ns)
            VALUES(?,?,?,?,?,?,NULL,?,NULL,?,NULL,?,?)
            """,
            (
                event_id,
                e_type,
                lvl,
                lvl_num,
                message or "",
                int(start_line),
                int(start_ts_ns),
                _json_dumps(data_start) if data_start is not None else None,
                int(now),
                int(now),
            ),
        )
        self.conn.commit()
        return event_id

    def end_event(
        self,
        *,
        event_id: str,
        end_line: int,
        end_ts_ns: int,
        data_end: Any | None = None,
    ) -> None:
        now = _now_ns()
        self.conn.execute(
            """
            UPDATE events
            SET end_line=?, end_ts_ns=?, data_end_json=?, updated_ns=?
            WHERE id=?
            """,
            (
                int(end_line),
                int(end_ts_ns),
                _json_dumps(data_end) if data_end is not None else None,
                int(now),
                event_id,
            ),
        )
        self.conn.commit()

    def end_last_event(
        self,
        *,
        e_type: str | None,
        end_line: int,
        end_ts_ns: int,
        data_end: Any | None = None,
    ) -> str | None:
        params: list[Any] = []
        where = "end_line IS NULL"
        if e_type:
            where += " AND e_type=?"
            params.append(e_type)
        row = self.conn.execute(
            f"SELECT id FROM events WHERE {where} ORDER BY start_line DESC, updated_ns DESC LIMIT 1",
            params,
        ).fetchone()
        if not row:
            return None
        event_id = row["id"]
        self.end_event(event_id=event_id, end_line=end_line, end_ts_ns=end_ts_ns, data_end=data_end)
        return event_id

    def get_event(self, event_id: str) -> EventRow | None:
        row = self.conn.execute(
            """
            SELECT id,e_type,level,message,start_line,end_line,start_ts_ns,end_ts_ns,data_start_json,data_end_json
            FROM events WHERE id=?
            """,
            (event_id,),
        ).fetchone()
        return EventRow.from_row(row) if row else None

    def list_events(
        self,
        *,
        e_type: str | None = None,
        open_only: bool = False,
        limit: int | None = 200,
        newest_first: bool = True,
    ) -> list[EventRow]:
        where: list[str] = []
        params: list[Any] = []
        if e_type:
            where.append("e_type=?")
            params.append(e_type)
        if open_only:
            where.append("end_line IS NULL")
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        order = "DESC" if newest_first else "ASC"
        sql = f"""
            SELECT id,e_type,level,message,start_line,end_line,start_ts_ns,end_ts_ns,data_start_json,data_end_json
            FROM events
            {where_sql}
            ORDER BY start_line {order}, updated_ns {order}
        """
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))
        rows = self.conn.execute(sql, params).fetchall()
        return [EventRow.from_row(r) for r in rows]
