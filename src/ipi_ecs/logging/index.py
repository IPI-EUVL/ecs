from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable, Optional

DEFAULT_LEVEL_MAP = {
    "SOFTW": {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50},
    "EXP": {"NOTE": 10, "STEP": 20, "CHECK": 30, "IMPORTANT": 40, "ALERT": 50},
}

DDL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS segments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  path TEXT NOT NULL UNIQUE,
  start_line INTEGER NOT NULL,
  end_line INTEGER,
  start_ts_ns INTEGER NOT NULL,
  end_ts_ns INTEGER
);

CREATE TABLE IF NOT EXISTS records (
  line INTEGER PRIMARY KEY,
  origin_uuid TEXT NOT NULL,
  origin_ts_ns INTEGER NOT NULL,
  ingest_ts_ns INTEGER NOT NULL,
  l_type TEXT NOT NULL,
  level TEXT NOT NULL,
  level_num INTEGER NOT NULL,
  segment_path TEXT NOT NULL,
  byte_off INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_records_uuid_ts
  ON records(origin_uuid, origin_ts_ns);

CREATE INDEX IF NOT EXISTS idx_records_origin_ts
  ON records(origin_ts_ns);

CREATE INDEX IF NOT EXISTS idx_records_type_levelnum
  ON records(l_type, level_num);
"""

class SQLiteIndex:
    def __init__(self, db_path: Path, *, level_map: dict[str, dict[str, int]] | None = None):
        self.db_path = db_path
        self.level_map = level_map or DEFAULT_LEVEL_MAP

        self.conn = sqlite3.connect(str(db_path))
        self.conn.execute("PRAGMA busy_timeout=5000;")
        self.conn.executescript(DDL)
        self._init_meta()

    def close(self) -> None:
        self.conn.close()

    def _init_meta(self) -> None:
        cur = self.conn.execute("SELECT value FROM meta WHERE key='next_line'")
        row = cur.fetchone()
        if row is None:
            self.conn.execute("INSERT INTO meta(key,value) VALUES('next_line','0')")
            self.conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('schema_version','1')")
            self.conn.commit()

    def get_next_line(self) -> int:
        (v,) = self.conn.execute("SELECT value FROM meta WHERE key='next_line'").fetchone()
        return int(v)

    def set_next_line(self, n: int) -> None:
        self.conn.execute("UPDATE meta SET value=? WHERE key='next_line'", (str(n),))

    def _level_num(self, l_type: str, level: str) -> int:
        return int(self.level_map.get(l_type, {}).get(level, 0))

    def create_segment(self, path: str, start_line: int, start_ts_ns: int) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO segments(path,start_line,start_ts_ns) VALUES(?,?,?)",
            (path, start_line, start_ts_ns),
        )

    def finalize_segment(self, path: str, end_line: int, end_ts_ns: int) -> None:
        self.conn.execute(
            "UPDATE segments SET end_line=?, end_ts_ns=? WHERE path=?",
            (end_line, end_ts_ns, path),
        )

    def insert_record(
        self,
        *,
        line: int,
        origin_uuid: str,
        origin_ts_ns: int,
        ingest_ts_ns: int,
        l_type: str,
        level: str,
        segment_path: str,
        byte_off: int,
    ) -> None:
        level_num = self._level_num(l_type, level)
        self.conn.execute(
            """
            INSERT INTO records(line,origin_uuid,origin_ts_ns,ingest_ts_ns,l_type,level,level_num,segment_path,byte_off)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (line, origin_uuid, origin_ts_ns, ingest_ts_ns, l_type, level, level_num, segment_path, byte_off),
        )

    def query_lines(
        self,
        *,
        line_min: int | None = None,
        line_max: int | None = None,
        uuid: str | None = None,
        ts_min_ns: int | None = None,
        ts_max_ns: int | None = None,
        l_type: str | None = None,
        level: str | None = None,
        min_level_num: int | None = None,
        order_by: str = "line",
        desc: bool = False,
        limit: int | None = None,
    ) -> list[tuple[int, str, int]]:
        """
        Returns list of (line, segment_path, byte_off).
        """
        clauses = []
        params: list[Any] = []

        if line_min is not None:
            clauses.append("line >= ?")
            params.append(line_min)
        if line_max is not None:
            clauses.append("line < ?")
            params.append(line_max)
        if uuid is not None:
            clauses.append("origin_uuid = ?")
            params.append(uuid)
        if ts_min_ns is not None:
            clauses.append("origin_ts_ns >= ?")
            params.append(ts_min_ns)
        if ts_max_ns is not None:
            clauses.append("origin_ts_ns <= ?")
            params.append(ts_max_ns)
        if l_type is not None:
            clauses.append("l_type = ?")
            params.append(l_type)
        if level is not None:
            clauses.append("level = ?")
            params.append(level)
        if min_level_num is not None:
            clauses.append("level_num >= ?")
            params.append(min_level_num)

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        direction = "DESC" if desc else "ASC"

        allowed_order = {"line", "origin_ts_ns", "ingest_ts_ns", "level_num"}
        if order_by not in allowed_order:
            raise ValueError(f"order_by must be one of {sorted(allowed_order)}")

        sql = f"SELECT line, segment_path, byte_off FROM records {where} ORDER BY {order_by} {direction}"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)

        return list(self.conn.execute(sql, params))
