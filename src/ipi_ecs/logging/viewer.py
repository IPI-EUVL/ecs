# src/ipi_ecs/logging/viewer.py
from __future__ import annotations

import shutil
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

from ipi_ecs.logging.db_reader import DBJournalReader
from ipi_ecs.logging.index import DEFAULT_LEVEL_MAP, SQLiteIndex
from ipi_ecs.logging.journal import resolve_log_dir
from ipi_ecs.logging.timefmt import parse_time_to_ns, fmt_ns_local

ENV_LOG_DIR_DEFAULT = "IPI_ECS_LOG_DIR"


@dataclass(frozen=True)
class LogLine:
    line: int
    record: dict[str, Any]


@dataclass(frozen=True)
class ArchiveInfo:
    name: str
    path: Path
    start_line: int
    end_line_exclusive: int
    start_ts_ns: int
    end_ts_ns: int
@dataclass(frozen=True)
class EventInfo:
    event_id: str
    e_type: str
    level: str
    message: str
    start_line: int
    end_line: int | None
    start_ts_ns: int
    end_ts_ns: int | None
    data_start: dict[str, Any]
    data_end: dict[str, Any]





@dataclass
class QueryOptions:
    # filters
    uuid: str | None = None
    line_from: int | None = None                # inclusive
    line_to: int | None = None                  # inclusive
    since: str | None = None                    # human string
    until: str | None = None                    # human string
    l_type: str | None = None
    exclude_types: list[str] | None = None
    level: str | None = None
    min_level: str | None = None                # requires l_type


    # sorting / limits
    order_by: str = "line"
    desc: bool = False
    limit: int | None = None

    def to_db_kwargs(self) -> dict[str, Any]:
        ts_min = parse_time_to_ns(self.since) if self.since else None
        ts_max = parse_time_to_ns(self.until) if self.until else None

        # inclusive -> exclusive upper bound
        line_max = (self.line_to + 1) if self.line_to is not None else None

        min_level_num = None
        if self.min_level is not None:
            min_level_num = DEFAULT_LEVEL_MAP.get(self.min_level, 0)

        return dict(
            line_min=self.line_from,
            line_max=line_max,
            uuid=self.uuid,
            ts_min_ns=ts_min,
            ts_max_ns=ts_max,
            l_type=self.l_type,
            l_type_not=self.exclude_types,
            level=self.level,
            min_level_num=min_level_num,
            order_by=self.order_by,
            descending=self.desc,
            limit=self.limit,
        )

RICH_TYPE_STYLE: dict[str, str] = {
    "ERROR": "bold red blink",
    "WARN": "yellow",
    "INFO": "cyan",
    "DEBUG": "dim",
    "rec": "dim",
    "default": "",

    "subsystem": "cyan"
}

RICH_MESSAGE_STYLE: dict[str, str] = {
    "ERROR": "bold red",
    "WARN": "yellow",
    "INFO": "",
    "DEBUG": "dim",
    "rec": "dim",
    "default": "",
}


# prompt_toolkit Style.from_dict() keys (used as "class:log.error", etc.)
PT_STYLE: dict[str, str] = {
    "log.error": "fg:#ff4444 bold blink",
    "log.warn": "fg:#ffaa00",
    "log.info": "fg:#44bbff",
    "log.debug": "fg:#777777",
    "log.exp_hi": "fg:#44ff88 bold",
    "log.exp": "fg:#44ff88",
    "log.exp_low": "fg:#cccccc",
    "log.telem": "fg:#666666",
    "log.default": "",

    "log.lineno": "fg:#777777",
    "log.ts": "fg:#777777",
    "log.uuid": "fg:#7f7f7f",
    "log.subsystem": "fg:#44bbff",
}



def get_subsystem(rec: dict[str, Any]) -> str:
    """
    Extract subsystem string from a record.

    Convention: subsystem is stored under record["data"]["subsystem"].
    """
    data = rec.get("data") or {}
    if isinstance(data, dict):
        s = data.get("subsystem")
        if isinstance(s, str) and s:
            return s
    return "(no subsystem)"


def format_line(line: LogLine) -> str:
    """CLI-friendly formatter. GUIs should use the structured record directly."""
    rec = line.record
    origin = rec.get("origin", {}) or {}
    ts = origin.get("ts_ns")
    ou = origin.get("uuid", "?")
    l_type = rec.get("l_type", "?")
    level = rec.get("level", "?")
    msg = rec.get("msg", "")
    s = get_subsystem(rec)
    return f"{line.line:>10}  {fmt_ns_local(ts)}  [{l_type}/{level}]  {ou} {s} : {msg}"


class ArchiveView:
    """
    View into a single archive directory (including 'current').
    Provides query/follow/windowed reads suitable for CLI *and* GUI.
    """
    def __init__(self, archive_dir: Path):
        self.archive_dir = archive_dir
        self.reader = DBJournalReader(archive_dir)

    def close(self) -> None:
        self.reader.close()

    def next_line(self) -> int:
        return self.reader.index.get_next_line()

    def query(self, opts: QueryOptions) -> list[LogLine]:
        rows = self.reader.query(**opts.to_db_kwargs())
        return [LogLine(line, rec) for line, rec in rows]

    def follow(
        self,
        opts: QueryOptions,
        *,
        tail: int = 200,
        batch: int = 200,
        poll: float = 0.25,
    ) -> Iterator[LogLine]:
        """
        Generator that yields lines matching `opts` as they arrive.

        Important: if filters are applied, the returned line numbers may be non-contiguous.
        We track the max emitted line and resume from max+1.
        """
        base = opts.to_db_kwargs()

        if opts.line_from is not None:
            pos = opts.line_from
        else:
            pos = max(0, self.next_line() - tail)

        while True:
            q = dict(base)
            q["line_min"] = pos
            q["limit"] = batch
            q["order_by"] = "line"
            q["desc"] = False

            rows = self.reader.query(**q)
            if rows:
                max_line = pos
                for line, rec in rows:
                    yield LogLine(line, rec)
                    if line > max_line:
                        max_line = line
                pos = max_line + 1
            else:
                # optional stop if the user bounded the upper range
                if base.get("line_max") is not None and pos >= base["line_max"]:
                    return
                time.sleep(poll)

    def window_before(self, opts: QueryOptions, *, line_max_exclusive: int | None, window: int) -> list[LogLine]:
        qopts = QueryOptions(**vars(opts))
        qopts.order_by = "line"
        qopts.desc = True
        qopts.limit = window
        rows = self.reader.query(**{**qopts.to_db_kwargs(), "line_max": line_max_exclusive})
        rows.reverse()
        return [LogLine(line, rec) for line, rec in rows]

    def window_after(self, opts: QueryOptions, *, line_min_inclusive: int | None, window: int) -> list[LogLine]:
        qopts = QueryOptions(**vars(opts))
        qopts.order_by = "line"
        qopts.desc = False
        qopts.limit = window
        rows = self.reader.query(**{**qopts.to_db_kwargs(), "line_min": line_min_inclusive})
        return [LogLine(line, rec) for line, rec in rows]
    
    def window_between(self, opts: QueryOptions, *, line_min_inclusive: int | None, line_max_exclusive: int | None, window: int) -> list[LogLine]:
        qopts = QueryOptions(**vars(opts))
        qopts.order_by = "line"
        qopts.desc = False
        qopts.limit = window
        rows = self.reader.query(**{**qopts.to_db_kwargs(), "line_min": line_min_inclusive, "line_max": line_max_exclusive})
        return [LogLine(line, rec) for line, rec in rows]
    # ----------------------------
    # Event helpers (archive-local)
    # ----------------------------
    def list_events(
        self,
        *,
        e_type: str | None = None,
        open_only: bool | None = None,
        line_min: int | None = None,
        line_max: int | None = None,   # exclusive
        since: str | None = None,
        until: str | None = None,
        limit: int | None = None,
        desc: bool = True,
    ) -> list[EventInfo]:
        ts_min = parse_time_to_ns(since) if since else None
        ts_max = parse_time_to_ns(until) if until else None
        rows = self.reader.list_events(
            e_type=e_type,
            open_only=open_only,
            line_min=line_min,
            line_max=line_max,
            ts_min_ns=ts_min,
            ts_max_ns=ts_max,
            limit=limit,
            desc=desc,
        )
        return [EventInfo(**r) for r in rows]

    def get_event(self, event_id: str) -> EventInfo | None:
        r = self.reader.get_event(event_id)
        return EventInfo(**r) if r else None

    def event_line_range(self, ev: EventInfo) -> tuple[int, int]:
        """
        Return inclusive (start_line, end_line) for viewing.

        Open events (end_line is NULL) are treated as one-line markers.
        """
        start = int(ev.start_line)
        end = int(ev.end_line) if ev.end_line is not None else start
        if end < start:
            end = start
        return start, end

    def apply_event_range(self, opts: QueryOptions, ev: EventInfo) -> QueryOptions:
        """Copy opts and set line_from/line_to to this event's inclusive range."""
        start, end = self.event_line_range(ev)
        q = QueryOptions(**vars(opts))
        q.line_from = start
        q.line_to = end
        return q




class LogViewer:
    """
    Handles:
      - resolving log root / archive directories
      - listing archives
      - locating which archive contains a global line
      - archiving current -> archives/<name> (and seeding new current next_line)

    CLI and GUI can both use this.
    """
    def __init__(self, log_dir: Path | None, *, env_var: str = ENV_LOG_DIR_DEFAULT):
        resolved = resolve_log_dir(log_dir, env_var)

        # If user pointed directly at an archive dir, treat it as a fixed archive view.
        self._direct_archive = (resolved / "index.sqlite3").exists()
        self._resolved = resolved
        self.env_var = env_var

    @property
    def log_root(self) -> Path:
        if self._direct_archive:
            raise RuntimeError("log_root is not available when log_dir points directly at an archive directory")
        return self._resolved

    def open_archive(self, archive: str | None = None) -> ArchiveView:
        if self._direct_archive:
            if archive:
                raise ValueError("--archive cannot be used when log_dir points directly at an archive directory")
            return ArchiveView(self._resolved)

        if archive is None or archive == "current":
            return ArchiveView(self.log_root / "current")
        return ArchiveView(self.log_root / "archives" / archive)

    # ----------------------------
    # Archive helpers
    # ----------------------------
    @staticmethod
    def _read_next_line_from_index(db_path: Path) -> int:
        if not db_path.exists():
            return 0
        conn = sqlite3.connect(str(db_path))
        try:
            cur = conn.execute("SELECT v FROM meta WHERE k='next_line'")
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()

    @staticmethod
    def _default_archive_name(archives_dir: Path) -> str:
        base = datetime.now().strftime("%Y-%m-%d")
        seq = 1
        while True:
            name = f"{base}_{seq:03d}"
            if not (archives_dir / name).exists():
                return name
            seq += 1

    def archive_current(self, name: str | None = None) -> ArchiveInfo:
        """
        Move <log_root>/current -> <log_root>/archives/<name> and create a new empty current.
        Seeds new current's next_line to preserve global monotonic line numbers.

        NOTE: Stop the logger before calling this (especially on Windows).
        """
        if self._direct_archive:
            raise RuntimeError("Cannot archive when log_dir points directly at an archive directory")

        current_dir = self.log_root / "current"
        archives_dir = self.log_root / "archives"
        archives_dir.mkdir(parents=True, exist_ok=True)

        if not current_dir.exists():
            raise FileNotFoundError(f"No current archive found at: {current_dir}")

        name = name or self._default_archive_name(archives_dir)
        dest_dir = archives_dir / name
        if dest_dir.exists():
            raise FileExistsError(f"Archive already exists: {dest_dir}")

        prev_next = self._read_next_line_from_index(current_dir / "index.sqlite3")

        shutil.move(str(current_dir), str(dest_dir))

        current_dir.mkdir(parents=True, exist_ok=True)
        idx = SQLiteIndex(current_dir / "index.sqlite3")
        idx.set_next_line(prev_next)
        idx.conn.commit()
        idx.close()

        info = self._archive_info(name, dest_dir)
        return info

    def _archive_info(self, name: str, archive_dir: Path) -> ArchiveInfo:
        db = archive_dir / "index.sqlite3"
        if not db.exists():
            return ArchiveInfo(name, archive_dir, 0, 0, 0, 0)

        conn = sqlite3.connect(str(db))
        try:
            srow = conn.execute(
                "SELECT MIN(start_ts_ns), MAX(COALESCE(end_ts_ns, start_ts_ns)) FROM segments"
            ).fetchone()
            start_ts_ns = int(srow[0] or 0)
            end_ts_ns = int(srow[1] or 0)

            lrow = conn.execute("SELECT MIN(start_line) FROM segments").fetchone()
            start_line = int(lrow[0] or 0)
        finally:
            conn.close()

        end_line = self._read_next_line_from_index(db)
        return ArchiveInfo(name, archive_dir, start_line, end_line, start_ts_ns, end_ts_ns)

    def list_archives(self, *, since: str | None = None, until: str | None = None) -> list[ArchiveInfo]:
        """
        List <log_root>/archives/*, optionally filtering by time overlap.
        """
        if self._direct_archive:
            raise RuntimeError("Cannot list archives when log_dir points directly at an archive directory")

        archives_dir = self.log_root / "archives"
        if not archives_dir.exists():
            return []

        since_ns = parse_time_to_ns(since) if since else None
        until_ns = parse_time_to_ns(until) if until else None

        items: list[ArchiveInfo] = []
        for p in sorted([x for x in archives_dir.iterdir() if x.is_dir()]):
            info = self._archive_info(p.name, p)

            # overlap filter (if archive has 0 timestamps, don't filter it out)
            if since_ns is not None and info.end_ts_ns and info.end_ts_ns < since_ns:
                continue
            if until_ns is not None and info.start_ts_ns and info.start_ts_ns > until_ns:
                continue

            items.append(info)

        return items

    def locate_line(self, line: int) -> ArchiveInfo | None:
        """
        Return the archive that contains `line`, checking current then archives.
        """
        if self._direct_archive:
            # If user pointed at a single archive, we can't know other archives.
            # Still: we can check if the line exists in this archive by range.
            info = self._archive_info("archive", self._resolved)
            return info if info.start_line <= line < info.end_line_exclusive else None

        candidates: list[ArchiveInfo] = []

        # current
        current_dir = self.log_root / "current"
        if (current_dir / "index.sqlite3").exists():
            candidates.append(self._archive_info("current", current_dir))

        # archives
        for info in self.list_archives():
            candidates.append(info)

        matches = [c for c in candidates if c.start_line <= line < c.end_line_exclusive]
        if not matches:
            return None

        matches.sort(key=lambda a: (a.end_line_exclusive - a.start_line, a.start_line))
        return matches[0]
