from __future__ import annotations

import json
import os
import time
import uuid

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from platformdirs import site_data_dir
from ipi_ecs.logging.index import SQLiteIndex


def resolve_log_dir(cli_log_dir: Path | None, env_var: str) -> Path:
    if cli_log_dir is not None:
        return cli_log_dir
    env = os.getenv(env_var)
    if env:
        return Path(env)
    return Path(site_data_dir(appname="ipi-ecs", appauthor="IPI", ensure_exists=True))


@dataclass
class SegmentInfo:
    path: str
    start_line: int
    end_line: int | None
    start_ts_ns: int
    end_ts_ns: int | None
    idx_path: str | None = None


class JournalWriter:
    def __init__(
        self,
        root: Path,
        *,
        rotate_max_bytes: int = 256 * 1024 * 1024,
        rotate_max_seconds: int = 60 * 60,
        index_every_lines: int = 2000,
        session_id: str | None = None,
        service_name: str = "logger",
        commit_interval_s: float = 5.0,
        segment_update_interval_s: float = 5.0,
    ):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

        self.rotate_max_bytes = rotate_max_bytes
        self.rotate_max_seconds = rotate_max_seconds
        self.index_every_lines = index_every_lines

        self.commit_interval_s = commit_interval_s
        self.segment_update_interval_s = segment_update_interval_s
        self._last_commit_mon = time.monotonic()
        self._last_seg_update_mon = time.monotonic()

        self.session_id = session_id or str(uuid.uuid4())
        self.service_name = service_name

        self._manifest_path = self.root / "manifest.json"
        self._segments: list[SegmentInfo] = []
        self._global_line = 0

        self._active_fp = None
        self._active_idx_fp = None
        self._active_started_ns = 0
        self._active_seg: SegmentInfo | None = None

        self.index = SQLiteIndex(root / "index.sqlite3")
        self._global_line = self.index.get_next_line()

        self._load_manifest()
        self._open_new_segment()

    def close(self) -> None:
        self._finalize_active_segment()
        self._save_manifest()

    def _load_manifest(self) -> None:
        if not self._manifest_path.exists():
            return
        obj = json.loads(self._manifest_path.read_text(encoding="utf-8"))
        self._global_line = int(obj.get("next_line", 0))
        self._segments = [SegmentInfo(**seg) for seg in obj.get("segments", [])]

    def _save_manifest(self) -> None:
        tmp = self._manifest_path.with_suffix(".json.tmp")
        obj = {
            "v": 1,
            "session_id": self.session_id,
            "service": self.service_name,
            "next_line": self._global_line,
            "segments": [seg.__dict__ for seg in self._segments],
        }
        tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
        tmp.replace(self._manifest_path)

    def _segment_filename(self, seq: int, start_ns: int) -> str:
        t = time.strftime("%Y-%m-%dT%H%M%S", time.gmtime(start_ns / 1e9))
        return f"{t}.{start_ns % 1_000_000_000:09d}Z_{self.service_name}_session-{self.session_id}_{seq:06d}.ndjson"

    def _finalize_active_segment(self) -> None:
        if self._active_seg is not None:
            self._active_seg.end_line = self._global_line
            self._active_seg.end_ts_ns = time.time_ns()

            self.index.finalize_segment(path=self._active_name, end_line=self._global_line, end_ts_ns=time.time_ns())
            self.index.conn.commit()

        for fp in (self._active_fp, self._active_idx_fp):
            if fp is None:
                continue
            try:
                fp.flush()
            except Exception:
                pass
            try:
                fp.close()
            except Exception:
                pass

        self._active_fp = None
        self._active_idx_fp = None
        self._active_seg = None

    def _open_new_segment(self) -> None:
        self._finalize_active_segment()

        start_ns = time.time_ns()
        seq = len(self._segments) + 1
        name = self._segment_filename(seq, start_ns)
        path = self.root / name

        self._active_fp = path.open("ab", buffering=0)
        self._active_started_ns = start_ns

        idx_path = None
        if self.index_every_lines and self.index_every_lines > 0:
            idx_path = str(path.with_suffix(".idx").name)
            self._active_idx_fp = (self.root / idx_path).open("ab", buffering=0)

        seg = SegmentInfo(
            path=str(path.name),
            start_line=self._global_line,
            end_line=None,
            start_ts_ns=start_ns,
            end_ts_ns=None,
            idx_path=idx_path,
        )
        self._segments.append(seg)
        self._active_seg = seg

        self._active_name = path.name
        self.index.create_segment(path=self._active_name, start_line=self._global_line, start_ts_ns=start_ns)
        self.index.conn.commit()

        self._save_manifest()

    def _should_rotate(self) -> bool:
        if self._active_fp is None:
            return True
        try:
            size = self._active_fp.tell()
        except Exception:
            size = 0
        age_s = (time.time_ns() - self._active_started_ns) / 1e9
        return (size >= self.rotate_max_bytes) or (age_s >= self.rotate_max_seconds)

    # ----- Events (experiment markers) -----

    def next_line(self) -> int:
        """Return the next global line number that will be assigned to the next appended log record."""
        return int(self._global_line)

    def begin_event(
        self,
        *,
        event_id: str,
        e_type: str,
        level: str,
        message: str,
        data_start: dict[str, Any] | None = None,
    ) -> None:
        """
        Begin an event marker at the current global next_line().

        Commits immediately so a crash mid-experiment still leaves an OPEN event marker in the archive DB.
        """
        start_line = int(self._global_line)
        start_ts_ns = int(time.time_ns())
        self.index.begin_event(
            event_id=event_id,
            e_type=e_type,
            level=level,
            message=message,
            start_line=start_line,
            start_ts_ns=start_ts_ns,
            data_start=data_start,
        )
        self.index.conn.commit()
        self._last_commit_mon = time.monotonic()

    def end_event(
        self,
        *,
        event_id: str,
        data_end: dict[str, Any] | None = None,
    ) -> bool:
        """
        End an event marker by id.

        end_line is (next_line - 1), clamped >= start_line. Commits immediately.
        """
        end_ts_ns = int(time.time_ns())
        end_line = int(self._global_line) - 1
        if end_line < 0:
            end_line = 0

        ev = self.index.get_event(event_id)
        if ev and isinstance(ev.start_line, int):
            end_line = max(end_line, int(ev.start_line))

        updated = self.index.end_event(
            event_id=event_id,
            end_line=end_line,
            end_ts_ns=end_ts_ns,
            data_end=data_end,
        )
        self.index.conn.commit()
        self._last_commit_mon = time.monotonic()
        return bool(updated)

    def end_last_event(
        self,
        *,
        e_type: str | None = None,
        data_end: dict[str, Any] | None = None,
    ) -> str | None:
        """
        Recovery helper: end the most recent OPEN event (optionally restricted by e_type).
        Commits immediately.
        """
        end_ts_ns = int(time.time_ns())
        end_line = int(self._global_line) - 1
        if end_line < 0:
            end_line = 0

        ended_id = self.index.end_last_event(
            e_type=e_type,
            end_line=end_line,
            end_ts_ns=end_ts_ns,
            data_end=data_end,
        )
        if ended_id is not None:
            self.index.conn.commit()
            self._last_commit_mon = time.monotonic()
        return ended_id

    def append(self, record: dict[str, Any]) -> int:
        if self._active_fp is None or self._should_rotate():
            self._open_new_segment()

        rec = dict(record)
        ingest_ts_ns = int(rec.get("ingest_ts_ns") or time.time_ns())
        rec["ingest_ts_ns"] = ingest_ts_ns

        origin = rec.get("origin") or {}
        origin_uuid = origin.get("uuid") if isinstance(origin.get("uuid"), str) else "UNKNOWN"
        origin_ts_ns = int(origin.get("ts_ns") or 0)

        l_type = rec.get("l_type") if isinstance(rec.get("l_type"), str) else "UNKNOWN"
        level = rec.get("level") if isinstance(rec.get("level"), str) else "UNKNOWN"

        line_no = self._global_line

        # byte offset BEFORE writing the line
        byte_off = self._active_fp.tell()

        b = json.dumps(rec, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n"
        self._active_fp.write(b)

        # index row
        self.index.insert_record(
            line=line_no,
            uuid=origin_uuid,
            ts_ns=origin_ts_ns,
            ingest_ts_ns=ingest_ts_ns,
            l_type=l_type,
            level=level,
            segment_path=self._active_name,
            offset=byte_off,
        )

        self._global_line += 1
        self.index.set_next_line(self._global_line)

        # ---- periodic commit + segment progress ----
        now_mon = time.monotonic()

        # update the segments table so metadata stays current
        if (now_mon - self._last_seg_update_mon) >= self.segment_update_interval_s:
            # This is safe to call repeatedly; it’s just an UPDATE.
            self.index.finalize_segment(path=self._active_name, end_line=self._global_line, end_ts_ns=time.time_ns())
            self._last_seg_update_mon = now_mon

            # Optional: flush file so “tailing” sees bytes quickly
            try:
                self._active_fp.flush()
            except Exception:
                pass

        # commit at least every commit_interval_s, and also every N lines for throughput
        if (now_mon - self._last_commit_mon) >= self.commit_interval_s or (self._global_line % 200 == 0):
            self.index.conn.commit()
            self._last_commit_mon = now_mon

        return line_no
