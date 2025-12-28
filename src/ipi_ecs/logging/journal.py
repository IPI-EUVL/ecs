from __future__ import annotations

import json
import os
import time
import uuid

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from platformdirs import user_log_dir


def resolve_log_dir(cli_log_dir: Path | None, env_var: str) -> Path:
    if cli_log_dir is not None:
        return cli_log_dir
    env = os.getenv(env_var)
    if env:
        return Path(env)
    return Path(user_log_dir(appname="ipi-ecs", appauthor="IPI", ensure_exists=True))


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
    ):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

        self.rotate_max_bytes = rotate_max_bytes
        self.rotate_max_seconds = rotate_max_seconds
        self.index_every_lines = index_every_lines

        self.session_id = session_id or str(uuid.uuid4())
        self.service_name = service_name

        self._manifest_path = self.root / "manifest.json"
        self._segments: list[SegmentInfo] = []
        self._global_line = 0

        self._active_fp = None
        self._active_idx_fp = None
        self._active_started_ns = 0
        self._active_seg: SegmentInfo | None = None

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

    def append(self, record: dict[str, Any]) -> int:
        if self._active_fp is None or self._should_rotate():
            self._open_new_segment()

        rec = dict(record)
        rec.setdefault("ingest_ts_ns", time.time_ns())

        line_no = self._global_line

        # index checkpoint: store byte offset at the START of the line
        if (
            self._active_idx_fp is not None
            and self.index_every_lines > 0
            and self._active_seg is not None
        ):
            if (line_no - self._active_seg.start_line) % self.index_every_lines == 0:
                off = self._active_fp.tell()
                self._active_idx_fp.write(f"{line_no}\t{off}\n".encode("ascii"))

        b = json.dumps(rec, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n"
        self._active_fp.write(b)

        self._global_line += 1

        # Persist manifest occasionally; rotation always persists immediately anyway.
        if self._global_line % 2000 == 0:
            self._save_manifest()

        return line_no
