from __future__ import annotations

import bisect
import json

from pathlib import Path
from typing import Any

from ipi_ecs.logging.journal import SegmentInfo


class JournalReader:
    def __init__(self, root: Path):
        self.root = root
        self.manifest_path = root / "manifest.json"
        self.segments: list[SegmentInfo] = []
        self.next_line: int = 0
        self.refresh()

    def refresh(self) -> None:
        obj = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.segments = [SegmentInfo(**seg) for seg in obj.get("segments", [])]
        self.next_line = int(obj.get("next_line", 0))

    def _segments_for_range(self, start: int, end: int) -> list[SegmentInfo]:
        out = []
        for seg in self.segments:
            seg_end = seg.end_line if seg.end_line is not None else 10**30
            if seg.start_line < end and seg_end > start:
                out.append(seg)
        return out

    def _load_idx(self, seg: SegmentInfo) -> tuple[list[int], list[int]]:
        """
        Returns (lines, offsets). If no idx, returns empty lists.
        """
        if not seg.idx_path:
            return ([], [])
        idx_file = self.root / seg.idx_path
        if not idx_file.exists():
            return ([], [])
        lines: list[int] = []
        offs: list[int] = []
        with idx_file.open("rb") as f:
            for raw in f:
                s = raw.decode("ascii", errors="ignore").strip()
                if not s:
                    continue
                a, b = s.split("\t")
                lines.append(int(a))
                offs.append(int(b))
        return (lines, offs)

    def read_between(self, start_linenum: int, end_linenum: int) -> list[dict[str, Any]]:
        """
        Returns records with global line numbers in [start_linenum, end_linenum).
        Will see new segments after rollover because it refreshes manifest first.
        """
        self.refresh()

        if end_linenum <= start_linenum:
            return []

        records: list[dict[str, Any]] = []
        for seg in self._segments_for_range(start_linenum, end_linenum):
            path = self.root / seg.path

            want_start = max(start_linenum, seg.start_line)
            want_end = end_linenum if seg.end_line is None else min(end_linenum, seg.end_line)

            # Optional seek optimization using idx checkpoints
            idx_lines, idx_offs = self._load_idx(seg)
            seek_line = seg.start_line
            seek_off = 0
            if idx_lines:
                i = bisect.bisect_right(idx_lines, want_start) - 1
                if i >= 0:
                    seek_line = idx_lines[i]
                    seek_off = idx_offs[i]

            with path.open("rb") as f:
                if seek_off:
                    f.seek(seek_off)
                cur = seek_line
                for line in f:
                    if cur >= want_end:
                        break
                    if cur >= want_start:
                        records.append(json.loads(line.decode("utf-8")))
                    cur += 1

        return records
