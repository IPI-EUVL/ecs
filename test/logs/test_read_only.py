from __future__ import annotations

from pathlib import Path

import pytest

from ipi_ecs.logging.index import SQLiteIndex
from ipi_ecs.logging.viewer import LogViewer, QueryOptions


def _file_signatures(root: Path) -> dict[Path, tuple[int, int]]:
    return {
        path.relative_to(root): (path.stat().st_mtime_ns, path.stat().st_size)
        for path in root.rglob("*")
        if path.is_file()
    }


def test_read_only_log_viewer_queries_existing_current_archive_without_writes(tmp_path: Path) -> None:
    current = tmp_path / "current"
    writer = SQLiteIndex(current / "index.sqlite3")
    try:
        writer.set_next_line(12)
        before = _file_signatures(tmp_path)

        viewer = LogViewer(tmp_path, read_only=True)
        view = viewer.open_archive("current")
        try:
            assert view.next_line() == 12
            assert view.query(QueryOptions()) == []
        finally:
            view.close()

        assert viewer.current_archive_info().end_line_exclusive == 12
        assert _file_signatures(tmp_path) == before
    finally:
        writer.close()


def test_read_only_log_viewer_queries_archived_logs_without_creating_wal_sidecars(tmp_path: Path) -> None:
    archive = tmp_path / "archives" / "run-001"
    writer = SQLiteIndex(archive / "index.sqlite3")
    writer.set_next_line(12)
    writer.close()
    before = _file_signatures(tmp_path)

    view = LogViewer(tmp_path, read_only=True).open_archive("run-001")
    try:
        assert view.next_line() == 12
        assert view.query(QueryOptions()) == []
    finally:
        view.close()

    archives = LogViewer(tmp_path, read_only=True).list_archives()

    assert _file_signatures(tmp_path) == before
    assert [(archive.name, archive.end_line_exclusive) for archive in archives] == [("run-001", 12)]


def test_read_only_log_viewer_does_not_create_missing_current_archive(tmp_path: Path) -> None:
    viewer = LogViewer(tmp_path, read_only=True)

    with pytest.raises(FileNotFoundError, match="Log index does not exist"):
        viewer.open_archive("current")

    assert not (tmp_path / "current").exists()