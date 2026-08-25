from __future__ import annotations

import copy
import uuid
from pathlib import Path

import pytest

import ipi_ecs.db.db_library as db_library
from ipi_ecs.subsystems.experiment_controller import ExperimentReader


def _create_run(library: db_library.Library, experiment: str, dose: float) -> db_library.Entry:
    run_uuid = uuid.uuid4()
    entry = library.create_entry(f"Run {dose}", "Fixture")
    entry.set_tag("experiment", experiment)
    entry.set_tag("run", run_uuid.hex)
    entry.set_tag("dose", dose)
    return entry


def _entry_ids(runs) -> list[uuid.UUID]:
    return [run.get_record().get_uuid() for run in runs]


def test_reader_supports_read_only_count_and_pagination_without_mutating_filters(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(db_library.time, "time", lambda: 1_700_000_000)
    writer = db_library.Library(str(tmp_path))
    exposure_entries = [_create_run(writer, "exposure", float(index)) for index in range(5)]
    _create_run(writer, "calibration", 10.0)
    writer.close()

    expected = sorted(exposure_entries[1:], key=lambda entry: str(entry.get_uuid()), reverse=True)
    filters = {"tags": {"dose": {"min": 1.0}}}
    filters_before = copy.deepcopy(filters)
    reader = ExperimentReader(str(tmp_path), "exposure", read_only=True)

    assert reader.count(filters) == 4
    first_page = reader.query(filters, limit=2)
    offset_page = reader.query(filters, limit=2, offset=2)
    cursor_entry = first_page[-1].get_record()
    cursor_page = reader.query(
        filters,
        limit=2,
        cursor=(cursor_entry.get_timestamp(), cursor_entry.get_uuid()),
    )

    assert _entry_ids(first_page) == [entry.get_uuid() for entry in expected[:2]]
    assert _entry_ids(offset_page) == [entry.get_uuid() for entry in expected[2:]]
    assert _entry_ids(cursor_page) == _entry_ids(offset_page)
    assert filters == filters_before
    assert reader.query({"tags": {"experiment": "calibration"}}) != []
    with pytest.raises(PermissionError, match="read-only"):
        first_page[0].set_name("Changed")

    reader.close()


def test_list_runs_does_not_mutate_legacy_arguments(tmp_path: Path) -> None:
    writer = db_library.Library(str(tmp_path))
    _create_run(writer, "exposure", 2.0)
    writer.close()

    tags = {"dose": {"min": 1.0}}
    args = {"name": "Run"}
    reader = ExperimentReader(str(tmp_path), "exposure", read_only=True)

    assert len(reader.list_runs(tags, args, 1, offset=0)) == 1
    assert tags == {"dose": {"min": 1.0}}
    assert args == {"name": "Run"}

    reader.close()