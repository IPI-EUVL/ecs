from __future__ import annotations

import multiprocessing
import queue
import uuid
from pathlib import Path

import pytest

import ipi_ecs.db.db_library as db_library


def _concurrent_entry_write(
    data_path: str,
    entry_uuid: str,
    operation: str,
    ready,
    start,
    results,
) -> None:
    library = None
    try:
        library = db_library.Library(data_path)
        entry = library.read_entry(uuid.UUID(entry_uuid))
        ready.put(operation)
        if not start.wait(timeout=10.0):
            raise TimeoutError("Concurrent DB test start signal was not released.")
        if operation.startswith("resource:"):
            filename = operation.partition(":")[2]
            with entry.resource(filename, "concurrent_data", "w") as resource:
                resource.write(filename)
        elif operation == "name":
            entry.set_name("Concurrent name")
        elif operation == "description":
            entry.set_desc("Concurrent description")
        elif operation == "tag":
            entry.set_tag("concurrent_tag", "written")
        else:
            raise ValueError(f"Unknown concurrent DB test operation {operation!r}.")
    except Exception as exc:
        results.put((operation, f"{type(exc).__name__}: {exc}"))
    else:
        results.put((operation, None))
    finally:
        if library is not None:
            library.close()


def _run_concurrent_entry_writes(tmp_path: Path, entry_uuid: uuid.UUID, operations: tuple[str, ...]) -> None:
    context = multiprocessing.get_context("spawn")
    ready = context.Queue()
    start = context.Event()
    results = context.Queue()
    processes = [
        context.Process(
            target=_concurrent_entry_write,
            args=(str(tmp_path), str(entry_uuid), operation, ready, start, results),
        )
        for operation in operations
    ]
    try:
        for process in processes:
            process.start()
        assert {ready.get(timeout=10.0) for _process in processes} == set(operations)
        start.set()
        outcomes = dict(results.get(timeout=10.0) for _process in processes)
        for process in processes:
            process.join(timeout=10.0)
        assert outcomes == {operation: None for operation in operations}
        assert all(process.exitcode == 0 for process in processes)
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=2.0)
        for process_queue in (ready, results):
            try:
                while True:
                    process_queue.get_nowait()
            except queue.Empty:
                pass
            process_queue.close()


def _create_entries(library: db_library.Library, count: int) -> list[db_library.Entry]:
    entries = []
    for index in range(count):
        entry = library.create_entry(f"Run {index}", f"Description {index}")
        entry.set_tag("experiment", "exposure")
        entry.set_tag("dose", float(index))
        entries.append(entry)
    return entries


def _ids(entries: list[db_library.Entry]) -> list[uuid.UUID]:
    return [entry.get_uuid() for entry in entries]


def test_query_supports_stable_offset_cursor_and_count(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(db_library.time, "time", lambda: 1_700_000_000)
    library = db_library.Library(str(tmp_path))
    entries = _create_entries(library, 7)
    expected = sorted(entries, key=lambda entry: str(entry.get_uuid()), reverse=True)

    assert library.count({"tags": {"experiment": "exposure"}}) == 7
    assert library.count({"tags": {"dose": {"min": 3.0}}}) == 4
    assert _ids(library.query({}, limit=3)) == _ids(expected[:3])
    assert _ids(library.query({}, limit=3, offset=3)) == _ids(expected[3:6])

    cursor_entry = expected[2]
    cursor = (cursor_entry.get_timestamp(), cursor_entry.get_uuid())
    assert _ids(library.query({}, limit=3, cursor=cursor)) == _ids(expected[3:6])

    with pytest.raises(ValueError, match="offset"):
        library.query({}, limit=2, offset=1, cursor=cursor)
    with pytest.raises(ValueError, match="offset"):
        library.query({}, limit=2, offset=-1)
    with pytest.raises(ValueError, match="cursor"):
        library.query({}, limit=2, cursor=(1_700_000_000, "not-a-uuid"))

    library.close()


def test_read_only_library_rejects_mutation_and_preserves_files(tmp_path: Path) -> None:
    writer = db_library.Library(str(tmp_path))
    entry = writer.create_entry("Read only", "Fixture")
    entry.set_tag("experiment", "exposure")
    with entry.resource("payload.txt", "data", "w") as resource:
        resource.write("payload")
    folder = tmp_path / entry.get_foldername()
    (folder / "unregistered.txt").write_text("unregistered", encoding="utf-8")
    writer.close()

    db_path = tmp_path / "library.sqlite3"
    registry_path = folder / "registry.dat"
    db_before = db_path.read_bytes()
    registry_before = registry_path.read_bytes()
    db_mtime_before = db_path.stat().st_mtime_ns
    registry_mtime_before = registry_path.stat().st_mtime_ns

    library = db_library.Library(str(tmp_path), read_only=True)
    loaded = library.query({"tags": {"experiment": "exposure"}}, limit=1)[0]
    assert loaded.get_name() == "Read only"
    assert loaded.get_tags()["experiment"] == "exposure"
    with loaded.resource("payload.txt", "data", "r") as resource:
        assert resource.read() == "payload"
    with loaded.resource("unregistered.txt", "data", "r") as resource:
        assert resource.read() == "unregistered"

    mutators = (
        lambda: library.create_entry("No", "Write"),
        lambda: library.update(loaded),
        lambda: loaded.set_name("Changed"),
        lambda: loaded.set_desc("Changed"),
        lambda: loaded.set_tag("new", "value"),
        lambda: loaded.add_tag("new"),
        lambda: loaded.remove_tag("experiment"),
        lambda: loaded.resource("payload.txt", "data", "w"),
        lambda: loaded.resource("payload.txt", "data", "r+"),
    )
    for mutate in mutators:
        with pytest.raises(PermissionError, match="read-only"):
            mutate()

    library.close()

    assert db_path.read_bytes() == db_before
    assert registry_path.read_bytes() == registry_before
    assert db_path.stat().st_mtime_ns == db_mtime_before
    assert registry_path.stat().st_mtime_ns == registry_mtime_before


def test_read_only_library_requires_an_existing_index(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="library.sqlite3"):
        db_library.Library(str(tmp_path), read_only=True)


def test_reading_unregistered_resource_does_not_modify_registry_in_rw_mode(tmp_path: Path) -> None:
    library = db_library.Library(str(tmp_path))
    entry = library.create_entry("Reader", "Fixture")
    folder = tmp_path / entry.get_foldername()
    resource_path = folder / "external.txt"
    resource_path.write_text("external", encoding="utf-8")
    registry_path = folder / "registry.dat"
    registry_before = registry_path.read_bytes()

    with entry.resource("external.txt", "external", "r") as resource:
        assert resource.read() == "external"

    assert registry_path.read_bytes() == registry_before
    assert "external.txt" not in dict(entry.list_resources())
    library.close()


def test_register_existing_resource_registers_only_a_published_local_file(tmp_path: Path) -> None:
    library = db_library.Library(str(tmp_path))
    entry = library.create_entry("Publisher", "Fixture")
    folder = tmp_path / entry.get_foldername()
    published = folder / "published.h5"
    published.write_bytes(b"complete artifact")

    entry.register_existing_resource("published.h5", "exposure_graph")

    assert dict(entry.list_resources()) == {"published.h5": "exposure_graph"}
    with pytest.raises(FileNotFoundError, match="missing resource"):
        entry.register_existing_resource("missing.h5", "exposure_graph")
    with pytest.raises(ValueError, match="local filename"):
        entry.register_existing_resource("../published.h5", "exposure_graph")
    library.close()


def test_registry_write_merges_resources_from_other_entry_instances(tmp_path: Path) -> None:
    controller_library = db_library.Library(str(tmp_path))
    controller_entry = controller_library.create_entry("Run", "Fixture")
    with controller_entry.resource("run.json", "run_state", "w") as resource:
        resource.write("{}")
    with controller_entry.resource("metadata.json", "metadata", "w") as resource:
        resource.write("{}")

    snapshot_library = db_library.Library(str(tmp_path))
    snapshot_entry = snapshot_library.read_entry(controller_entry.get_uuid())
    with snapshot_entry.resource("snap_test.npz", "snapshot", "wb") as resource:
        resource.write(b"snapshot")
    with snapshot_entry.resource("snap_test.json", "snap_meta", "w") as resource:
        resource.write("{}")

    with controller_entry.resource("end_metadata.json", "metadata", "w") as resource:
        resource.write("{}")

    resources = dict(controller_library.read_entry(controller_entry.get_uuid()).list_resources())
    assert resources == {
        "run.json": "run_state",
        "metadata.json": "metadata",
        "snap_test.npz": "snapshot",
        "snap_test.json": "snap_meta",
        "end_metadata.json": "metadata",
    }

    snapshot_library.close()
    controller_library.close()


def test_tag_writes_merge_changes_from_stale_entry_instances(tmp_path: Path) -> None:
    controller_library = db_library.Library(str(tmp_path))
    controller_entry = controller_library.create_entry("Run", "Fixture")
    controller_entry.set_tag("experiment", "exposure")
    controller_entry.set_tag("run", "run-id")

    analysis_library = db_library.Library(str(tmp_path))
    analysis_entry = analysis_library.read_entry(controller_entry.get_uuid())

    controller_entry.set_tag("status", "STOPPED")
    analysis_entry.set_tag("dose", 12.5)

    controller_entry.remove_tag("status")
    tags = controller_library.read_entry(controller_entry.get_uuid()).get_tags()

    assert tags == {
        "experiment": "exposure",
        "run": "run-id",
        "dose": "12.5",
    }

    analysis_library.close()
    controller_library.close()


def test_overlapping_processes_preserve_distinct_resource_registrations(tmp_path: Path) -> None:
    library = db_library.Library(str(tmp_path))
    entry = library.create_entry("Concurrent resources", "Fixture")
    entry_uuid = entry.get_uuid()
    library.close()
    operations = tuple(f"resource:resource_{index}.txt" for index in range(4))

    _run_concurrent_entry_writes(tmp_path, entry_uuid, operations)

    reader = db_library.Library(str(tmp_path), read_only=True)
    try:
        loaded = reader.read_entry(entry_uuid)
        resources = dict(loaded.list_resources())
        assert resources == {
            f"resource_{index}.txt": "concurrent_data"
            for index in range(4)
        }
        folder = tmp_path / loaded.get_foldername()
        assert all((folder / filename).read_text(encoding="utf-8") == filename for filename in resources)
        assert (folder / "registry.dat").stat().st_size > 0
    finally:
        reader.close()


def test_overlapping_processes_preserve_independent_metadata_and_tag_changes(tmp_path: Path) -> None:
    library = db_library.Library(str(tmp_path))
    entry = library.create_entry("Original name", "Original description")
    entry.set_tag("existing", "kept")
    entry_uuid = entry.get_uuid()
    library.close()

    _run_concurrent_entry_writes(tmp_path, entry_uuid, ("name", "description", "tag"))

    reader = db_library.Library(str(tmp_path), read_only=True)
    try:
        loaded = reader.read_entry(entry_uuid)
        assert loaded.get_name() == "Concurrent name"
        assert loaded.get_description() == "Concurrent description"
        assert loaded.get_tags() == {"existing": "kept", "concurrent_tag": "written"}
        loaded.list_resources()
        assert loaded.get_name() == "Concurrent name"
        assert loaded.get_description() == "Concurrent description"
    finally:
        reader.close()