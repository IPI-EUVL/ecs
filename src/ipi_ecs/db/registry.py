from __future__ import annotations

import os
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator

import portalocker


REGISTRY_FILENAME = "registry.dat"
REGISTRY_LOCK_FILENAME = ".registry.dat.lock"
DEFAULT_LOCK_TIMEOUT_SECONDS = 10.0
DEFAULT_REPLACE_ATTEMPTS = 5
DEFAULT_REPLACE_DELAY_SECONDS = 0.05


@dataclass(frozen=True)
class RegistryContents:
    entry_uuid: uuid.UUID
    name: str
    created: int
    description: str
    resources: dict[str, str]


_path_locks_guard = threading.Lock()
_path_locks: dict[Path, threading.RLock] = {}


def _path_lock(path: Path) -> threading.RLock:
    normalized = path.resolve()
    with _path_locks_guard:
        return _path_locks.setdefault(normalized, threading.RLock())


def registry_lock_path(registry_path: str | Path) -> Path:
    path = Path(registry_path)
    return path.with_name(REGISTRY_LOCK_FILENAME)


@contextmanager
def registry_lock(
    registry_path: str | Path,
    *,
    timeout: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
) -> Iterator[None]:
    path = Path(registry_path)
    if timeout < 0:
        raise ValueError("Registry lock timeout must be non-negative.")
    path.parent.mkdir(parents=True, exist_ok=True)
    with _path_lock(path):
        with portalocker.Lock(
            registry_lock_path(path),
            mode="a+b",
            timeout=timeout,
            flags=portalocker.LockFlags.EXCLUSIVE | portalocker.LockFlags.NON_BLOCKING,
        ):
            yield


def parse_registry(contents: bytes, path: str | Path = REGISTRY_FILENAME) -> RegistryContents:
    registry_path = Path(path)
    try:
        lines = contents.decode("utf-8").splitlines()
        if len(lines) < 4:
            raise ValueError("Registry metadata is incomplete")
        entry_uuid = uuid.UUID(lines[0].strip())
        created = int(lines[2].strip())
        resources: dict[str, str] = {}
        for line_number, line in enumerate(lines[4:], start=5):
            parts = line.split(":")
            if len(parts) != 2 or not parts[0] or not parts[1]:
                raise ValueError(f"Malformed registry resource on line {line_number}")
            resources[parts[0]] = parts[1]
    except (UnicodeError, ValueError) as exc:
        raise ValueError(f"Invalid registry {registry_path}: {exc}") from exc
    return RegistryContents(
        entry_uuid=entry_uuid,
        name=lines[1],
        created=created,
        description=lines[3],
        resources=resources,
    )


def read_registry(path: str | Path) -> RegistryContents:
    registry_path = Path(path)
    try:
        contents = registry_path.read_bytes()
    except OSError as exc:
        raise ValueError(f"Invalid registry {registry_path}: {exc}") from exc
    return parse_registry(contents, registry_path)


def serialize_registry(registry: RegistryContents) -> bytes:
    header = (
        str(registry.entry_uuid),
        registry.name,
        str(registry.created),
        registry.description,
    )
    if any("\n" in value or "\r" in value for value in header):
        raise ValueError("Registry metadata cannot contain newlines.")
    lines = list(header)
    for filename, resource_type in registry.resources.items():
        if not filename or not resource_type:
            raise ValueError("Registry resource names and types cannot be empty.")
        if any(character in filename or character in resource_type for character in ("\n", "\r", ":")):
            raise ValueError("Registry resource names and types cannot contain newlines or colons.")
        lines.append(f"{filename}:{resource_type}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def is_registry_artifact(filename: str) -> bool:
    return (
        filename == REGISTRY_FILENAME
        or filename == REGISTRY_LOCK_FILENAME
        or filename.startswith(f"{REGISTRY_FILENAME}.")
        or filename.startswith(f".{REGISTRY_FILENAME}.")
    )


def _replace_with_retry(
    temporary_path: Path,
    destination: Path,
    *,
    attempts: int,
    delay: float,
) -> None:
    if attempts < 1 or delay < 0:
        raise ValueError("Registry replacement retry settings are invalid.")
    for attempt in range(1, attempts + 1):
        try:
            os.replace(temporary_path, destination)
            return
        except OSError:
            if attempt == attempts:
                raise
            if delay:
                time.sleep(delay)


def _write_registry_atomic(
    registry_path: Path,
    registry: RegistryContents,
    *,
    replace_attempts: int = DEFAULT_REPLACE_ATTEMPTS,
    replace_delay: float = DEFAULT_REPLACE_DELAY_SECONDS,
) -> None:
    temporary_path = registry_path.with_name(f".{registry_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary_path.open("xb") as temporary:
            temporary.write(serialize_registry(registry))
            temporary.flush()
            os.fsync(temporary.fileno())
        _replace_with_retry(
            temporary_path,
            registry_path,
            attempts=replace_attempts,
            delay=replace_delay,
        )
    finally:
        temporary_path.unlink(missing_ok=True)


def update_registry(
    registry_path: str | Path,
    *,
    entry_uuid: uuid.UUID,
    initial_name: str,
    initial_created: int,
    initial_description: str,
    name: str | None = None,
    description: str | None = None,
    resources: dict[str, str] | None = None,
    timeout: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
) -> RegistryContents:
    path = Path(registry_path)
    with registry_lock(path, timeout=timeout):
        registry_exists = path.exists()
        if registry_exists:
            current = read_registry(path)
            if current.entry_uuid != entry_uuid:
                raise ValueError("Registry UUID does not match entry UUID.")
        else:
            current = RegistryContents(
                entry_uuid=entry_uuid,
                name=initial_name,
                created=int(initial_created),
                description=initial_description,
                resources={},
            )
        updated = RegistryContents(
            entry_uuid=current.entry_uuid,
            name=current.name if name is None else name,
            created=current.created,
            description=current.description if description is None else description,
            resources={**current.resources, **(resources or {})},
        )
        if registry_exists and updated == current:
            return current
        _write_registry_atomic(path, updated)
        return updated


def replace_registry(
    registry_path: str | Path,
    registry: RegistryContents,
    *,
    expected_contents: bytes | None,
    backup_path_factory: Callable[[Path], Path] | None = None,
    timeout: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
) -> None:
    path = Path(registry_path)
    with registry_lock(path, timeout=timeout):
        if expected_contents is None:
            if path.exists():
                raise RuntimeError(f"Registry appeared while replacing {path}")
        else:
            try:
                current_contents = path.read_bytes()
            except FileNotFoundError as exc:
                raise RuntimeError(f"Registry disappeared while replacing {path}") from exc
            if current_contents != expected_contents:
                raise RuntimeError(f"Registry changed while replacing {path}")
            if backup_path_factory is not None:
                backup_path = backup_path_factory(path)
                with backup_path.open("xb") as backup:
                    backup.write(expected_contents)
                    backup.flush()
                    os.fsync(backup.fileno())
        _write_registry_atomic(path, registry)