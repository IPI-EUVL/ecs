from __future__ import annotations

import json
import uuid

import pytest

from ipi_ecs.db.db_library import Library
from ipi_ecs.subsystems.experiment_controller import (
    AutomationLease,
    RunRecord,
    RunSettings,
    RunState,
    decode_prepare_run_tags,
    encode_prepare_run_tags,
)


class _Logger:
    def begin_event(self, *_args, **_kwargs):
        return str(uuid.uuid4())


class _Controller:
    name = "Test Controller"


def test_prepare_run_tags_are_versioned_and_legacy_empty_payload_stays_valid() -> None:
    batch_uuid = uuid.uuid4()
    payload = encode_prepare_run_tags({"batch_uuid": str(batch_uuid), "attempt": 2})

    assert decode_prepare_run_tags(b"") == {}
    assert decode_prepare_run_tags(payload) == {
        "batch_uuid": str(batch_uuid),
        "attempt": 2,
    }


def test_prepare_run_tags_reject_reserved_and_non_scalar_values() -> None:
    with pytest.raises(ValueError, match="reserved"):
        decode_prepare_run_tags(encode_prepare_run_tags({"sample": "2"}), {"sample"})
    with pytest.raises(ValueError, match="string or finite number"):
        encode_prepare_run_tags({"batch_uuid": True})


def test_run_record_creation_persists_prepare_tags(tmp_path) -> None:
    batch_uuid = uuid.uuid4()
    run_uuid = uuid.uuid4()
    settings = RunSettings()
    settings.set_attr("name", "Batch run")
    state = RunState("exposure", settings, s_uuid=run_uuid)
    library = Library(str(tmp_path))
    try:
        record = RunRecord.create(
            _Logger(),
            library,
            state,
            settings,
            _Controller(),
            {"batch_uuid": str(batch_uuid)},
        )
        tags = record.get_tags()
    finally:
        library.close()

    assert tags["run"] == run_uuid.hex
    assert tags["batch_uuid"] == str(batch_uuid)


def test_automation_lease_enforces_single_owner_and_owner_only_starts() -> None:
    lease = AutomationLease()
    queue_uuid = uuid.uuid4()
    batch_uuid = uuid.uuid4()

    assert lease.acquire(queue_uuid, "Exposure Queue Controller", 100.0)[0] is True
    assert lease.can_start(queue_uuid)[0] is True
    allowed, reason = lease.can_start(batch_uuid)
    assert allowed is False
    assert "Exposure Queue Controller" in reason
    assert lease.acquire(batch_uuid, "Exposure Batch Controller", 101.0)[0] is False
    assert lease.release(batch_uuid)[0] is False
    assert lease.release(queue_uuid)[0] is True
    assert lease.acquire(batch_uuid, "Exposure Batch Controller", 102.0)[0] is True


def test_automation_lease_encodes_public_owner_state() -> None:
    lease = AutomationLease()
    owner_uuid = uuid.uuid4()
    lease.acquire(owner_uuid, "Exposure Batch Controller", 100.0)

    payload = json.loads(lease.encode())

    assert payload == {
        "schema_version": 1,
        "owner_uuid": str(owner_uuid),
        "owner_name": "Exposure Batch Controller",
        "acquired_at": 100.0,
    }


def test_automation_lease_rejects_non_owner_or_active_run_settings_writes() -> None:
    lease = AutomationLease()
    batch_uuid = uuid.uuid4()
    gui_uuid = uuid.uuid4()
    lease.acquire(batch_uuid, "Exposure Batch Controller", 100.0)

    allowed, reason = lease.can_update_settings(gui_uuid, run_active=False)
    assert allowed is False
    assert "Exposure Batch Controller" in reason

    allowed, reason = lease.can_update_settings(batch_uuid, run_active=True)
    assert allowed is False
    assert "active" in reason

    allowed, _reason = lease.can_update_settings(batch_uuid, run_active=False)
    assert allowed is True