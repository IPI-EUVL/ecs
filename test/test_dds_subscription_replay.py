import uuid

from ipi_ecs.dds.server import _DDSServer
from ipi_ecs.dds.subsystem import SubsystemInfo


class _Client:
    def ok(self) -> bool:
        return True


def test_registering_publisher_replays_all_pending_subscriptions() -> None:
    server = object.__new__(_DDSServer)
    publisher_client_id = uuid.uuid4()
    first_subscriber_id = uuid.uuid4()
    second_subscriber_id = uuid.uuid4()
    publisher_id = uuid.uuid4()
    first_subscriber = _Client()
    second_subscriber = _Client()
    server._DDSServer__subsystems = {}
    server._DDSServer__clients = []
    server._DDSServer__clients_uuid = {
        publisher_client_id: _Client(),
        first_subscriber_id: first_subscriber,
        second_subscriber_id: second_subscriber,
    }
    server._DDSServer__pending_subscribers = [
        (first_subscriber_id, publisher_id, b"timing_status"),
        (second_subscriber_id, publisher_id, b"timing_status"),
    ]
    server._DDSServer__logger = None
    server._DDSServer__log = lambda *_args, **_kwargs: None

    assert server.register_subsystem(
        publisher_client_id,
        SubsystemInfo(publisher_id, "Laser Controller"),
    ) is True

    publisher = server.find_subsystem(s_uuid=publisher_id)
    subscribers = publisher._SubsystemClient__kv_subscribers[b"timing_status"]
    assert subscribers == [first_subscriber, second_subscriber]
    assert server._DDSServer__pending_subscribers == []