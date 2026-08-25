from ipi_ecs.subsystems.experiment_client import ExperimentClient


def test_stop_feedback_renews_an_active_terminal_event() -> None:
    class _Handle:
        def __init__(self) -> None:
            self.feedback_messages = []

        def feedback(self, message: bytes) -> None:
            self.feedback_messages.append(message)

    client = object.__new__(ExperimentClient)
    handle = _Handle()
    client._ExperimentClient__stop_handle = handle

    client._on_stop_feedback(b"Still reconciling capture artifacts.")

    assert handle.feedback_messages == [b"Still reconciling capture artifacts."]