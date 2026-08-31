import queue

from ipi_ecs.core import tcp


class _OnePassStopFlag:
    def __init__(self) -> None:
        self._first = True

    def run(self) -> bool:
        if not self._first:
            return False
        self._first = False
        return True


class _SingleFrameQueue:
    def __init__(self, frame: bytes) -> None:
        self._frame = frame

    def empty(self) -> bool:
        return self._frame is None

    def get(self, timeout: float) -> bytes:
        del timeout
        if self._frame is None:
            raise queue.Empty
        frame = self._frame
        self._frame = None
        return frame


class _PartialWriteSocket:
    def __init__(self) -> None:
        self.sent = bytearray()

    def send(self, frame: bytes) -> int:
        accepted = frame[:4]
        self.sent.extend(accepted)
        return len(accepted)

    def sendall(self, frame: bytes) -> None:
        self.sent.extend(frame)


def test_tcp_sender_writes_the_complete_queued_frame() -> None:
    frame = b"large-dds-frame" * 100
    transport = tcp.TCPSocket()
    socket = _PartialWriteSocket()
    transport._socket = socket
    transport._send_queue = _SingleFrameQueue(frame)
    transport._TCPSocket__valid = lambda: True

    transport._TCPSocket__send_thread(_OnePassStopFlag())

    assert bytes(socket.sent) == frame