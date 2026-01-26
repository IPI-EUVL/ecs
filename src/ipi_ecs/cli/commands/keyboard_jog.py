import argparse
import sys
import time
import uuid
from typing import Dict, Iterable, Set

import mt_events
import segment_bytes

import ipi_ecs.core.tcp as tcp
import ipi_ecs.dds.client as client
import ipi_ecs.dds.magics as magics
import ipi_ecs.dds.subsystem as subsystem
import ipi_ecs.dds.types as types
from ipi_ecs.logging.client import LogClient


IS_WINDOWS = sys.platform.startswith("win")


class KeyPoller:
    """Cross-platform non-blocking key poller using stdlib only.

    Windows: msvcrt.kbhit/getwch
    Posix: curses in non-blocking mode
    """

    def __init__(self, release_timeout: float = 0.35):
        self.release_timeout = release_timeout
        self._pressed: Set[str] = set()
        self._last_seen: Dict[str, float] = {}
        self._stdscr = None
        self._curses = None

    def __enter__(self):
        if not IS_WINDOWS:
            import curses

            self._curses = curses
            self._stdscr = curses.initscr()
            curses.noecho()
            curses.cbreak()
            self._stdscr.nodelay(True)
            self._stdscr.keypad(True)
        return self

    def __exit__(self, exc_type, exc, tb):
        if not IS_WINDOWS and self._stdscr is not None:
            curses = self._curses
            curses.nocbreak()
            self._stdscr.keypad(False)
            curses.echo()
            curses.endwin()

    @property
    def pressed(self) -> Set[str]:
        return set(self._pressed)

    def _handle_key(self, key: str, ts: float) -> None:
        if not key:
            return
        key = key.lower()
        self._pressed.add(key)
        self._last_seen[key] = ts

    def poll(self) -> None:
        now = time.monotonic()

        if IS_WINDOWS:
            import msvcrt

            while msvcrt.kbhit():
                ch = msvcrt.getwch()
                if ch == "\x03":  # Ctrl-C
                    raise KeyboardInterrupt
                self._handle_key(ch, now)
        else:
            while True:
                ch = self._stdscr.getch()
                if ch == -1:
                    break
                if ch == 3:  # Ctrl-C
                    raise KeyboardInterrupt
                if 0 <= ch < 256:
                    self._handle_key(chr(ch), now)

        # Infer releases when a key stops repeating for longer than release_timeout.
        for key, ts in list(self._last_seen.items()):
            if now - ts > self.release_timeout:
                self._pressed.discard(key)
                self._last_seen.pop(key, None)


def vector_from_keys(keys: Iterable[str]) -> tuple[float, float]:
    x = 0.0
    y = 0.0
    ks = {k.lower() for k in keys}
    if "a" in ks:
        x -= 1.0
    if "d" in ks:
        x += 1.0
    if "w" in ks:
        y += 1.0
    if "s" in ks:
        y -= 1.0
    return x, y

class JogWriteClient:
    def __init__(self, t_name, target__uuid, key):
        self.__target = uuid.UUID(target__uuid) if target__uuid is not None else None
        self.__t_name = t_name.encode("utf-8") if t_name is not None else None
        self.__key = key.encode("utf-8")
        self.__run = True

        self.__remote_kv = None

        c_uuid = uuid.uuid4()

        self.__logger_sock = tcp.TCPClientSocket()

        self.__logger_sock.connect(("127.0.0.1", 11751))
        self.__logger_sock.start()

        self.__logger = LogClient(self.__logger_sock, origin_uuid=c_uuid)

        self.__did_config = False
        self.__subsystem = None

        def _on_ready():
            if self.__did_config:
                return
            
            self.__did_config = True
            m_uuid = uuid.uuid4()
            sh = self.__client.register_subsystem(f"__cli_{m_uuid}", m_uuid, temporary=True)

            self.__on_got_subsystem(sh)

        #print("Registering subsystem...")
        self.__client = client.DDSClient(c_uuid, logger=self.__logger)
        self.__client.when_ready().then(_on_ready)
        

    def __on_got_subsystem(self, handle: client._RegisteredSubsystemHandle):
        # state has to be named state as it will get passed as a KW argument, stop complaining Pylint!!
        def fail(state, reason): #pylint: disable=unused-argument
            print(f"Failed: {reason}")
            self.__run = False

        def __setup_kv(s_uuid):
            handle.get_subsystem(s_uuid).then(lambda subsystem: subsystem.get_kv(self.__key).then(self.__on_got_kv).catch(fail)).catch(fail)

        #pylint: disable=pointless-string-statement

        self.__subsystem = handle

        if self.__target is not None:
            __setup_kv(self.__target)
        else:
            self.__client.resolve(self.__t_name).then(__setup_kv).catch(fail)

    def __on_got_kv(self, value: client._RemoteProperty._PropertyHandler):
        self.__remote_kv = value
        self.__remote_kv.set_type(types.VectorTypeSpecifier(types.FloatTypeSpecifier(), 2))

    def ok(self):
        return self.__run and self.__client.ok()
    
    def close(self):
        self.__client.close()
        self.__logger_sock.close()

        self.__run = False

    def set_xy(self, x, y):
        if self.__remote_kv is None:
            return False

        self.__remote_kv.value = [x, y]
        return True


def main(args: argparse.Namespace):
    m_client = JogWriteClient(args.name, args.sys, args.key)

    hz = args.hz if args.hz is not None else 2.0

    try:
        with KeyPoller() as keys:
            keys.release_timeout = 0.1
            while m_client.ok():
                start_time = time.monotonic()

                keys.poll()
                vec = vector_from_keys(keys.pressed)
                m_client.set_xy(*vec)

                print(f"Keys: {sorted(keys.pressed)} -> Vector: {vec}")

                elapsed = time.monotonic() - start_time
                to_wait = (1.0 / hz) - elapsed
                if to_wait > 0.0:
                    time.sleep(to_wait)

    except KeyboardInterrupt:
        pass
    finally:
        m_client.close()

    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Keyboard jog command line tool.")
    parser.add_argument("--name", "-n", type=str, required=False, default=None, help="Target subsystem name to connect to.")
    parser.add_argument("--sys", "-s", type=str, required=False, default=None, help="Target subsystem UUID to connect to. If both name and sys are given, sys takes precedence.")
    parser.add_argument("--key", "-k", type=str, required=False, default="target_jog_vector", help="Key value to write the jog vector to.")
    parser.add_argument("--hz", "-f", type=float, required=False, default=None, help="Frequency to poll the keyboard and write jog values at. Default 2Hz.")

    args = parser.parse_args()

    sys.exit(main(args))