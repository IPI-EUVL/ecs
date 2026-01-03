import argparse
import time
import uuid

import ipi_ecs.core.tcp as tcp
from ipi_ecs.dds.server import get_server
from ipi_ecs.logging.client import LogClient

def cmd_server(args: argparse.Namespace) -> int:
    sock = tcp.TCPClientSocket()

    sock.connect(("127.0.0.1", 11751))
    sock.start()

    logger = LogClient(sock, origin_uuid=uuid.UUID(bytes=bytes(16)))

    m_server = get_server(args.host, args.port, logger)
    m_server.start()

    time.sleep(0.1)

    try:
        while m_server.ok():
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        m_server.close()
        sock.close()

    return 0