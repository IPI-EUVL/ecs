import time

import ipi_ecs.dds.server as server

server = server.DDSServer()
server.start()

time.sleep(1)
while server.ok():
    time.sleep(1)