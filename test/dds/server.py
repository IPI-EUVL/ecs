import time

import ipi_ecs.dds.server as server

my_server = server.get_server("0.0.0.0", None)
my_server.start()

time.sleep(1)
while my_server.ok():
    time.sleep(1)