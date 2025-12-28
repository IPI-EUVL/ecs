import time

import ecs.control.server as server

server = server.ControlServer()
server.start()

time.sleep(1)
while server.ok():
    time.sleep(1)