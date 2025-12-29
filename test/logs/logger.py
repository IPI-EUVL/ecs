import time

import ipi_ecs.core.tcp as tcp
import ipi_ecs.logging.client as client

sock = tcp.TCPClientSocket()
sock.connect(("127.0.0.1", 11751))
sock.start()

time.sleep(0.1)

c = client.LogClient(sock)
i = 0
while True:
    c.log(f"mewssage, this is {i}")
    print(f"mewssage, this is {i}")
    i+=1
    time.sleep(1)
