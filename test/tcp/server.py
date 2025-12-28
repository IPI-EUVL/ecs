import sys
import queue
import time
# Need to do this stupid thing to add lib to pythonpath
#sys.path.insert(1, './lib')

import ecs.lib.tcp as tcp

client_q = queue.Queue()

server = tcp.TCPServer(("127.0.0.1", 11750), client_q)
server.start()

while client_q.empty():
    time.sleep(1)

c = client_q.get()

while server.ok():
    time.sleep(1)
    c.put(b"TESTING")
    c.put(b"TESTING123445342432423")
    c.put(b"\x00\x00\x00\x00") # Test ability to send NULL char which is used internally as a terminator
    c.put(b"\x00\xff\x01\x00") # Test ability to send FF01 which is used internally as an escape sequence
    c.put(b"\x05") # Test ability to send FF01 which is used internally as an escape sequence

    while not c.empty():
        print(c.get())