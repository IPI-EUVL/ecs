import sys
import queue
import time
# caution: path[0] is reserved for script path (or '' in REPL)

sys.path.insert(1, './lib')
import ecs.lib.tcp as tcp

client = tcp.TCPClientSocket()
client.connect(("127.0.0.1", 11750))
client.start()
while not client.ok():
    time.sleep(0.1)

while client.ok():
    while client.empty():
        time.sleep(1)
    
    while not client.empty():
        m = client.get()
        print(time.time(), m)

    client.put(b"testingaa")
