import socket
import threading
import time
import queue
import select

import ipi_ecs.core.daemon as daemon
import ipi_ecs.core.mt_events as mt_events

SOCKET_BUFSIZE = 1024
DELIM = bytes([0x00])
ESCAPE = bytes([0xff, 0x01])

def escape_bytes(b : bytes):
    b = b.replace(ESCAPE, ESCAPE + ESCAPE)
    return b.replace(DELIM, ESCAPE + DELIM)

def unescape_bytes(b: bytes):
    #print(b)
    out = bytes()

    while len(b) > 0:
        s = b.find(ESCAPE)
        if s == -1:
            break

        out += b[:s]

        #print(s, b)

        if b[s+2:s+3] == DELIM:
            #print("FOUND DELIM")
            out += DELIM
            b = b[s+3:]
        elif b[s+2:s+4] == ESCAPE:
            #print("FOUND ESC")
            out += ESCAPE
            b = b[s+4:]
        else:
            raise ValueError("Invalid escape sequence")
        
    out += b
        
    return out

def __b_escaped(b: bytes, i: int):
    #print(b)
    if i < 0:
        return False
    
    #print(i, b.find(ESCAPE, i - 2))
    
    if b.find(ESCAPE, i - 2) != -1 and b.find(ESCAPE, i - 2) == i - 2 and not __b_escaped(b, i - 2):
        #print(i, True)
        return True
    
    return False

def sliced(b: bytes):
    s = 0
    while s != -1:
        i = b.find(DELIM, s)

        if i == -1:
            return (None, b)
        
        if __b_escaped(b, i):
            s = i + 1

            if s >= len(b):
                return (None, b)
            
            continue
        break

    #print("Slice index:", i)
    return (b[0: i], b[i + 1:])


class TCPSocket:
    """
    Common class for TCP server / client functions.
    """

    class _ReceiveQueueHandle:
        def __init__(self, q : queue.Queue):
            self.__q = q

        def get(self, *args, **argkw):
            return self.__q.get(*args, **argkw)
        
    class _SendQueueHandle:
        def __init__(self, q : queue.Queue):
            self.__q = q
            
        def put(self, *args, **argkw):
            return self.__q.put(*args, **argkw)

    def __init__(self):
        self._socket = None
        self._remote = None
        self.__connected = False

        self.__recv_queue = queue.Queue()
        self.__send_queue = queue.Queue()

        self.__last_data = 0
        self.__last_send = 0

        self.__daemon = daemon.Daemon()

        self.__daemon.add(self.__recv_thread)
        self.__daemon.add(self.__send_thread)

        self.__buffer = bytes()

        self._closed_event = mt_events.Event()
        self._connected_event = mt_events.Event()
        self._disconnected_event = mt_events.Event()

        self._received_event = mt_events.Event()

    def start(self):
        self.__daemon.start()

    def __rtr(self):
        res, _, _ = select.select([self._socket], [], [], 1)

        if not self.__valid():
            return False

        for fd in res:
            if fd == self._socket:
                return True
            
        return False
    
    def _closed(self):
        self.__connected = False
        self._closed_event.call()

    def _disconnected(self):
        self.__connected = False
        self._disconnected_event.call()

    def __valid(self):
        return self._socket is not None and self._remote is not None and self.__daemon.is_alive() and self.__connected
    
    def _reconnect(self):
        self.__connected = True
        self._connected_event.call()

    def __recv_thread(self, stop_flag : daemon.StopFlag):
        while stop_flag.run():
            if not self.__valid():
                time.sleep(0.1)
                continue

            if not self.__rtr():
                continue
            
            try:
                data = self._socket.recv(SOCKET_BUFSIZE)
            except ConnectionResetError:
                self._closed()
                continue
            except ConnectionAbortedError:
                self._closed()
                continue

            if len(data) == 0:
                self._closed()
                continue

            self.__last_data = time.time()

            if data == bytes([0x00]):
                continue

            self.__received(data)

    def __received(self, data: bytes):
        #print(f"Received {data}, buffer: {self.__buffer}")
        self.__buffer += data
        
        while True:
            chk, self.__buffer = sliced(self.__buffer)
            #print("split", chk, self.__buffer)

            if chk is not None:
                self.__recv_queue.put(unescape_bytes(chk))
                self._received_event.call()
            else:
                break

    def __send_thread(self, stop_flag : daemon.StopFlag):
        while stop_flag.run():
            if not self.__valid():

                time.sleep(0.1)
                self._reconnect()
                continue

            if time.time() - self.__last_send > 1.0 and self.__send_queue.empty():
                self.__send_queue.put(bytes([0x00]))

            while True:
                try:
                    data = self.__send_queue.get(timeout=1)
                except queue.Empty:
                    break

                self.__last_send = time.time()

                try:
                    self._socket.send(data)
                except OSError:
                    self._closed()
                    break

    def close(self):
        """
        Stop all threads
        """
        self.__daemon.stop()

        if self._socket is not None:
            self._socket.close()

    def put(self, data) -> None:
        """
        Enqueue data to send

        Args:
            data (bytes): Data to send
        """

        self.__send_queue.put(escape_bytes(data) + bytes(DELIM))

    def get(self, timeout=None, block=True) -> bytes:
        """
        Dequeue received data

        Args:
            timeout (float | None): Wait at most timeout seconds for new data
            block (bool | None): Should block for new data?
        Returns:
            bytes | None: Dequeued received data (or None if queue is empty)
        """

        res = None

        try:
            res = self.__recv_queue.get(timeout=timeout, block=block)
        except queue.Empty:
            res = None

        return res
    
    def get_recv_queue(self):
        return self._ReceiveQueueHandle(self.__recv_queue)
    
    def get_send_queue(self):
        return self._SendQueueHandle(self.__send_queue)

    def empty(self):
        """
        Returns if receive queue is empty or not

        Returns:
            bool: Is empty
        """

        return self.__recv_queue.empty()

    def remote(self):
        """
        Returns remote connection address

        Returns:
            socket._RetAddress: Remote address
        """
        return self._remote

    def last_data(self):
        """
        Returns last time any data was received from remote server
        Includes keepalive heartbeat messages.

        Returns:
            float: Last message timestamp in system time
        """

        return self.__last_data

    def last_send(self):
        """
        Returns last time any data was sent to remote server
        Includes keepalive heartbeat messages.

        Returns:
            float: Last message timestamp in system time
        """

        return self.__last_send

    def is_closed(self):
        """
        Returns if this connection has been closed
        """

        return not self.__valid()
    
    def connected(self):
        """
        Returns if connection is up
        """
        
        return self.__valid() and (time.time() - self.__last_data) < 5.0
    
    def ok(self):
        """
        Returns if connection is healthy
        """
        
        return self.__valid()
    
    def on_connect(self, consumer : mt_events.EventConsumer, event_id):
        self._connected_event.bind(consumer, event_id)

    def on_disconnect(self, consumer : mt_events.EventConsumer, event_id):
        self._disconnected_event.bind(consumer, event_id)

    def on_close(self, consumer : mt_events.EventConsumer, event_id):
        self._closed_event.bind(consumer, event_id)

    def on_receive(self, consumer : mt_events.EventConsumer, event_id):
        self._received_event.bind(consumer, event_id)


class TCPClientSocket(TCPSocket):
    def _reconnect(self):
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)

            self._socket.connect(self._remote)

            super()._reconnect()
            print(f"Client has connected to {self._remote}")
        except ConnectionRefusedError:
            pass

    def connect(self, remote):
        self._remote = remote

    def _closed(self):
        self._disconnected()
        print(f"Client has disconnected from {self._remote}")

    def is_closed(self):
        return False

class TCPServerSocket(TCPSocket):
    def __init__(self, sock, remote):
        super().__init__()

        self._socket = sock
        self._remote = remote

        super()._reconnect()

    def _reconnect(self):
        self.close()

class TCPServer:
    """
    TCP Server class that receives client connections and constructs handler classes
    """

    def __init__(self, bind_addr: tuple, client_queue: queue.Queue):
        self.__bind_addr = bind_addr

        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        self.__client_queue = client_queue

        self.__clients = []

        self.__event_consumer = mt_events.EventConsumer()
        self.__E_ON_CLOSE = 0

        self.__connected_event = mt_events.Event()
        self.__disconnected_event = mt_events.Event()

        self.__daemon = daemon.Daemon()
        self.__daemon.add(self.__accept_thread)
        self.__daemon.add(self.__cleanup_thread)

    def start(self):
        print(f"Binding to {self.__bind_addr}")
        self.__socket.bind(self.__bind_addr)
        self.__socket.listen()
        self.__daemon.start()

    def __accept_thread(self, stop_flag : daemon.StopFlag):
        while stop_flag.run():
            c_socket, addr = self.__socket.accept()

            handler = TCPServerSocket(c_socket, addr)
            print(f"{handler.remote()} has connected.")
            self.__clients.append(handler)
            self.__client_queue.put(handler)
            self.__connected_event.call()

            handler.on_close(self.__event_consumer, self.__E_ON_CLOSE)

            handler.start()

    def __handler_disconnected(self):
        for handler in self.__clients:
            if handler.is_closed():
                print(f"{handler.remote()} has disconnected.")
                self.__clients.remove(handler)
                print(f"Remaining clients: {len(self.__clients)}")

                self.__disconnected_event.call()
                break

    def __cleanup_thread(self, stop_flag : daemon.StopFlag):
        while stop_flag.run():
            e = self.__event_consumer.get()

            if e == self.__E_ON_CLOSE:
                self.__handler_disconnected()

    def close(self):
        """
        Stop all threads and close all client handlers constructed by this server
        """
        self.__daemon.stop()

        for handler in self.__clients:
            handler.close()

        self.__socket.close()

    def ok(self):
        return self.__daemon.is_alive()
    
    def on_connected(self, consumer : mt_events.EventConsumer, event_id):
        self.__connected_event.bind(consumer, event_id)

    def on_disconnected(self, consumer : mt_events.EventConsumer, event_id):
        self.__disconnected_event.bind(consumer, event_id)
