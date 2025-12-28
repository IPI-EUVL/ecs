import queue
import uuid

import ipi_ecs.core.mt_events as mt_events

MAGIC_NEW_TRANS = 0x00
MAGIC_ACK_TRANS = 0x01
MAGIC_NAK_TRANS = 0x02
MAGIC_RET_TRANS = 0x03

class TransactionManager:
    class __IncomingTransactionData:
        def __init__(self, t_uuid : uuid.UUID, t_data : bytes, transponder : "TransactionManager"):
            self.__uuid = t_uuid
            self.__transponder = transponder
            self.__data = t_data

        def get_uuid(self):
            return self.__uuid
        
        def get_data(self):
            return self.__data
        
        def ack(self):
            self.__transponder._send_ack(self.__uuid)

        def nak(self):
            self.__transponder._send_nak(self.__uuid)

        def ret(self, data : bytes):
            self.__transponder._send_ret(self.__uuid, data)

    class IncomingTransactionHandle:
        def __init__(self, t_data : "TransactionManager.__IncomingTransactionData"):
            self.__t_data = t_data

        def ack(self):
            self.__t_data.ack()

        def nak(self):
            self.__t_data.nak()

        def ret(self, data : bytes):
            self.__t_data.ret(data)

        def get_data(self):
            return self.__t_data.get_data()

    class __OutgoingTransactionData:
        STATE_PENDING = 0
        STATE_ACK = 1
        STATE_NAK = 2
        STATE_RET = 3
        def __init__(self, data : bytes):
            self.__data = data
            self.__result = None

            self.__ack = None

            self.__event_state_change = mt_events.Event()

            self.__cb_fn = None
            self.__cb_pargs = None
            self.__cb_kwargs = None

            self.__uuid = uuid.uuid4()

        def finished(self, res_data):
            self.__result = res_data
            self.__event_state_change.call()
            self.__call_cb()

        def receive_ack(self):
            self.__ack = True
            self.__event_state_change.call()
            self.__call_cb()

        def receive_nak(self):
            self.__ack = False
            self.__event_state_change.call()
            self.__call_cb()

        def get_data(self):
            return self.__data

        def get_uuid(self):
            return self.__uuid
        
        def get_result(self):
            return self.__result
        
        def get_state(self):
            if self.__result is not None:
                return self.STATE_RET
            elif self.__ack is None:
                return self.STATE_PENDING
            elif self.__ack:
                return self.STATE_ACK
            
            return self.STATE_NAK
        
        def on_state_change(self, event_c : mt_events.EventConsumer, event_id):
            self.__event_state_change.bind(event_c, event_id)

        def then(self, fn, pargs = [], kwargs = dict()):
            self.__cb_fn = fn
            self.__cb_pargs = pargs
            self.__cb_kwargs = dict(kwargs)

            self.__cb_kwargs["handle"] = self

        def __call_cb(self):
            if self.__cb_fn is not None:
                self.__cb_fn(*self.__cb_pargs, **self.__cb_kwargs)

    class OutgoingTransactionHandle:
        E_STATE_CHANGE = 0
        STATE_PENDING = 0
        STATE_ACK = 1
        STATE_NAK = 2
        STATE_RET = 3

        def __init__(self, handle : "TransactionManager.__OutgoingTransactionData"):
            self.__handle = handle
        
        def get_data(self):
            return self.__handle.get_data()
        
        def get_result(self):
            return self.__handle.get_result()
        
        def get_state(self):
            return self.__handle.get_state()
        
        def get_uuid(self):
            return self.__handle.get_uuid()
        
        def on_state_change(self, event_c : mt_events.EventConsumer, event_id):
            self.__handle.on_state_change(event_c, event_id)

        def then(self, fn, pargs = [], kwargs = dict()):
            self.__handle.then(fn, pargs, kwargs)

    def __init__(self, out_stream : queue.Queue):
        self.__sent_transactions = dict()
        self.__recv_trans_queue = queue.Queue()

        self.__on_recv_trans = mt_events.Event()
        self.__on_send_data = mt_events.Event()

        self.__out_stream = out_stream

    def __get_bytes(self, t : __OutgoingTransactionData):
        ret = bytes()

        ret += bytes([MAGIC_NEW_TRANS])
        ret += t.get_uuid().bytes
        ret += t.get_data()

        return ret

    def __recv_trans(self, data : bytes):
        t_uuid = uuid.UUID(bytes=data[:16])
        t_data = data[16:]

        t_d = self.__IncomingTransactionData(t_uuid, t_data, self)
        t_h = self.IncomingTransactionHandle(t_d)
        
        self.__recv_trans_queue.put(t_h)
        self.__on_recv_trans.call()

    def __recv_ret(self, t_uuid : uuid.UUID, data: bytes):
        out_d = self.__sent_transactions.pop(t_uuid)
        out_d.finished(data)

    def _send_ack(self, t_uuid : uuid.UUID):
        self.__out_stream.put(bytes([MAGIC_ACK_TRANS]) + t_uuid.bytes)
        self.__on_send_data.call()

    def _send_nak(self, t_uuid : uuid.UUID):
        self.__out_stream.put(bytes([MAGIC_NAK_TRANS]) + t_uuid.bytes)
        self.__on_send_data.call()

    def _send_ret(self, t_uuid : uuid.UUID, data : bytes):
        self.__out_stream.put(bytes([MAGIC_RET_TRANS]) + t_uuid.bytes + data)
        self.__on_send_data.call()

    def send_transaction(self, data : bytes):
        t = self.__OutgoingTransactionData(data)
        b = self.__get_bytes(t)

        if self.__sent_transactions.get(t.get_uuid()) is not None:
            raise Exception("Transaction with same UUID already exists?")

        self.__sent_transactions[t.get_uuid()] = t

        self.__out_stream.put(b)
        self.__on_send_data.call()
        return self.OutgoingTransactionHandle(t)
    
    def received(self, data: bytes):
        sw = data[0]

        if sw == MAGIC_NEW_TRANS:
            self.__recv_trans(data[1:])
        
        t_uuid = uuid.UUID(bytes=data[1:17])

        if sw == MAGIC_ACK_TRANS:
            self.__sent_transactions[t_uuid].receive_ack()
        elif sw == MAGIC_NAK_TRANS:
            self.__sent_transactions[t_uuid].receive_nak()
            self.__sent_transactions.pop(t_uuid)
        elif sw == MAGIC_RET_TRANS:
            self.__recv_ret(t_uuid, data[17:])

    def get_incoming(self, block = True, timeout = 1.0) -> "TransactionManager.IncomingTransactionHandle":
        return self.__recv_trans_queue.get(block=block, timeout=timeout)

    def on_receive_transaction(self, event_c : mt_events.EventConsumer, event_id):
        self.__on_recv_trans.bind(event_c, event_id)

    def on_send_data(self, event_c : mt_events.EventConsumer, event_id):
        self.__on_send_data.bind(event_c, event_id)
