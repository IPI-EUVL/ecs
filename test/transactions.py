import queue
import time

import ipi_ecs.core.transactions as transactions
import mt_events
import ipi_ecs.core.daemon as daemon

a_out = queue.Queue()
a = transactions.TransactionManager(a_out)
a_event = mt_events.EventConsumer()

E_MESSAGE = 0
E_TRANS = 1

A_E_MESSAGE = a.on_send_data().bind(a_event)
A_E_TRANS = a.on_receive_transaction().bind(a_event)

b_out = queue.Queue()
b = transactions.TransactionManager(b_out)
b_event = mt_events.EventConsumer()

B_E_MESSAGE = b.on_send_data().bind(b_event)
B_E_TRANS = b.on_receive_transaction().bind(b_event)

trans_handle = None

def a_thread(stop_flag : daemon.StopFlag):
    global trans_handle

    while stop_flag.run():
        e = a_event.get()

        if e == A_E_MESSAGE:
            m = a_out.get()
            b.received(m)
        
        if e == A_E_TRANS:
            print("A has an incoming transaction")
            trans_handle = a.get_incoming()

def b_thread(stop_flag : daemon.StopFlag):
    while stop_flag.run():
        e = b_event.get()


        if e == B_E_MESSAGE:
            m = b_out.get()
            a.received(m)

d = daemon.Daemon()
d.add(a_thread)
d.add(b_thread)
d.start()

out_t = b.send_transaction(b"testing")
print("B sent transaction with UUID", out_t.get_uuid())

print("Sent transaction state: ", out_t.get_state())
time.sleep(1)
trans_handle.ack()
time.sleep(0.1)
print("Sent transaction state: ", out_t.get_state())
time.sleep(0.1)
trans_handle.ret(b"returned")
print("Sent transaction state: ", out_t.get_state())
print("Sent transaction result: ", out_t.get_result())
time.sleep(0.1)
print("Sent transaction state: ",out_t.get_state())
print("Sent transaction result: ",out_t.get_result())