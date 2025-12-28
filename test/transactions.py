import queue
import time

import ecs.lib.transactions as transactions
import ecs.lib.mt_events as mt_events
import ecs.lib.daemon as daemon

a_out = queue.Queue()
a = transactions.TransactionManager(a_out)
a_event = mt_events.EventConsumer()

E_MESSAGE = 0
E_TRANS = 1

a.on_send_data(a_event, E_MESSAGE)
a.on_receive_transaction(a_event, E_TRANS)

b_out = queue.Queue()
b = transactions.TransactionManager(b_out)
b_event = mt_events.EventConsumer()

b.on_send_data(b_event, E_MESSAGE)
b.on_receive_transaction(a_event, E_TRANS)

trans_handle = None

def a_thread(stop_flag : daemon.StopFlag):
    global trans_handle

    while stop_flag.run():
        e = a_event.get()

        if e == E_MESSAGE:
            m = a_out.get()
            b.received(m)
        
        if e == E_TRANS:
            print("A has an incoming transaction")
            trans_handle = a.get_incoming()

def b_thread(stop_flag : daemon.StopFlag):
    while stop_flag.run():
        e = b_event.get()


        if e == E_MESSAGE:
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