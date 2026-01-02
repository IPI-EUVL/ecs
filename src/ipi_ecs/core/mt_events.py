import queue


class Event:
    """
    Event sender that can push event notifications to consumers
    """
    def __init__(self):
        self.__senders = []

    def bind(self, consumer : "EventConsumer", event):
        self.__senders.append(consumer.sender(event))

    def unbind(self, consumer):
        for sender in self.__senders:
            if sender.get_consumer() == consumer:
                self.__senders.remove(sender)
                break

    def call(self):
        for sender in self.__senders:
            sender.send()

class EventConsumer:
    """
    Event subscriber that can pull events from senders
    """
    def __init__(self):
        self.__queue = queue.Queue()

    def send(self, event, block = True, timeout = None):
        self.__queue.put(event, block=block, timeout=timeout)

    def get(self, block = True, timeout = 1):
        e = None

        try:
            e = self.__queue.get(block=block, timeout=timeout)
        except queue.Empty:
            pass

        return e
        
    
    def sender(self, event):
        return self._EventSender(self, event)

    class _EventSender:
        def __init__(self, flag : "EventConsumer", event):
            self.__flag = flag
            self.__event = event
        
        def send(self, block = True, timeout = None):
            self.__flag.send(self.__event, block, timeout)

        def get_consumer(self):
            return self.__flag
        
class Awaiter:
    class AwaiterHandle:
        def __init__(self, awaiter : "Awaiter"):
            self.__awaiter = awaiter

        def then(self, fn, pargs = [], kwargs = dict()):
            return self.__awaiter.then(fn, pargs, kwargs)
        
        def catch(self, fn, pargs = [], kwargs = dict()):
            return self.__awaiter.catch(fn, pargs, kwargs)

    def __init__(self):
        self.__params = dict()
        self.__cb_fn = None
        self.__except_fn = None

    def get_handle(self):
        return self.AwaiterHandle(self)

    def add_param(self, kw, value):
        self.__params[kw] = value

    def then(self, fn, pargs = [], kwargs = dict()):
        self.__cb_fn = fn
        self.__cb_pargs = list(pargs)
        self.__cb_kwargs = dict(kwargs)

        return self.AwaiterHandle(self)

    def catch(self, fn, pargs = [], kwargs = dict()):
        self.__except_fn = fn
        self.__except_pargs = list(pargs)
        self.__except_kwargs = dict(kwargs)

        return self.AwaiterHandle(self)

    def call(self, *pargs, **kwargs):
        if self.__cb_fn is not None:
            combined_args = self.__cb_kwargs

            for key in self.__params.keys():
                combined_args[key] = self.__params[key]

            for key in kwargs.keys():
                combined_args[key] = kwargs[key]

            self.__cb_fn(*(list(pargs) + self.__cb_pargs), **combined_args)

    def throw(self, *pargs, **kwargs):
        if self.__except_fn is not None:
            combined_args = self.__except_kwargs

            for key in self.__params.keys():
                combined_args[key] = self.__params[key]

            for key in kwargs.keys():
                combined_args[key] = kwargs[key]

            self.__except_fn(*(list(pargs) + self.__except_pargs), **combined_args)
