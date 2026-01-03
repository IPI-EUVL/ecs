import threading
import queue
import time
import traceback

class StopFlag:
    def __init__(self):
        self.__stop = False

    def run(self):
        return not self.__stop

    def stop(self):
        self.__stop = True

class _DaemonThread:
    def __init__(self, target, args : tuple, kwargs : dict, exception_queue : queue.Queue):
        self.__target = target
        self.__args = args
        self.__kwargs = kwargs

        self.__stop_flag = StopFlag()

        self.__kwargs["stop_flag"] = self.__stop_flag

        self.__exception_queue = exception_queue
        self.__thread = threading.Thread(target=self.__thread_handler, daemon=True)

    def __thread_handler(self):
        try:
            self.__target(*self.__args, **self.__kwargs)
        except Exception as e: # pylint: disable=broad-exception-caught
            self.__exception_queue.put(e)

    def start(self):
        """
        Start the thread
        """
        self.__thread.start()

    def is_alive(self):
        """
        Is thread still running?
        Returns:
            bool: If thread is still running
        """
        return self.__thread.is_alive()
    
    def stop(self):
        """
        Set stop flag
        """
        self.__stop_flag.stop()

class Daemon:
    def __init__(self, exception_handler = None):
        self.__threads = []

        self.__started = False

        self.__exception_queue = queue.Queue()
        self.__exception_handler = exception_handler

    def add(self, target, *args, **kwargs):
        """
        Add target to daemon
        Args:
            target (function): target function
            *args, **kvargs: will be passed to target
        """

        if self.__started:
            return False
        
        self.__threads.append(_DaemonThread(target, args, kwargs, self.__exception_queue))

        return True

    def start(self):
        """
        Start execution of daemon threads
        """
        if self.__started:
            return
        
        for thread in self.__threads:
            thread.start()

        threading.Thread(target=self.__supervisor_thread, daemon=True).start()
        self.__started = True

    def is_alive(self):
        running = False

        for thread in self.__threads:
            if thread.is_alive():
                running = True
                break

        return running
    
    def stop(self):
        for thread in self.__threads:
            thread.stop()
        
    def __on_exception(self, exception : Exception):
        print("Caught exception in daemon thread!")
        traceback.print_exception(type(exception), value=exception, tb=None)

        if self.__exception_handler is not None:
            self.__exception_handler(exception)

        self.stop()

        raise exception

    def __supervisor_thread(self):
        while self.is_alive():
            try:
                exc = self.__exception_queue.get(timeout=1)
                self.__on_exception(exc)
            except queue.Empty:
                continue