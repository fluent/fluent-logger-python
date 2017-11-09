# -*- coding: utf-8 -*-

from __future__ import print_function
import threading
import time
try:
    from queue import Queue, Full, Empty
except ImportError:
    from Queue import Queue, Full, Empty

from fluent import sender
from fluent.sender import EventTime

_global_sender = None

DEFAULT_QUEUE_TIMEOUT = 0.05


def _set_global_sender(sender):
    """ [For testing] Function to set global sender directly
    """
    global _global_sender
    _global_sender = sender


def setup(tag, **kwargs):
    global _global_sender
    _global_sender = FluentSender(tag, **kwargs)


def get_global_sender():
    return _global_sender


def close():
    get_global_sender().close()


class CommunicatorThread(threading.Thread):
    def __init__(self, tag,
                 host='localhost',
                 port=24224,
                 bufmax=1 * 1024 * 1024,
                 timeout=3.0,
                 verbose=False,
                 buffer_overflow_handler=None,
                 nanosecond_precision=False,
                 msgpack_kwargs=None,
                 queue_timeout=DEFAULT_QUEUE_TIMEOUT, *args, **kwargs):
        super(CommunicatorThread, self).__init__(**kwargs)
        self._queue = Queue()
        self._do_run = True
        self._queue_timeout = queue_timeout
        self._conn_close_lock = threading.Lock()
        self._sender = sender.FluentSender(tag=tag, host=host, port=port, bufmax=bufmax, timeout=timeout,
                                           verbose=verbose, buffer_overflow_handler=buffer_overflow_handler,
                                           nanosecond_precision=nanosecond_precision, msgpack_kwargs=msgpack_kwargs)

    def send(self, bytes_):
        try:
            self._queue.put(bytes_)
        except Full:
            return False
        return True

    def run(self):
        while self._do_run:
            try:
                bytes_ = self._queue.get(block=True, timeout=self._queue_timeout)
            except Empty:
                continue
            self._conn_close_lock.acquire()
            self._sender._send(bytes_)
            self._conn_close_lock.release()

    def close(self, flush=True, discard=True):
        if discard:
            while not self._queue.empty():
                try:
                    self._queue.get(block=False)
                except Empty:
                    break
        while flush and (not self._queue.empty()):
            time.sleep(0.1)
        self._do_run = False
        self._sender.close()

    def _close(self):
        self._conn_close_lock.acquire()
        # self._sender.lock.acquire()
        try:
            self._sender._close()
        finally:
            # self._sender.lock.release()
            self._conn_close_lock.release()
            pass

    @property
    def last_error(self):
        return self._sender.last_error

    @last_error.setter
    def last_error(self, err):
        self._sender.last_error = err

    def clear_last_error(self, _thread_id = None):
        self._sender.clear_last_error(_thread_id=_thread_id)

    @property
    def queue_timeout(self):
        return self._queue_timeout

    @queue_timeout.setter
    def queue_timeout(self, value):
        self._queue_timeout = value

    def __enter__(self):
        return self

    def __exit__(self, typ, value, traceback):
        self.close()


class FluentSender(sender.FluentSender):
    def __init__(self,
                 tag,
                 host='localhost',
                 port=24224,
                 bufmax=1 * 1024 * 1024,
                 timeout=3.0,
                 verbose=False,
                 buffer_overflow_handler=None,
                 nanosecond_precision=False,
                 msgpack_kwargs=None,
                 queue_timeout=DEFAULT_QUEUE_TIMEOUT,
                 **kwargs): # This kwargs argument is not used in __init__. This will be removed in the next major version.
        super(FluentSender, self).__init__(tag=tag, host=host, port=port, bufmax=bufmax, timeout=timeout,
                                           verbose=verbose, buffer_overflow_handler=buffer_overflow_handler,
                                           nanosecond_precision=nanosecond_precision, msgpack_kwargs=msgpack_kwargs,
                                           **kwargs)
        self._communicator = CommunicatorThread(tag=tag, host=host, port=port, bufmax=bufmax, timeout=timeout,
                                                verbose=verbose, buffer_overflow_handler=buffer_overflow_handler,
                                                nanosecond_precision=nanosecond_precision, msgpack_kwargs=msgpack_kwargs,
                                                queue_timeout=queue_timeout)
        self._communicator.start()

    def _send(self, bytes_):
        return self._communicator.send(bytes_=bytes_)

    def _close(self):
        # super(FluentSender, self)._close()
        self._communicator._close()

    def _send_internal(self, bytes_):
        return

    def _send_data(self, bytes_):
        return

    # override reconnect, so we don't open a socket here (since it
    # will be opened by the CommunicatorThread)
    def _reconnect(self):
        return

    def close(self):
        self._communicator.close(flush=True)
        self._communicator.join()
        return super(FluentSender, self).close()

    @property
    def last_error(self):
        return self._communicator.last_error

    @last_error.setter
    def last_error(self, err):
        self._communicator.last_error = err

    def clear_last_error(self, _thread_id = None):
        self._communicator.clear_last_error(_thread_id=_thread_id)

    @property
    def queue_timeout(self):
        return self._communicator.queue_timeout

    @queue_timeout.setter
    def queue_timeout(self, value):
        self._communicator.queue_timeout = value

    def __enter__(self):
        return self

    def __exit__(self, typ, value, traceback):
        # give time to the comm. thread to send its queued messages
        time.sleep(0.2)
        self.close()
