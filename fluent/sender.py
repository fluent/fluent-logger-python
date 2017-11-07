# -*- coding: utf-8 -*-

from __future__ import print_function
import struct
import socket
import threading
import time
import traceback

import msgpack


_global_sender = None


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


class EventTime(msgpack.ExtType):
    def __new__(cls, timestamp):
        seconds = int(timestamp)
        nanoseconds = int(timestamp % 1 * 10 ** 9)
        return super(EventTime, cls).__new__(
            cls,
            code=0,
            data=struct.pack(">II", seconds, nanoseconds),
        )


class FluentSender(object):
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
                 **kwargs): # This kwargs argument is not used in __init__. This will be removed in the next major version.

        self.tag = tag
        self.host = host
        self.port = port
        self.bufmax = bufmax
        self.timeout = timeout
        self.verbose = verbose
        self.buffer_overflow_handler = buffer_overflow_handler
        self.nanosecond_precision = nanosecond_precision
        self.msgpack_kwargs = {} if msgpack_kwargs is None else msgpack_kwargs

        self.socket = None
        self.pendings = None
        self.lock = threading.Lock()
        self._last_error_threadlocal = threading.local()

        try:
            self._reconnect()
        except socket.error:
            # will be retried in emit()
            self._close()

    def emit(self, label, data):
        if self.nanosecond_precision:
            cur_time = EventTime(time.time())
        else:
            cur_time = int(time.time())
        return self.emit_with_time(label, cur_time, data)

    def emit_with_time(self, label, timestamp, data):
        if self.nanosecond_precision and isinstance(timestamp, float):
            timestamp = EventTime(timestamp)
        try:
            bytes_ = self._make_packet(label, timestamp, data)
        except Exception as e:
            self.last_error = e
            bytes_ = self._make_packet(label, timestamp,
                                       {"level": "CRITICAL",
                                        "message": "Can't output to log",
                                        "traceback": traceback.format_exc()})
        return self._send(bytes_)

    def close(self):
        self.lock.acquire()
        try:
            if self.pendings:
                try:
                    self._send_data(self.pendings)
                except Exception:
                    self._call_buffer_overflow_handler(self.pendings)

            self._close()
            self.pendings = None
        finally:
            self.lock.release()

    def _make_packet(self, label, timestamp, data):
        if label:
            tag = '.'.join((self.tag, label))
        else:
            tag = self.tag
        packet = (tag, timestamp, data)
        if self.verbose:
            print(packet)
        return msgpack.packb(packet, **self.msgpack_kwargs)

    def _send(self, bytes_):
        self.lock.acquire()
        try:
            return self._send_internal(bytes_)
        finally:
            self.lock.release()

    def _send_internal(self, bytes_):
        # buffering
        if self.pendings:
            self.pendings += bytes_
            bytes_ = self.pendings

        try:
            self._send_data(bytes_)

            # send finished
            self.pendings = None

            return True
        except socket.error as e:
        #except Exception as e:
            self.last_error = e

            # close socket
            self._close()

            # clear buffer if it exceeds max bufer size
            if self.pendings and (len(self.pendings) > self.bufmax):
                self._call_buffer_overflow_handler(self.pendings)
                self.pendings = None
            else:
                self.pendings = bytes_

            return False

    def _send_data(self, bytes_):
        # reconnect if possible
        self._reconnect()
        # send message
        self.socket.sendall(bytes_)

    def _reconnect(self):
        if not self.socket:
            if self.host.startswith('unix://'):
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(self.timeout)
                sock.connect(self.host[len('unix://'):])
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.timeout)
                sock.connect((self.host, self.port))
            self.socket = sock

    def _call_buffer_overflow_handler(self, pending_events):
        try:
            if self.buffer_overflow_handler:
                self.buffer_overflow_handler(pending_events)
        except Exception as e:
            # User should care any exception in handler
            pass

    @property
    def last_error(self):
        return getattr(self._last_error_threadlocal, 'exception', None)

    @last_error.setter
    def last_error(self, err):
        self._last_error_threadlocal.exception  =  err

    def clear_last_error(self, _thread_id = None):
        if hasattr(self._last_error_threadlocal, 'exception'):
            delattr(self._last_error_threadlocal, 'exception')

    def _close(self):
        if self.socket:
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
        self.socket = None

    def __enter__(self):
        return self

    def __exit__(self, typ, value, traceback):
        self.close()
