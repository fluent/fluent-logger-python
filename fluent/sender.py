# -*- coding: utf-8 -*-

from __future__ import print_function
import socket
import threading
import time

import msgpack


_global_sender = None


def setup(tag, **kwargs):
    host = kwargs.get('host', 'localhost')
    port = kwargs.get('port', 24224)

    global _global_sender
    _global_sender = FluentSender(tag, host=host, port=port)


def get_global_sender():
    return _global_sender


class FluentSender(object):
    def __init__(self,
                 tag,
                 host='localhost',
                 port=24224,
                 bufmax=1 * 1024 * 1024,
                 timeout=3.0,
                 verbose=False):

        self.tag = tag
        self.host = host
        self.port = port
        self.bufmax = bufmax
        self.timeout = timeout
        self.verbose = verbose

        self.socket = None
        self.pendings = None
        self.lock = threading.Lock()

        self._last_error_by_thread_id = {}

        try:
            self._reconnect()
        except Exception as e:
            # remember latest error
            self.last_error = e

            # will be retried in emit()
            self._close()

    def emit(self, label, data):
        cur_time = int(time.time())
        self.emit_with_time(label, cur_time, data)

    def emit_with_time(self, label, timestamp, data):
        bytes_ = self._make_packet(label, timestamp, data)
        self._send(bytes_)

    def _make_packet(self, label, timestamp, data):
        if label:
            tag = '.'.join((self.tag, label))
        else:
            tag = self.tag
        packet = (tag, timestamp, data)
        if self.verbose:
            print(packet)
        return msgpack.packb(packet)

    def _send(self, bytes_):
        self.lock.acquire()
        try:
            self._send_internal(bytes_)
        finally:
            self.lock.release()

    def _send_internal(self, bytes_):
        # buffering
        if self.pendings:
            self.pendings += bytes_
            bytes_ = self.pendings

        try:
            # connect/reconnect if necessary
            self._reconnect()

            # send message
            self.socket.sendall(bytes_)
        except Exception as e:
            # remember latest error
            self.last_error = e

            # close socket
            self._close()
            # clear buffer if it exceeds max bufer size
            if self.pendings and (len(self.pendings) > self.bufmax):
                # TODO: add callback handler here
                self.pendings = None
            else:
                self.pendings = bytes_
        else:
            # send finished
            self.pendings = None

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

    @property
    def last_error(self):
        thread_id = threading.current_thread().ident
        if thread_id in self._last_error_by_thread_id:
            return self._last_error_by_thread_id[thread_id]
        return None

    @last_error.setter
    def last_error(self, err):
        thread_id = threading.current_thread().ident
        self._last_error_by_thread_id[thread_id] = err

    def clear_last_error(self, _thread_id=None):
        if _thread_id is None:
            thread_id = threading.current_thread().ident
        else:
            thread_id = _thread_id
        del self._last_error_by_thread_id[thread_id]

    def clear_errors_for_all_threads(self):
        for thread_id in self._last_error_by_thread_id.keys():
            self.clear_last_error(_thread_id=thread_id)

    def _close(self):
        if self.socket:
            self.socket.close()
        self.socket = None
