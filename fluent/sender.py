# -*- coding: utf-8 -*-

from __future__ import print_function
import socket
import threading
import time
import urllib2, urllib
import msgpack
import sys

_global_sender = None


def setup(tag, **kwargs):
    host = kwargs.get('host', 'localhost')
    port = kwargs.get('port', 24224)

    global _global_sender
    _global_sender = FluentSender(tag, host=host, port=port)


def get_global_sender():
    return _global_sender

class FluentHTTPSender(object):
    def __init__(self, tag, host='localhost', port=9880, events_max=20):
        tag_path = tag.replace('.', '/')
        self.url = '%s:%i/%s'%(host, port, tag_path)
       	self.events = []
        self.events_max = events_max
        self.msgpack_packer = msgpack.Packer()

    def emit(self, record):
        if "time" not in record:
            record["time"] = int(time.time())
        self._buffer_or_send(time, record)

    def _buffer_or_send(self, time, record):
        self.events.append(record)
        if len(self.events) > self.events_max:
          self._send()
    
    def _send(self):
        request = urllib2.Request(self.url)
        data = urllib.urlencode({"msgpack":self.msgpack_packer.pack(self.events)})
        request.add_data(data)
        response = urllib2.urlopen(request)
        if response.getcode() == 200:
            self.events = [] # flushing the buffer
        else:
            print('Failed to flush the buffer', sys.stderr)

    def __del__(self):
        self._send()


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
        self.packer = msgpack.Packer()
        self.lock = threading.Lock()

        try:
            self._reconnect()
        except Exception:
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
        return self.packer.pack(packet)

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
            # reconnect if possible
            self._reconnect()

            # send message
            self.socket.sendall(bytes_)

            # send finished
            self.pendings = None
        except Exception:
            # close socket
            self._close()
            # clear buffer if it exceeds max bufer size
            if self.pendings and (len(self.pendings) > self.bufmax):
                # TODO: add callback handler here
                self.pendings = None
            else:
                self.pendings = bytes_

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

    def _close(self):
        if self.socket:
            self.socket.close()
        self.socket = None
