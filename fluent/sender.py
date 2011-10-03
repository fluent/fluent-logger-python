import os
import sys, urllib
import msgpack
import socket
import threading
import json
import time

global_sender = None

def setup(tag, **kwargs):
    host = ('host' in kwargs) and kwargs['host'] or 'localhost'
    port = ('port' in kwargs) and kwargs['port'] or 24224

    global global_sender
    global_sender = FluentSender(tag, host=host, port=port)

def get_global_sender():
    global global_sender
    return global_sender

class FluentSender(object):
    def __init__(self,
           tag,
           host='127.0.0.1',
           port=24224,
           bufmax=1*1024*1024,
           timeout=3.0,
           verbose=False):

        self.tag = tag
        self.host = host
        self.port = port
        self.bufmax = bufmax
        self.timeout = timeout
        self.verbose = verbose

        self.pendings = None
        self.packer = msgpack.Packer()
        self.lock = threading.Lock()
        try:
            self.socket = self._connect()
        except:
            # will be retried in emit()
            self.socket = None

    def emit(self, label, data):
        bytes = self._make_packet(label, data)
        self._send(bytes)

    def _reconnect(self):
        if not self.socket:
            self.socket = self._connect()

    def _connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect((self.host, self.port))
        return sock

    def _close(self):
        if self.socket:
            self.socket.close()
        self.socket = None

    def _make_packet(self, label, data):
        tag = "%s.%s" % (self.tag, label)
        cur_time = int(time.mktime(time.localtime()))
        packet = [ tag, cur_time, data ]
        if self.verbose:
            print packet
        return self.packer.pack(packet)

    def _send(self, bytes):
        self.lock.acquire()
        try:
            self._send_internal(bytes)
        finally:
            self.lock.release()

    def _send_internal(self, bytes):
        # buffering
        if self.pendings:
            self.pendings += bytes
            bytes = self.pendings

        try:
            # reconnect if possible
            self._reconnect()

            # send message
            total = len(bytes)
            nsent = 0
            while nsent < total:
                n = self.socket.send(bytes[nsent:])
                if n <= 0:
                    raise RuntimeError("socket connection broken")
                nsent += n

            # send finished
            self.pendings = None
        except:
            # close socket
            self._close()
            # clear buffer if it exceeds max bufer size
            if self.pendings and (len(self.pendings) > self.bufmax):
                self.pendings = None
            else:
                self.pendings = bytes
