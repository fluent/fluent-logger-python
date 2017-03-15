# -*- coding: utf-8 -*-

import socket

try:
    from cStringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO

from msgpack import Unpacker

from fluent.transport import get_connection_params


def create_server(host, port=0):
    family, socket_type, addr = get_connection_params(host, port)

    if socket_type == UDPServer.SOCKET_TYPE:
        conn_type = UDPServer
    else:
        if family == socket.AF_UNIX:
            conn_type = UnixSocketServer
        else:
            conn_type = TCPServer

    conn = conn_type(family, addr)
    conn.listen()
    return conn


class Server(object):
    SOCKET_TYPE = ""

    def __init__(self, family, addr):
        self._family = family
        self._addr = addr

        self._sock = socket.socket(self._family, self.SOCKET_TYPE)
        self._sock.bind(self._addr)
        self._sock.settimeout(0.5)

    def listen(self):
        # Okay move along, move along people, there's nothing to see here!
        pass

    def recv(self, qty_messages=1):
        data = BytesIO()
        while True:
            chunk = self.recv_raw()
            data.seek(0, 2)
            data.write(chunk)

            data.seek(0)
            messages = list(Unpacker(data, encoding='utf-8'))
            if len(messages) >= qty_messages:
                break

        return list(messages)

    def recv_raw(self, limit=1024):
        raise NotImplementedError

    def close(self):
        self._sock.close()

    def addr(self):
        raise NotImplementedError


class TCPServer(Server):
    SOCKET_TYPE = socket.SOCK_STREAM

    def __init__(self, *args, **kwargs):
        super(TCPServer, self).__init__(*args, **kwargs)

        self.accepted_connection = None

    def listen(self):
        self._sock.listen(1)

    def recv_raw(self, limit=1024):
        if not self.accepted_connection:
            self.accepted_connection, _ = self._sock.accept()

        return self.accepted_connection.recv(limit)

    def close(self):
        super(TCPServer, self).close()
        if self.accepted_connection:
            self.accepted_connection.close()

    def addr(self):
        return "tcp://{}:{}".format(*self._sock.getsockname())


class UnixSocketServer(TCPServer):
    def addr(self):
        return "unix://{}".format(self._sock.getsockname())


class UDPServer(Server):
    SOCKET_TYPE = socket.SOCK_DGRAM

    def recv_raw(self, limit=1024):
        return self._sock.recv(limit)

    def addr(self):
        return "udp://{}:{}".format(*self._sock.getsockname())
