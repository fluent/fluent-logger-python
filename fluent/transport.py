# encoding=utf-8

import socket

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


class Transport(object):
    def __init__(self, host, port, timeout):
        self.host = host
        self.port = port
        self.timeout = timeout

        self._conn = None

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def connect(self):
        if self._conn:
            return

        family, socket_type, addr = get_connection_params(self.host, self.port)
        self._conn = socket.socket(family, socket_type)
        self._conn.connect(addr)
        self._conn.settimeout(self.timeout)

    def send(self, data):
        self.connect()
        self._conn.sendall(data.encode('utf-8'))


def get_connection_params(url, port=0):
    parsed = urlparse(url)

    port = parsed.port or port or 0

    scheme = parsed.scheme.lower()
    if scheme == 'unix':
        family = socket.AF_UNIX
        socket_type = socket.SOCK_STREAM
        addr = parsed.hostname

    elif scheme == 'udp':
        family = socket.AF_INET
        socket_type = socket.SOCK_DGRAM
        addr = (parsed.hostname, port)

    elif scheme in ('tcp', ''):
        family = socket.AF_INET
        socket_type = socket.SOCK_STREAM
        addr = (parsed.hostname or parsed.path, port)

    else:
        raise TransportError(
            "Unknown connection protocol: url={}, port={}".format(
                url, port,
            )
        )

    return family, socket_type, addr


TransportError = socket.error
