# -*- coding: utf-8 -*-

try:
    from cStringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO

import socket
import threading
import time

from msgpack import Unpacker


class MockRecvServer(threading.Thread):
    """
    Single threaded server accepts one connection and recv until EOF.
    """
    def __init__(self, host='localhost', port=24224):
        if host.startswith('unix://'):
            self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._sock.bind(host[len('unix://'):])
        else:
            self._sock = socket.socket()
            self._sock.bind((host, port))
        self._buf = BytesIO()

        threading.Thread.__init__(self)
        self.start()

    def run(self):
        sock = self._sock
        sock.listen(1)
        con, _ = sock.accept()
        while True:
            data = con.recv(4096)
            if not data:
                break
            self._buf.write(data)
        con.close()
        sock.close()
        self._sock = None

    def wait(self):
        while self._sock:
            time.sleep(0.1)

    def get_recieved(self):
        self.wait()
        self._buf.seek(0)
        # TODO: have to process string encoding properly. currently we assume
        # that all encoding is utf-8.
        return list(Unpacker(self._buf, encoding='utf-8'))
