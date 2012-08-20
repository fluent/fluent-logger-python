import socket
import threading
import time
from msgpack import Unpacker

try:
    from cStringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO

class MockRecvServer(threading.Thread):
    """
    Single threaded server accepts one connection and recv until EOF.
    """
    def __init__(self, port):
        self._sock = socket.socket()
        self._sock.bind(('localhost', port))
        self._buf = BytesIO()

        threading.Thread.__init__(self)
        self.start()

    def run(self):
        s = self._sock
        s.listen(1)
        con, _ = s.accept()
        while True:
            d = con.recv(4096)
            if not d:
                break
            self._buf.write(d)
        con.close()
        s.close()
        self._sock = None

    def wait(self):
        while self._sock:
            time.sleep(0.1)

    def get_recieved(self):
        self.wait()
        self._buf.seek(0)
        # TODO: have to process string encoding properly. currently we assume that all encoding is utf-8.
        return list(Unpacker(self._buf, encoding='utf-8'))
