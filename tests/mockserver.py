try:
    from cStringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO

import socket
import threading

from msgpack import Unpacker


class MockRecvServer(threading.Thread):
    """
    Single threaded server accepts one connection and recv until EOF.
    """

    def __init__(self, host="localhost", port=0):
        super().__init__()

        if host.startswith("unix://"):
            self.socket_proto = socket.AF_UNIX
            self.socket_type = socket.SOCK_STREAM
            self.socket_addr = host[len("unix://") :]
        else:
            self.socket_proto = socket.AF_INET
            self.socket_type = socket.SOCK_STREAM
            self.socket_addr = (host, port)

        self._sock = socket.socket(self.socket_proto, self.socket_type)
        self._sock.bind(self.socket_addr)
        if self.socket_proto == socket.AF_INET:
            self.port = self._sock.getsockname()[1]

        self._sock.listen(1)
        self._buf = BytesIO()
        self._con = None

        self.start()

    def run(self):
        sock = self._sock

        try:
            try:
                con, _ = sock.accept()
            except Exception:
                return
            self._con = con
            try:
                while True:
                    try:
                        data = con.recv(16384)
                        if not data:
                            break
                        self._buf.write(data)
                    except OSError as e:
                        print("MockServer error: %s" % e)
                        break
            finally:
                con.close()
        finally:
            sock.close()

    def get_received(self):
        self.join()
        self._buf.seek(0)
        return list(Unpacker(self._buf))

    def close(self):
        try:
            self._sock.close()
        except Exception:
            pass

        try:
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conn.connect((self.socket_addr[0], self.port))
            finally:
                conn.close()
        except Exception:
            pass

        if self._con:
            try:
                self._con.close()
            except Exception:
                pass

        self.join()
