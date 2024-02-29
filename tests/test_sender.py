import errno
import sys
import unittest
from shutil import rmtree
from tempfile import mkdtemp

import msgpack

import fluent.sender
from tests import mockserver


class TestSetup(unittest.TestCase):
    def tearDown(self):
        from fluent.sender import _set_global_sender

        _set_global_sender(None)

    def test_no_kwargs(self):
        fluent.sender.setup("tag")
        actual = fluent.sender.get_global_sender()
        self.assertEqual(actual.tag, "tag")
        self.assertEqual(actual.host, "localhost")
        self.assertEqual(actual.port, 24224)
        self.assertEqual(actual.timeout, 3.0)

    def test_host_and_port(self):
        fluent.sender.setup("tag", host="myhost", port=24225)
        actual = fluent.sender.get_global_sender()
        self.assertEqual(actual.tag, "tag")
        self.assertEqual(actual.host, "myhost")
        self.assertEqual(actual.port, 24225)
        self.assertEqual(actual.timeout, 3.0)

    def test_tolerant(self):
        fluent.sender.setup("tag", host="myhost", port=24225, timeout=1.0)
        actual = fluent.sender.get_global_sender()
        self.assertEqual(actual.tag, "tag")
        self.assertEqual(actual.host, "myhost")
        self.assertEqual(actual.port, 24225)
        self.assertEqual(actual.timeout, 1.0)


class TestSender(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self._server = mockserver.MockRecvServer("localhost")
        self._sender = fluent.sender.FluentSender(tag="test", port=self._server.port)

    def tearDown(self):
        try:
            self._sender.close()
        finally:
            self._server.close()

    def get_data(self):
        return self._server.get_received()

    def test_simple(self):
        sender = self._sender
        sender.emit("foo", {"bar": "baz"})
        sender._close()
        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq("test.foo", data[0][0])
        eq({"bar": "baz"}, data[0][2])
        self.assertTrue(data[0][1])
        self.assertTrue(isinstance(data[0][1], int))

    def test_decorator_simple(self):
        with self._sender as sender:
            sender.emit("foo", {"bar": "baz"})
        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq("test.foo", data[0][0])
        eq({"bar": "baz"}, data[0][2])
        self.assertTrue(data[0][1])
        self.assertTrue(isinstance(data[0][1], int))

    def test_nanosecond(self):
        sender = self._sender
        sender.nanosecond_precision = True
        sender.emit("foo", {"bar": "baz"})
        sender._close()
        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq("test.foo", data[0][0])
        eq({"bar": "baz"}, data[0][2])
        self.assertTrue(isinstance(data[0][1], msgpack.ExtType))
        eq(data[0][1].code, 0)

    def test_nanosecond_coerce_float(self):
        time = 1490061367.8616468906402588
        sender = self._sender
        sender.nanosecond_precision = True
        sender.emit_with_time("foo", time, {"bar": "baz"})
        sender._close()
        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq("test.foo", data[0][0])
        eq({"bar": "baz"}, data[0][2])
        self.assertTrue(isinstance(data[0][1], msgpack.ExtType))
        eq(data[0][1].code, 0)
        eq(data[0][1].data, b"X\xd0\x8873[\xb0*")

    def test_no_last_error_on_successful_emit(self):
        sender = self._sender
        sender.emit("foo", {"bar": "baz"})
        sender._close()

        self.assertEqual(sender.last_error, None)

    def test_last_error_property(self):
        EXCEPTION_MSG = "custom exception for testing last_error property"
        self._sender.last_error = OSError(EXCEPTION_MSG)

        self.assertEqual(self._sender.last_error.args[0], EXCEPTION_MSG)

    def test_clear_last_error(self):
        EXCEPTION_MSG = "custom exception for testing clear_last_error"
        self._sender.last_error = OSError(EXCEPTION_MSG)
        self._sender.clear_last_error()

        self.assertEqual(self._sender.last_error, None)
        self._sender.clear_last_error()
        self.assertEqual(self._sender.last_error, None)

    def test_emit_error(self):
        with self._sender as sender:
            sender.emit("blah", {"a": object()})

        data = self._server.get_received()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0][2]["message"], "Can't output to log")

    def test_emit_error_no_forward(self):
        with self._sender as sender:
            sender.forward_packet_error = False
            with self.assertRaises(TypeError):
                sender.emit("blah", {"a": object()})

    def test_emit_after_close(self):
        with self._sender as sender:
            self.assertTrue(sender.emit("blah", {"a": "123"}))
            sender.close()
            self.assertFalse(sender.emit("blah", {"a": "456"}))

        data = self._server.get_received()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0][2]["a"], "123")

    def test_verbose(self):
        with self._sender as sender:
            sender.verbose = True
            sender.emit("foo", {"bar": "baz"})
            # No assertions here, just making sure there are no exceptions

    def test_failure_to_connect(self):
        self._server.close()

        with self._sender as sender:
            sender._send_internal(b"123")
            self.assertEqual(sender.pendings, b"123")
            self.assertIsNone(sender.socket)

            sender._send_internal(b"456")
            self.assertEqual(sender.pendings, b"123456")
            self.assertIsNone(sender.socket)

            sender.pendings = None
            overflows = []

            def boh(buf):
                overflows.append(buf)

            def boh_with_error(buf):
                raise RuntimeError

            sender.buffer_overflow_handler = boh

            sender._send_internal(b"0" * sender.bufmax)
            self.assertFalse(overflows)  # No overflow

            sender._send_internal(b"1")
            self.assertTrue(overflows)
            self.assertEqual(overflows.pop(0), b"0" * sender.bufmax + b"1")

            sender.buffer_overflow_handler = None
            sender._send_internal(b"0" * sender.bufmax)
            sender._send_internal(b"1")
            self.assertIsNone(sender.pendings)

            sender.buffer_overflow_handler = boh_with_error
            sender._send_internal(b"0" * sender.bufmax)
            sender._send_internal(b"1")
            self.assertIsNone(sender.pendings)

            sender._send_internal(b"1")
            self.assertFalse(overflows)  # No overflow
            self.assertEqual(sender.pendings, b"1")
            self.assertIsNone(sender.socket)

            sender.buffer_overflow_handler = boh
            sender.close()
            self.assertEqual(overflows.pop(0), b"1")

    def test_broken_conn(self):
        with self._sender as sender:
            sender._send_internal(b"123")
            self.assertIsNone(sender.pendings, b"123")
            self.assertTrue(sender.socket)

            class FakeSocket:
                def __init__(self):
                    self.to = 123
                    self.send_side_effects = [3, 0, 9]
                    self.send_idx = 0
                    self.recv_side_effects = [
                        OSError(errno.EWOULDBLOCK, "Blah"),
                        b"this data is going to be ignored",
                        b"",
                        OSError(errno.EWOULDBLOCK, "Blah"),
                        OSError(errno.EWOULDBLOCK, "Blah"),
                        OSError(errno.EACCES, "This error will never happen"),
                    ]
                    self.recv_idx = 0

                def send(self, bytes_):
                    try:
                        v = self.send_side_effects[self.send_idx]
                        if isinstance(v, Exception):
                            raise v
                        if isinstance(v, type) and issubclass(v, Exception):
                            raise v()
                        return v
                    finally:
                        self.send_idx += 1

                def shutdown(self, mode):
                    pass

                def close(self):
                    pass

                def settimeout(self, to):
                    self.to = to

                def gettimeout(self):
                    return self.to

                def recv(self, bufsize, flags=0):
                    try:
                        v = self.recv_side_effects[self.recv_idx]
                        if isinstance(v, Exception):
                            raise v
                        if isinstance(v, type) and issubclass(v, Exception):
                            raise v()
                        return v
                    finally:
                        self.recv_idx += 1

            old_sock = self._sender.socket
            sock = FakeSocket()

            try:
                self._sender.socket = sock
                sender.last_error = None
                self.assertTrue(sender._send_internal(b"456"))
                self.assertFalse(sender.last_error)

                self._sender.socket = sock
                sender.last_error = None
                self.assertFalse(sender._send_internal(b"456"))
                self.assertEqual(sender.last_error.errno, errno.EPIPE)

                self._sender.socket = sock
                sender.last_error = None
                self.assertFalse(sender._send_internal(b"456"))
                self.assertEqual(sender.last_error.errno, errno.EPIPE)

                self._sender.socket = sock
                sender.last_error = None
                self.assertFalse(sender._send_internal(b"456"))
                self.assertEqual(sender.last_error.errno, errno.EACCES)
            finally:
                self._sender.socket = old_sock

    @unittest.skipIf(sys.platform == "win32", "Unix socket not supported")
    def test_unix_socket(self):
        self.tearDown()
        tmp_dir = mkdtemp()
        try:
            server_file = "unix://" + tmp_dir + "/tmp.unix"
            self._server = mockserver.MockRecvServer(server_file)
            self._sender = fluent.sender.FluentSender(tag="test", host=server_file)
            with self._sender as sender:
                self.assertTrue(sender.emit("foo", {"bar": "baz"}))

            data = self._server.get_received()
            self.assertEqual(len(data), 1)
            self.assertEqual(data[0][2], {"bar": "baz"})

        finally:
            rmtree(tmp_dir, True)


class TestEventTime(unittest.TestCase):
    def test_event_time(self):
        time = fluent.sender.EventTime(1490061367.8616468906402588)
        self.assertEqual(time.code, 0)
        self.assertEqual(time.data, b"X\xd0\x8873[\xb0*")
