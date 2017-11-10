# -*- coding: utf-8 -*-

from __future__ import print_function
import unittest
import socket
import msgpack
import time

import fluent.asyncsender
from tests import mockserver


class TestSetup(unittest.TestCase):
    def tearDown(self):
        from fluent.asyncsender import _set_global_sender
        _set_global_sender(None)

    def test_no_kwargs(self):
        fluent.asyncsender.setup("tag")
        actual = fluent.asyncsender.get_global_sender()
        self.assertEqual(actual.tag, "tag")
        self.assertEqual(actual.host, "localhost")
        self.assertEqual(actual.port, 24224)
        self.assertEqual(actual.timeout, 3.0)
        actual.close()

    def test_host_and_port(self):
        fluent.asyncsender.setup("tag", host="myhost", port=24225)
        actual = fluent.asyncsender.get_global_sender()
        self.assertEqual(actual.tag, "tag")
        self.assertEqual(actual.host, "myhost")
        self.assertEqual(actual.port, 24225)
        self.assertEqual(actual.timeout, 3.0)
        actual.close()

    def test_tolerant(self):
        fluent.asyncsender.setup("tag", host="myhost", port=24225, timeout=1.0)
        actual = fluent.asyncsender.get_global_sender()
        self.assertEqual(actual.tag, "tag")
        self.assertEqual(actual.host, "myhost")
        self.assertEqual(actual.port, 24225)
        self.assertEqual(actual.timeout, 1.0)
        actual.close()


class TestSender(unittest.TestCase):
    def setUp(self):
        super(TestSender, self).setUp()
        self._server = mockserver.MockRecvServer('localhost')
        self._sender = fluent.asyncsender.FluentSender(tag='test',
                                                  port=self._server.port)

    def tearDown(self):
        self._sender.close()

    def get_data(self):
        return self._server.get_recieved()

    def test_simple(self):
        sender = self._sender
        sender.emit('foo', {'bar': 'baz'})
        time.sleep(0.5)
        sender._close()
        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('test.foo', data[0][0])
        eq({'bar': 'baz'}, data[0][2])
        self.assertTrue(data[0][1])
        self.assertTrue(isinstance(data[0][1], int))

    def test_decorator_simple(self):
        with self._sender as sender:
            sender.emit('foo', {'bar': 'baz'})
        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('test.foo', data[0][0])
        eq({'bar': 'baz'}, data[0][2])
        self.assertTrue(data[0][1])
        self.assertTrue(isinstance(data[0][1], int))

    def test_nanosecond(self):
        sender = self._sender
        sender.nanosecond_precision = True
        sender.emit('foo', {'bar': 'baz'})
        time.sleep(0.5)
        sender._close()
        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('test.foo', data[0][0])
        eq({'bar': 'baz'}, data[0][2])
        self.assertTrue(isinstance(data[0][1], msgpack.ExtType))
        eq(data[0][1].code, 0)

    def test_nanosecond_coerce_float(self):
        time_ = 1490061367.8616468906402588
        sender = self._sender
        sender.nanosecond_precision = True
        sender.emit_with_time('foo', time_, {'bar': 'baz'})
        time.sleep(0.5)
        sender._close()
        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('test.foo', data[0][0])
        eq({'bar': 'baz'}, data[0][2])
        self.assertTrue(isinstance(data[0][1], msgpack.ExtType))
        eq(data[0][1].code, 0)
        eq(data[0][1].data, b'X\xd0\x8873[\xb0*')

    def test_no_last_error_on_successful_emit(self):
        sender = self._sender
        sender.emit('foo', {'bar': 'baz'})
        sender._close()

        self.assertEqual(sender.last_error, None)

    def test_last_error_property(self):
        EXCEPTION_MSG = "custom exception for testing last_error property"
        self._sender.last_error = socket.error(EXCEPTION_MSG)

        self.assertEqual(self._sender.last_error.args[0], EXCEPTION_MSG)

    def test_clear_last_error(self):
        EXCEPTION_MSG = "custom exception for testing clear_last_error"
        self._sender.last_error = socket.error(EXCEPTION_MSG)
        self._sender.clear_last_error()

        self.assertEqual(self._sender.last_error, None)

    @unittest.skip("This test failed with 'TypeError: catching classes that do not inherit from BaseException is not allowed' so skipped")
    #@patch('fluent.asyncsender.socket')
    def test_connect_exception_during_sender_init(self, mock_socket):
        # Make the socket.socket().connect() call raise a custom exception
        mock_connect = mock_socket.socket.return_value.connect
        EXCEPTION_MSG = "a sender init socket connect() exception"
        mock_connect.side_effect = socket.error(EXCEPTION_MSG)

        self.assertEqual(self._sender.last_error.args[0], EXCEPTION_MSG)


class TestSenderWithTimeout(unittest.TestCase):
    def setUp(self):
        super(TestSenderWithTimeout, self).setUp()
        self._server = mockserver.MockRecvServer('localhost')
        self._sender = fluent.asyncsender.FluentSender(tag='test',
                                                       port=self._server.port,
                                                       queue_timeout=0.04)

    def tearDown(self):
        self._sender.close()

    def get_data(self):
        return self._server.get_recieved()

    def test_simple(self):
        sender = self._sender
        sender.emit('foo', {'bar': 'baz'})
        time.sleep(0.5)
        sender._close()
        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('test.foo', data[0][0])
        eq({'bar': 'baz'}, data[0][2])
        self.assertTrue(data[0][1])
        self.assertTrue(isinstance(data[0][1], int))

    def test_simple_with_timeout_props(self):
        sender = self._sender
        sender.queue_timeout = 0.06
        assert sender.queue_timeout == 0.06
        sender.emit('foo', {'bar': 'baz'})
        time.sleep(0.5)
        sender._close()
        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('test.foo', data[0][0])
        eq({'bar': 'baz'}, data[0][2])
        self.assertTrue(data[0][1])
        self.assertTrue(isinstance(data[0][1], int))


class TestEventTime(unittest.TestCase):
    def test_event_time(self):
        time = fluent.asyncsender.EventTime(1490061367.8616468906402588)
        self.assertEqual(time.code, 0)
        self.assertEqual(time.data, b'X\xd0\x8873[\xb0*')
