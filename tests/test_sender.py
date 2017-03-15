# -*- coding: utf-8 -*-

from __future__ import print_function
import unittest
import socket

import fluent.sender
from tests.mockserver import create_server


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


class BaseTestSender(object):
    ADDR = ""

    def setUp(self):
        super(BaseTestSender, self).setUp()
        self._server = create_server(self.ADDR)
        self._sender = fluent.sender.FluentSender(
            tag='test', host=self._server.addr(),
        )

    def tearDown(self):
        self._sender.close()
        self._server.close()

    def get_messages(self, qty=1):
        return self._server.recv(qty)

    def test_simple(self):
        self._sender.emit('foo', {'bar': 'baz'})

        data = self.get_messages(1)
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('test.foo', data[0][0])
        eq({'bar': 'baz'}, data[0][2])
        self.assertTrue(data[0][1])
        self.assertTrue(isinstance(data[0][1], int))

    def test_no_last_error_on_successful_emit(self):
        sender = self._sender
        sender.emit('foo', {'bar': 'baz'})
        sender.transport.close()

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
    def test_connect_exception_during_sender_init(self, mock_socket):
        # Make the socket.socket().connect() call raise a custom exception
        mock_connect = mock_socket.socket.return_value.connect
        EXCEPTION_MSG = "a sender init socket connect() exception"
        mock_connect.side_effect = socket.error(EXCEPTION_MSG)

        self.assertEqual(self._sender.last_error.args[0], EXCEPTION_MSG)


class TestSender_TCP(BaseTestSender, unittest.TestCase):
    ADDR = 'tcp://localhost'


class TestSender_UDP(BaseTestSender, unittest.TestCase):
    ADDR = 'udp://localhost'
