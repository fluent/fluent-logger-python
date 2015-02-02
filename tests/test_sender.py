# -*- coding: utf-8 -*-

from __future__ import print_function
import unittest

from mock import patch

import fluent.sender

from tests import mockserver


class TestSender(unittest.TestCase):
    def setUp(self):
        super(TestSender, self).setUp()
        for port in range(10000, 20000):
            try:
                self._server = mockserver.MockRecvServer('localhost', port)
                break
            # except IOError as exc:
            #     print(exc)
            except IOError:
                pass
        self.server_port = port
        self._sender = fluent.sender.FluentSender(tag='test', port=port)

    def tearDown(self):
        # Make sure that the mock server thread terminates after each test
        sender = self._sender
        sender.emit('foo', {'bar': 'baz'})
        sender._close()
        self.get_data()

    def get_data(self):
        return self._server.get_recieved()

    def test_simple(self):
        sender = self._sender
        sender.emit('foo', {'bar': 'baz'})
        sender._close()
        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('test.foo', data[0][0])
        eq({'bar': 'baz'}, data[0][2])
        self.assert_(data[0][1])
        self.assert_(isinstance(data[0][1], int))

    def test_no_last_error_on_successful_emit(self):
        sender = self._sender
        sender.emit('foo', {'bar': 'baz'})
        sender._close()

        self.assertEqual(sender.last_error, None)

    def test_last_error_property(self):
        sender = self._sender
        EXCEPTION_MSG = "custom exception for testing last_error property"

        sender.last_error = Exception(EXCEPTION_MSG)

        self.assertEqual(sender.last_error.message, EXCEPTION_MSG)

    def test_clear_last_error(self):
        sender = self._sender
        EXCEPTION_MSG = "custom exception for testing clear_last_error"
        sender.last_error = Exception(EXCEPTION_MSG)

        sender.clear_last_error()

        self.assertEqual(sender.last_error, None)

    @patch('fluent.sender.socket')
    def test_connect_exception_during_sender_init(self, mock_socket):
        # Make the socket.socket().connect() call raise a custom exception
        mock_connect = mock_socket.socket.return_value.connect
        EXCEPTION_MSG = "a sender init socket connect() exception"
        mock_connect.side_effect = Exception(EXCEPTION_MSG)

        sender = fluent.sender.FluentSender(tag='test', port=self.server_port)

        ex = sender.last_error
        self.assertEqual(ex.message, EXCEPTION_MSG)
