# -*- coding: utf-8 -*-

import unittest

from fluent import event, sender

from tests.mockserver import create_server


class TestException(BaseException):
    pass


class BaseTestEvent(object):
    ADDR = ""

    def setUp(self):
        self._server = create_server(self.ADDR)
        sender.setup('app', host=self._server.addr())

    def tearDown(self):
        from fluent.sender import _set_global_sender
        sender.close()
        _set_global_sender(None)
        self._server.close()

    def test_logging(self):
        # XXX: This tests succeeds even if the fluentd connection failed
        # send event with tag app.follow
        event.Event('follow', {
            'from': 'userA',
            'to':   'userB'
        })

    def test_logging_with_timestamp(self):
        # XXX: This tests succeeds even if the fluentd connection failed

        # send event with tag app.follow, with timestamp
        event.Event('follow', {
            'from': 'userA',
            'to':   'userB'
        }, time=int(0))

    def test_no_last_error_on_successful_event(self):
        global_sender = sender.get_global_sender()
        event.Event('unfollow', {
            'from': 'userC',
            'to':   'userD'
        })

        self.assertEqual(global_sender.last_error, None)
        sender.close()

    @unittest.skip("This test failed with 'TypeError: catching classes that do not inherit from BaseException is not allowed' so skipped")
    def test_connect_exception_during_event_send(self, mock_socket):
        # Make the socket.socket().connect() call raise a custom exception
        mock_connect = mock_socket.socket.return_value.connect
        EXCEPTION_MSG = "a event send socket connect() exception"
        mock_connect.side_effect = TestException(EXCEPTION_MSG)

        # Force the socket to reconnect while trying to emit the event
        global_sender = sender.get_global_sender()
        global_sender.transport.close()

        event.Event('unfollow', {
            'from': 'userE',
            'to':   'userF'
        })

        ex = global_sender.last_error
        self.assertEqual(ex.args, EXCEPTION_MSG)
        global_sender.clear_last_error()


class TestEvent_TCP(BaseTestEvent, unittest.TestCase):
    ADDR = 'tcp://localhost'


class TestEvent_UDP(BaseTestEvent, unittest.TestCase):
    ADDR = 'udp://localhost'
