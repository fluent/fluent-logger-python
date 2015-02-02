# -*- coding: utf-8 -*-

import unittest

from mock import patch

from fluent import event, sender

sender.setup(server='localhost', tag='app')


class TestEvent(unittest.TestCase):
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

        # This test will fail unless you have a working connection to fluentd
        self.assertEqual(global_sender.last_error, None)

    @patch('fluent.sender.socket')
    def test_connect_exception_during_event_send(self, mock_socket):
        # Make the socket.socket().connect() call raise a custom exception
        mock_connect = mock_socket.socket.return_value.connect
        EXCEPTION_MSG = "a event send socket connect() exception"
        mock_connect.side_effect = Exception(EXCEPTION_MSG)

        # Force the socket to reconnect while trying to emit the event
        global_sender = sender.get_global_sender()
        global_sender._close()

        event.Event('unfollow', {
            'from': 'userE',
            'to':   'userF'
        })

        ex = global_sender.last_error
        self.assertEqual(ex.message, EXCEPTION_MSG)
        global_sender.clear_last_error()
