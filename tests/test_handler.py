# -*- coding: utf-8 -*-

import logging
import unittest

import fluent.handler

from tests import mockserver


class TestHandler(unittest.TestCase):
    def setUp(self):
        super(TestHandler, self).setUp()
        for port in range(10000, 20000):
            try:
                self._server = mockserver.MockRecvServer('localhost', port)
                self._port = port
                break
            except IOError:
                pass

    def get_data(self):
        return self._server.get_recieved()

    def test_simple(self):
        handler = fluent.handler.FluentHandler('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        formatter = fluent.handler.FluentRecordFormatter()
        handler.setFormatter(formatter)
        log.addHandler(handler)
        log.info({
            'from': 'userA',
            'to': 'userB'
        })
        log.info('{"from": "userC", "to": "userD"}')
        log.info("Test log message")
        handler.close()

        data = self.get_data()
        eq = self.assertEqual
        eq(3, len(data))
        eq(3, len(data[0]))
        eq('app.follow', data[0][0])
        eq('userA', data[0][2]['from'])
        eq('userB', data[0][2]['to'])
        eq('userC', data[1][2]['from'])
        eq('userD', data[1][2]['to'])
        eq('Test log message', data[2][2]['message'])
        self.assertFalse(formatter.usesTime())
        self.assertTrue(data[0][1])
        self.assertTrue(isinstance(data[0][1], int))
