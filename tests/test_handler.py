# -*- coding: utf-8 -*-

import logging
import unittest

import fluent.handler

from tests import mockserver


class TestLogger(unittest.TestCase):
    def setUp(self):
        super(TestLogger, self).setUp()
        for port in range(10000, 20000):
            try:
                self._server = mockserver.MockRecvServer(port)
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
        log.addHandler(handler)
        log.info({
            'from': 'userA',
            'to': 'userB'
        })
        handler._close()

        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('app.follow', data[0][0])
        eq('userA', data[0][2]['from'])
        eq('userB', data[0][2]['to'])
        self.assert_(data[0][1])
        self.assert_(isinstance(data[0][1], int))
