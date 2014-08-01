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
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info({
            'from': 'userA',
            'to': 'userB'
        })
        handler.close()

        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('app.follow', data[0][0])
        eq('userA', data[0][2]['from'])
        eq('userB', data[0][2]['to'])
        self.assertTrue(data[0][1])
        self.assertTrue(isinstance(data[0][1], int))

    def test_custom_fmt(self):
        handler = fluent.handler.FluentHandler('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(
            fluent.handler.FluentRecordFormatter(fmt={
                'name': '%(name)s',
                'lineno': '%(lineno)d',
            })
        )
        log.addHandler(handler)
        log.info({'sample': 'value'})
        handler.close()

        data = self.get_data()
        self.assertTrue('name' in data[0][2])
        self.assertEqual('fluent.test', data[0][2]['name'])
        self.assertTrue('lineno' in data[0][2])

    def test_unstructured_message(self):
        handler = fluent.handler.FluentHandler('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info('hello world')
        handler.close()

        data = self.get_data()
        self.assertTrue('message' in data[0][2])
        self.assertEqual('hello world', data[0][2]['message'])

    def test_non_string_simple_message(self):
        handler = fluent.handler.FluentHandler('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info(42)
        handler.close()

        data = self.get_data()
        self.assertTrue('message' in data[0][2])

    def test_non_string_dict_message(self):
        handler = fluent.handler.FluentHandler('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info({42: 'root'})
        handler.close()

        data = self.get_data()
        # For some reason, non-string keys are ignored
        self.assertFalse(42 in data[0][2])
