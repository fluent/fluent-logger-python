# -*- coding: utf-8 -*-

import logging
import unittest

import fluent.handler

from tests.mockserver import create_server


class BaseTestHandler(object):
    ADDR = ""

    def setUp(self):
        super(BaseTestHandler, self).setUp()
        self._server = create_server(self.ADDR)

    def tearDown(self):
        self._server.close()

    def create_handler(self, tag):
        return fluent.handler.FluentHandler(tag, host=self._server.addr())

    def get_messages(self, qty=1):
        return self._server.recv(qty)

    def test_simple(self):
        handler = self.create_handler('app.follow')

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info({
            'from': 'userA',
            'to': 'userB'
        })
        handler.close()

        data = self.get_messages(1)
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('app.follow', data[0][0])
        eq('userA', data[0][2]['from'])
        eq('userB', data[0][2]['to'])
        self.assertTrue(data[0][1])
        self.assertTrue(isinstance(data[0][1], int))

    def test_custom_fmt(self):
        handler = self.create_handler('app.follow')

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(
            fluent.handler.FluentRecordFormatter(fmt={
                'name': '%(name)s',
                'lineno': '%(lineno)d',
                'emitted_at': '%(asctime)s',
            })
        )
        log.addHandler(handler)
        log.info({'sample': 'value'})
        handler.close()

        data = self.get_messages()
        self.assertTrue('name' in data[0][2])
        self.assertEqual('fluent.test', data[0][2]['name'])
        self.assertTrue('lineno' in data[0][2])
        self.assertTrue('emitted_at' in data[0][2])

    def test_custom_field_raise_exception(self):
        handler = self.create_handler('app.follow')

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(
            fluent.handler.FluentRecordFormatter(fmt={
                'name': '%(name)s',
                'custom_field': '%(custom_field)s'
            })
        )
        log.addHandler(handler)
        with self.assertRaises(KeyError):
            log.info({'sample': 'value'})
        log.removeHandler(handler)
        handler.close()

    def test_custom_field_fill_missing_fmt_key_is_true(self):
        handler = self.create_handler('app.follow')

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(
            fluent.handler.FluentRecordFormatter(fmt={
                'name': '%(name)s',
                'custom_field': '%(custom_field)s'
                },
                fill_missing_fmt_key=True
            )
        )
        log.addHandler(handler)
        log.info({'sample': 'value'})
        log.removeHandler(handler)
        handler.close()

        data = self.get_messages()
        self.assertTrue('name' in data[0][2])
        self.assertEqual('fluent.test', data[0][2]['name'])
        self.assertTrue('custom_field' in data[0][2])
        # field defaults to none if not in log record
        self.assertIsNone(data[0][2]['custom_field'])

    def test_json_encoded_message(self):
        handler = self.create_handler('app.follow')

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info('{"key": "hello world!", "param": "value"}')
        handler.close()

        data = self.get_messages()
        self.assertTrue('key' in data[0][2])
        self.assertEqual('hello world!', data[0][2]['key'])

    def test_unstructured_message(self):
        handler = self.create_handler('app.follow')

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info('hello %s', 'world')
        handler.close()

        data = self.get_messages()
        self.assertTrue('message' in data[0][2])
        self.assertEqual('hello world', data[0][2]['message'])

    def test_unstructured_formatted_message(self):
        handler = self.create_handler('app.follow')

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info('hello world, %s', 'you!')
        handler.close()

        data = self.get_messages()
        self.assertTrue('message' in data[0][2])
        self.assertEqual('hello world, you!', data[0][2]['message'])

    def test_number_string_simple_message(self):
        handler = self.create_handler('app.follow')

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info("1")
        handler.close()

        data = self.get_messages()
        self.assertTrue('message' in data[0][2])

    def test_non_string_simple_message(self):
        handler = self.create_handler('app.follow')

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info(42)
        handler.close()

        data = self.get_messages()
        self.assertTrue('message' in data[0][2])

    def test_non_string_dict_message(self):
        handler = self.create_handler('app.follow')

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info({42: 'root'})
        handler.close()

        data = self.get_messages()
        # For some reason, non-string keys are ignored
        self.assertFalse(42 in data[0][2])


class TestHandler_TCP(BaseTestHandler, unittest.TestCase):
    ADDR = 'tcp://localhost'


class TestHandler_UDP(BaseTestHandler, unittest.TestCase):
    ADDR = 'udp://localhost'
