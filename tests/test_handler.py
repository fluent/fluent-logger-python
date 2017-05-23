# -*- coding: utf-8 -*-

import logging
import sys
import unittest

import fluent.handler

from tests import mockserver


class TestHandler(unittest.TestCase):
    def setUp(self):
        super(TestHandler, self).setUp()
        self._server = mockserver.MockRecvServer('localhost')
        self._port = self._server.port

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
                'emitted_at': '%(asctime)s',
            })
        )
        log.addHandler(handler)
        log.info({'sample': 'value'})
        handler.close()

        data = self.get_data()
        self.assertTrue('name' in data[0][2])
        self.assertEqual('fluent.test', data[0][2]['name'])
        self.assertTrue('lineno' in data[0][2])
        self.assertTrue('emitted_at' in data[0][2])

    @unittest.skipUnless(sys.version_info[0:2] >= (3, 2), 'supported with Python 3.2 or above')
    def test_custom_fmt_with_format_style(self):
        handler = fluent.handler.FluentHandler('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(
            fluent.handler.FluentRecordFormatter(fmt={
                'name': '{name}',
                'lineno': '{lineno}',
                'emitted_at': '{asctime}',
            }, style='{')
        )
        log.addHandler(handler)
        log.info({'sample': 'value'})
        handler.close()

        data = self.get_data()
        self.assertTrue('name' in data[0][2])
        self.assertEqual('fluent.test', data[0][2]['name'])
        self.assertTrue('lineno' in data[0][2])
        self.assertTrue('emitted_at' in data[0][2])

    @unittest.skipUnless(sys.version_info[0:2] >= (3, 2), 'supported with Python 3.2 or above')
    def test_custom_fmt_with_template_style(self):
        handler = fluent.handler.FluentHandler('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(
            fluent.handler.FluentRecordFormatter(fmt={
                'name': '${name}',
                'lineno': '${lineno}',
                'emitted_at': '${asctime}',
            }, style='$')
        )
        log.addHandler(handler)
        log.info({'sample': 'value'})
        handler.close()

        data = self.get_data()
        self.assertTrue('name' in data[0][2])
        self.assertEqual('fluent.test', data[0][2]['name'])
        self.assertTrue('lineno' in data[0][2])
        self.assertTrue('emitted_at' in data[0][2])

    def test_custom_field_raise_exception(self):
        handler = fluent.handler.FluentHandler('app.follow', port=self._port)

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
        handler = fluent.handler.FluentHandler('app.follow', port=self._port)

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

        data = self.get_data()
        self.assertTrue('name' in data[0][2])
        self.assertEqual('fluent.test', data[0][2]['name'])
        self.assertTrue('custom_field' in data[0][2])
        # field defaults to none if not in log record
        self.assertIsNone(data[0][2]['custom_field'])

    def test_custom_field_convert_none_strings(self):
        handler = fluent.handler.FluentHandler('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(
            fluent.handler.FluentRecordFormatter(fmt={
                'name': '%(name)s',
                }
            )
        )
        log.addHandler(handler)
        log.info({'name': 'None', 'sample':''})
        log.removeHandler(handler)
        handler.close()

        data = self.get_data()
        # field should be none
        self.assertIsNone(data[0][2]['name'])
        self.assertIsNone(data[0][2]['sample'])


    def test_json_encoded_message(self):
        handler = fluent.handler.FluentHandler('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info('{"key": "hello world!", "param": "value"}')
        handler.close()

        data = self.get_data()
        self.assertTrue('key' in data[0][2])
        self.assertEqual('hello world!', data[0][2]['key'])

    def test_unstructured_message(self):
        handler = fluent.handler.FluentHandler('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info('hello %s', 'world')
        handler.close()

        data = self.get_data()
        self.assertTrue('message' in data[0][2])
        self.assertEqual('hello world', data[0][2]['message'])

    def test_unstructured_formatted_message(self):
        handler = fluent.handler.FluentHandler('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info('hello world, %s', 'you!')
        handler.close()

        data = self.get_data()
        self.assertTrue('message' in data[0][2])
        self.assertEqual('hello world, you!', data[0][2]['message'])

    def test_number_string_simple_message(self):
        handler = fluent.handler.FluentHandler('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info("1")
        handler.close()

        data = self.get_data()
        self.assertTrue('message' in data[0][2])

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
