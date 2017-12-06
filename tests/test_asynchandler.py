# -*- coding: utf-8 -*-

import logging
import sys
import unittest
import time

import fluent.handler
import fluent.asynchandler

from tests import mockserver


class TestHandler(unittest.TestCase):
    def setUp(self):
        super(TestHandler, self).setUp()
        self._server = mockserver.MockRecvServer('localhost')
        self._port = self._server.port
        self.handler = None

    def get_handler_class(self):
        # return fluent.handler.FluentHandler
        return fluent.asynchandler.FluentHandler

    def get_data(self):
        return self._server.get_recieved()

    def test_simple(self):
        handler = self.get_handler_class()('app.follow', port=self._port)
        self.handler = handler

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info({
            'from': 'userA',
            'to': 'userB'
        })

        # wait, giving time to the communicator thread to send the messages
        time.sleep(0.5)
        # close the handler, to join the thread and let the test suite to terminate
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
        handler = self.get_handler_class()('app.follow', port=self._port)

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
        # wait, giving time to the communicator thread to send the messages
        time.sleep(0.5)
        # close the handler, to join the thread and let the test suite to terminate
        handler.close()

        data = self.get_data()
        self.assertTrue('name' in data[0][2])
        self.assertEqual('fluent.test', data[0][2]['name'])
        self.assertTrue('lineno' in data[0][2])
        self.assertTrue('emitted_at' in data[0][2])

    @unittest.skipUnless(sys.version_info[0:2] >= (3, 2), 'supported with Python 3.2 or above')
    def test_custom_fmt_with_format_style(self):
        handler = self.get_handler_class()('app.follow', port=self._port)

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
        # wait, giving time to the communicator thread to send the messages
        time.sleep(0.5)
        # close the handler, to join the thread and let the test suite to terminate
        handler.close()

        data = self.get_data()
        self.assertTrue('name' in data[0][2])
        self.assertEqual('fluent.test', data[0][2]['name'])
        self.assertTrue('lineno' in data[0][2])
        self.assertTrue('emitted_at' in data[0][2])

    @unittest.skipUnless(sys.version_info[0:2] >= (3, 2), 'supported with Python 3.2 or above')
    def test_custom_fmt_with_template_style(self):
        handler = self.get_handler_class()('app.follow', port=self._port)

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
        # wait, giving time to the communicator thread to send the messages
        time.sleep(0.5)
        # close the handler, to join the thread and let the test suite to terminate
        handler.close()

        data = self.get_data()
        self.assertTrue('name' in data[0][2])
        self.assertEqual('fluent.test', data[0][2]['name'])
        self.assertTrue('lineno' in data[0][2])
        self.assertTrue('emitted_at' in data[0][2])

    def test_custom_field_raise_exception(self):
        handler = self.get_handler_class()('app.follow', port=self._port)

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
        # wait, giving time to the communicator thread to send the messages
        time.sleep(0.5)
        # close the handler, to join the thread and let the test suite to terminate
        handler.close()

    def test_custom_field_fill_missing_fmt_key_is_true(self):
        handler = self.get_handler_class()('app.follow', port=self._port)

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
        # wait, giving time to the communicator thread to send the messages
        time.sleep(0.5)
        # close the handler, to join the thread and let the test suite to terminate
        handler.close()

        data = self.get_data()
        self.assertTrue('name' in data[0][2])
        self.assertEqual('fluent.test', data[0][2]['name'])
        self.assertTrue('custom_field' in data[0][2])
        # field defaults to none if not in log record
        self.assertIsNone(data[0][2]['custom_field'])

    def test_json_encoded_message(self):
        handler = self.get_handler_class()('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info('{"key": "hello world!", "param": "value"}')
        # wait, giving time to the communicator thread to send the messages
        time.sleep(0.5)
        # close the handler, to join the thread and let the test suite to terminate
        handler.close()

        data = self.get_data()
        self.assertTrue('key' in data[0][2])
        self.assertEqual('hello world!', data[0][2]['key'])

    def test_unstructured_message(self):
        handler = self.get_handler_class()('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info('hello %s', 'world')
        # wait, giving time to the communicator thread to send the messages
        time.sleep(0.5)
        # close the handler, to join the thread and let the test suite to terminate
        handler.close()

        data = self.get_data()
        self.assertTrue('message' in data[0][2])
        self.assertEqual('hello world', data[0][2]['message'])

    def test_unstructured_formatted_message(self):
        handler = self.get_handler_class()('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info('hello world, %s', 'you!')
        # wait, giving time to the communicator thread to send the messages
        time.sleep(0.5)
        # close the handler, to join the thread and let the test suite to terminate
        handler.close()

        data = self.get_data()
        self.assertTrue('message' in data[0][2])
        self.assertEqual('hello world, you!', data[0][2]['message'])

    def test_number_string_simple_message(self):
        handler = self.get_handler_class()('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info("1")
        # wait, giving time to the communicator thread to send the messages
        time.sleep(0.5)
        # close the handler, to join the thread and let the test suite to terminate
        handler.close()

        data = self.get_data()
        self.assertTrue('message' in data[0][2])

    def test_non_string_simple_message(self):
        handler = self.get_handler_class()('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info(42)
        # wait, giving time to the communicator thread to send the messages
        time.sleep(0.5)
        # close the handler, to join the thread and let the test suite to terminate
        handler.close()

        data = self.get_data()
        self.assertTrue('message' in data[0][2])

    def test_non_string_dict_message(self):
        handler = self.get_handler_class()('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info({42: 'root'})
        # wait, giving time to the communicator thread to send the messages
        time.sleep(0.5)
        # close the handler, to join the thread and let the test suite to terminate
        handler.close()

        data = self.get_data()
        # For some reason, non-string keys are ignored
        self.assertFalse(42 in data[0][2])


class TestHandlerWithCircularQueue(unittest.TestCase):
    Q_TIMEOUT = 0.04
    Q_SIZE = 3

    def setUp(self):
        super(TestHandlerWithCircularQueue, self).setUp()
        self._server = mockserver.MockRecvServer('localhost')
        self._port = self._server.port
        self.handler = None

    def get_handler_class(self):
        # return fluent.handler.FluentHandler
        return fluent.asynchandler.FluentHandler

    def get_data(self):
        return self._server.get_recieved()

    def test_simple(self):
        handler = self.get_handler_class()('app.follow', port=self._port,
                                           queue_timeout=self.Q_TIMEOUT,
                                           queue_maxsize=self.Q_SIZE,
                                           queue_circular=True)
        self.handler = handler

        self.assertEqual(self.handler.sender.queue_circular, True)
        self.assertEqual(self.handler.sender.queue_maxsize, self.Q_SIZE)

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('fluent.test')
        handler.setFormatter(fluent.handler.FluentRecordFormatter())
        log.addHandler(handler)
        log.info({'cnt': 1, 'from': 'userA', 'to': 'userB'})
        log.info({'cnt': 2, 'from': 'userA', 'to': 'userB'})
        log.info({'cnt': 3, 'from': 'userA', 'to': 'userB'})
        log.info({'cnt': 4, 'from': 'userA', 'to': 'userB'})
        log.info({'cnt': 5, 'from': 'userA', 'to': 'userB'})

        # wait, giving time to the communicator thread to send the messages
        time.sleep(0.5)
        # close the handler, to join the thread and let the test suite to terminate
        handler.close()

        data = self.get_data()
        eq = self.assertEqual
        # with the logging interface, we can't be sure to have filled up the queue, so we can
        # test only for a cautelative condition here
        self.assertTrue(len(data) >= self.Q_SIZE)

        el = data[0]
        eq(3, len(el))
        eq('app.follow', el[0])
        eq('userA', el[2]['from'])
        eq('userB', el[2]['to'])
        self.assertTrue(el[1])
        self.assertTrue(isinstance(el[1], int))
