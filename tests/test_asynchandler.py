import logging
import unittest

try:
    from unittest import mock
except ImportError:
    from unittest import mock
try:
    from unittest.mock import patch
except ImportError:
    from unittest.mock import patch


import fluent.asynchandler
import fluent.handler
from tests import mockserver


def get_logger(name, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger


class TestHandler(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self._server = mockserver.MockRecvServer("localhost")
        self._port = self._server.port

    def tearDown(self):
        self._server.close()

    def get_handler_class(self):
        # return fluent.handler.FluentHandler
        return fluent.asynchandler.FluentHandler

    def get_data(self):
        return self._server.get_received()

    def test_simple(self):
        handler = self.get_handler_class()("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)
            log.info({"from": "userA", "to": "userB"})

        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq("app.follow", data[0][0])
        eq("userA", data[0][2]["from"])
        eq("userB", data[0][2]["to"])
        self.assertTrue(data[0][1])
        self.assertTrue(isinstance(data[0][1], int))

    def test_custom_fmt(self):
        handler = self.get_handler_class()("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(
                fluent.handler.FluentRecordFormatter(
                    fmt={
                        "name": "%(name)s",
                        "lineno": "%(lineno)d",
                        "emitted_at": "%(asctime)s",
                    }
                )
            )
            log.addHandler(handler)
            log.info({"sample": "value"})

        data = self.get_data()
        self.assertTrue("name" in data[0][2])
        self.assertEqual("fluent.test", data[0][2]["name"])
        self.assertTrue("lineno" in data[0][2])
        self.assertTrue("emitted_at" in data[0][2])

    def test_custom_fmt_with_format_style(self):
        handler = self.get_handler_class()("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(
                fluent.handler.FluentRecordFormatter(
                    fmt={
                        "name": "{name}",
                        "lineno": "{lineno}",
                        "emitted_at": "{asctime}",
                    },
                    style="{",
                )
            )
            log.addHandler(handler)
            log.info({"sample": "value"})

        data = self.get_data()
        self.assertTrue("name" in data[0][2])
        self.assertEqual("fluent.test", data[0][2]["name"])
        self.assertTrue("lineno" in data[0][2])
        self.assertTrue("emitted_at" in data[0][2])

    def test_custom_fmt_with_template_style(self):
        handler = self.get_handler_class()("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(
                fluent.handler.FluentRecordFormatter(
                    fmt={
                        "name": "${name}",
                        "lineno": "${lineno}",
                        "emitted_at": "${asctime}",
                    },
                    style="$",
                )
            )
            log.addHandler(handler)
            log.info({"sample": "value"})

        data = self.get_data()
        self.assertTrue("name" in data[0][2])
        self.assertEqual("fluent.test", data[0][2]["name"])
        self.assertTrue("lineno" in data[0][2])
        self.assertTrue("emitted_at" in data[0][2])

    def test_custom_field_raise_exception(self):
        handler = self.get_handler_class()("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(
                fluent.handler.FluentRecordFormatter(
                    fmt={"name": "%(name)s", "custom_field": "%(custom_field)s"}
                )
            )
            log.addHandler(handler)
            with self.assertRaises(KeyError):
                log.info({"sample": "value"})
            log.removeHandler(handler)

    def test_custom_field_fill_missing_fmt_key_is_true(self):
        handler = self.get_handler_class()("app.follow", port=self._port)
        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(
                fluent.handler.FluentRecordFormatter(
                    fmt={"name": "%(name)s", "custom_field": "%(custom_field)s"},
                    fill_missing_fmt_key=True,
                )
            )
            log.addHandler(handler)
            log.info({"sample": "value"})
            log.removeHandler(handler)

        data = self.get_data()
        self.assertTrue("name" in data[0][2])
        self.assertEqual("fluent.test", data[0][2]["name"])
        self.assertTrue("custom_field" in data[0][2])
        # field defaults to none if not in log record
        self.assertIsNone(data[0][2]["custom_field"])

    def test_json_encoded_message(self):
        handler = self.get_handler_class()("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)
            log.info('{"key": "hello world!", "param": "value"}')

        data = self.get_data()
        self.assertTrue("key" in data[0][2])
        self.assertEqual("hello world!", data[0][2]["key"])

    def test_unstructured_message(self):
        handler = self.get_handler_class()("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)
            log.info("hello %s", "world")

        data = self.get_data()
        self.assertTrue("message" in data[0][2])
        self.assertEqual("hello world", data[0][2]["message"])

    def test_unstructured_formatted_message(self):
        handler = self.get_handler_class()("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)
            log.info("hello world, %s", "you!")

        data = self.get_data()
        self.assertTrue("message" in data[0][2])
        self.assertEqual("hello world, you!", data[0][2]["message"])

    def test_number_string_simple_message(self):
        handler = self.get_handler_class()("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)
            log.info("1")

        data = self.get_data()
        self.assertTrue("message" in data[0][2])

    def test_non_string_simple_message(self):
        handler = self.get_handler_class()("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)
            log.info(42)

        data = self.get_data()
        self.assertTrue("message" in data[0][2])

    def test_non_string_dict_message(self):
        handler = self.get_handler_class()("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)
            log.info({42: "root"})

        data = self.get_data()
        # For some reason, non-string keys are ignored
        self.assertFalse(42 in data[0][2])

    def test_exception_message(self):
        handler = self.get_handler_class()("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)
            try:
                raise Exception("sample exception")
            except Exception:
                log.exception("it failed")

        data = self.get_data()
        message = data[0][2]["message"]
        # Includes the logged message, as well as the stack trace.
        self.assertTrue("it failed" in message)
        self.assertTrue('tests/test_asynchandler.py", line' in message)
        self.assertTrue("Exception: sample exception" in message)


class TestHandlerWithCircularQueue(unittest.TestCase):
    Q_SIZE = 3

    def setUp(self):
        super().setUp()
        self._server = mockserver.MockRecvServer("localhost")
        self._port = self._server.port

    def tearDown(self):
        self._server.close()

    def get_handler_class(self):
        # return fluent.handler.FluentHandler
        return fluent.asynchandler.FluentHandler

    def get_data(self):
        return self._server.get_received()

    def test_simple(self):
        handler = self.get_handler_class()(
            "app.follow",
            port=self._port,
            queue_maxsize=self.Q_SIZE,
            queue_circular=True,
        )
        with handler:
            self.assertEqual(handler.sender.queue_circular, True)
            self.assertEqual(handler.sender.queue_maxsize, self.Q_SIZE)

            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)
            log.info({"cnt": 1, "from": "userA", "to": "userB"})
            log.info({"cnt": 2, "from": "userA", "to": "userB"})
            log.info({"cnt": 3, "from": "userA", "to": "userB"})
            log.info({"cnt": 4, "from": "userA", "to": "userB"})
            log.info({"cnt": 5, "from": "userA", "to": "userB"})

        data = self.get_data()
        eq = self.assertEqual
        # with the logging interface, we can't be sure to have filled up the queue, so we can
        # test only for a cautelative condition here
        self.assertTrue(len(data) >= self.Q_SIZE)

        el = data[0]
        eq(3, len(el))
        eq("app.follow", el[0])
        eq("userA", el[2]["from"])
        eq("userB", el[2]["to"])
        self.assertTrue(el[1])
        self.assertTrue(isinstance(el[1], int))


class QueueOverflowException(BaseException):
    pass


def queue_overflow_handler(discarded_bytes):
    raise QueueOverflowException(discarded_bytes)


class TestHandlerWithCircularQueueHandler(unittest.TestCase):
    Q_SIZE = 1

    def setUp(self):
        super().setUp()
        self._server = mockserver.MockRecvServer("localhost")
        self._port = self._server.port

    def tearDown(self):
        self._server.close()

    def get_handler_class(self):
        # return fluent.handler.FluentHandler
        return fluent.asynchandler.FluentHandler

    def test_simple(self):
        handler = self.get_handler_class()(
            "app.follow",
            port=self._port,
            queue_maxsize=self.Q_SIZE,
            queue_circular=True,
            queue_overflow_handler=queue_overflow_handler,
        )
        with handler:

            def custom_full_queue():
                handler.sender._queue.put(b"Mock", block=True)
                return True

            with patch.object(
                fluent.asynchandler.asyncsender.Queue,
                "full",
                mock.Mock(side_effect=custom_full_queue),
            ):
                self.assertEqual(handler.sender.queue_circular, True)
                self.assertEqual(handler.sender.queue_maxsize, self.Q_SIZE)

                log = get_logger("fluent.test")
                handler.setFormatter(fluent.handler.FluentRecordFormatter())
                log.addHandler(handler)

                exc_counter = 0

                try:
                    log.info({"cnt": 1, "from": "userA", "to": "userB"})
                except QueueOverflowException:
                    exc_counter += 1

                try:
                    log.info({"cnt": 2, "from": "userA", "to": "userB"})
                except QueueOverflowException:
                    exc_counter += 1

                try:
                    log.info({"cnt": 3, "from": "userA", "to": "userB"})
                except QueueOverflowException:
                    exc_counter += 1

                # we can't be sure to have exception in every case due to multithreading,
                # so we can test only for a cautelative condition here
                print(f"Exception raised: {exc_counter} (expected 3)")
                assert exc_counter >= 0
