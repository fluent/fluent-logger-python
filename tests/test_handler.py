import logging
import unittest

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

    def get_data(self):
        return self._server.get_received()

    def test_simple(self):
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)

            log.info({"from": "userA", "to": "userB"})

            log.removeHandler(handler)

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
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

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
            log.removeHandler(handler)

        data = self.get_data()
        self.assertTrue("name" in data[0][2])
        self.assertEqual("fluent.test", data[0][2]["name"])
        self.assertTrue("lineno" in data[0][2])
        self.assertTrue("emitted_at" in data[0][2])

    def test_exclude_attrs(self):
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter(exclude_attrs=[]))
            log.addHandler(handler)
            log.info({"sample": "value"})
            log.removeHandler(handler)

        data = self.get_data()
        self.assertTrue("name" in data[0][2])
        self.assertEqual("fluent.test", data[0][2]["name"])
        self.assertTrue("lineno" in data[0][2])

    def test_exclude_attrs_with_exclusion(self):
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(
                fluent.handler.FluentRecordFormatter(exclude_attrs=["funcName"])
            )
            log.addHandler(handler)
            log.info({"sample": "value"})
            log.removeHandler(handler)

        data = self.get_data()
        self.assertTrue("name" in data[0][2])
        self.assertEqual("fluent.test", data[0][2]["name"])
        self.assertTrue("lineno" in data[0][2])

    def test_exclude_attrs_with_extra(self):
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter(exclude_attrs=[]))
            log.addHandler(handler)
            log.info("Test with value '%s'", "test value", extra={"x": 1234})
            log.removeHandler(handler)

        data = self.get_data()
        self.assertTrue("name" in data[0][2])
        self.assertEqual("fluent.test", data[0][2]["name"])
        self.assertTrue("lineno" in data[0][2])
        self.assertEqual("Test with value 'test value'", data[0][2]["message"])
        self.assertEqual(1234, data[0][2]["x"])

    def test_format_dynamic(self):
        def formatter(record):
            return {"message": record.message, "x": record.x, "custom_value": 1}

        formatter.usesTime = lambda: True

        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter(fmt=formatter))
            log.addHandler(handler)
            log.info("Test with value '%s'", "test value", extra={"x": 1234})
            log.removeHandler(handler)

        data = self.get_data()
        self.assertTrue("x" in data[0][2])
        self.assertEqual(1234, data[0][2]["x"])
        self.assertEqual(1, data[0][2]["custom_value"])

    def test_custom_fmt_with_format_style(self):
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

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
            log.removeHandler(handler)

        data = self.get_data()
        self.assertTrue("name" in data[0][2])
        self.assertEqual("fluent.test", data[0][2]["name"])
        self.assertTrue("lineno" in data[0][2])
        self.assertTrue("emitted_at" in data[0][2])

    def test_custom_fmt_with_template_style(self):
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

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
            log.removeHandler(handler)

        data = self.get_data()
        self.assertTrue("name" in data[0][2])
        self.assertEqual("fluent.test", data[0][2]["name"])
        self.assertTrue("lineno" in data[0][2])
        self.assertTrue("emitted_at" in data[0][2])

    def test_custom_field_raise_exception(self):
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

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
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

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
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)

            log.info('{"key": "hello world!", "param": "value"}')

            log.removeHandler(handler)

        data = self.get_data()
        self.assertTrue("key" in data[0][2])
        self.assertEqual("hello world!", data[0][2]["key"])

    def test_json_encoded_message_without_json(self):
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(
                fluent.handler.FluentRecordFormatter(format_json=False)
            )
            log.addHandler(handler)

            log.info('{"key": "hello world!", "param": "value"}')

            log.removeHandler(handler)

        data = self.get_data()
        self.assertTrue("key" not in data[0][2])
        self.assertEqual(
            '{"key": "hello world!", "param": "value"}', data[0][2]["message"]
        )

    def test_unstructured_message(self):
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)
            log.info("hello %s", "world")
            log.removeHandler(handler)

        data = self.get_data()
        self.assertTrue("message" in data[0][2])
        self.assertEqual("hello world", data[0][2]["message"])

    def test_unstructured_formatted_message(self):
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)
            log.info("hello world, %s", "you!")
            log.removeHandler(handler)

        data = self.get_data()
        self.assertTrue("message" in data[0][2])
        self.assertEqual("hello world, you!", data[0][2]["message"])

    def test_number_string_simple_message(self):
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)
            log.info("1")
            log.removeHandler(handler)

        data = self.get_data()
        self.assertTrue("message" in data[0][2])

    def test_non_string_simple_message(self):
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)
            log.info(42)
            log.removeHandler(handler)

        data = self.get_data()
        self.assertTrue("message" in data[0][2])

    def test_non_string_dict_message(self):
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)
            log.info({42: "root"})
            log.removeHandler(handler)

        data = self.get_data()
        # For some reason, non-string keys are ignored
        self.assertFalse(42 in data[0][2])

    def test_exception_message(self):
        handler = fluent.handler.FluentHandler("app.follow", port=self._port)

        with handler:
            log = get_logger("fluent.test")
            handler.setFormatter(fluent.handler.FluentRecordFormatter())
            log.addHandler(handler)
            try:
                raise Exception("sample exception")
            except Exception:
                log.exception("it failed")
            log.removeHandler(handler)

        data = self.get_data()
        message = data[0][2]["message"]
        # Includes the logged message, as well as the stack trace.
        self.assertTrue("it failed" in message)
        self.assertTrue('tests/test_handler.py", line' in message)
        self.assertTrue("Exception: sample exception" in message)
