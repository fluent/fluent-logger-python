import json
import logging
import socket

from fluent import sender


class FluentRecordFormatter(logging.Formatter):
    """A structured formatter for Fluent.

    Best used with server storing data in an ElasticSearch cluster for example.

    :param fmt: a dict or a callable with format string as values to map to provided keys.
        If callable, should accept a single argument `LogRecord` and return a dict,
        and have a field `usesTime` that is callable and return a bool as would
        `FluentRecordFormatter.usesTime`
    :param datefmt: strftime()-compatible date/time format string.
    :param style: '%', '{' or '$' (used only with Python 3.2 or above)
    :param fill_missing_fmt_key: if True, do not raise a KeyError if the format
        key is not found. Put None if not found.
    :param format_json: if True, will attempt to parse message as json. If not,
        will use message as-is. Defaults to True
    :param exclude_attrs: switches this formatter into a mode where all attributes
        except the ones specified by `exclude_attrs` are logged with the record as is.
        If `None`, operates as before, otherwise `fmt` is ignored.
        Can be an iterable.
    """

    def __init__(
        self,
        fmt=None,
        datefmt=None,
        style="%",
        fill_missing_fmt_key=False,
        format_json=True,
        exclude_attrs=None,
    ):
        super().__init__(None, datefmt)

        if style != "%":
            self.__style, basic_fmt_dict = {
                "{": (
                    logging.StrFormatStyle,
                    {
                        "sys_host": "{hostname}",
                        "sys_name": "{name}",
                        "sys_module": "{module}",
                    },
                ),
                "$": (
                    logging.StringTemplateStyle,
                    {
                        "sys_host": "${hostname}",
                        "sys_name": "${name}",
                        "sys_module": "${module}",
                    },
                ),
            }[style]
        else:
            self.__style = None
            basic_fmt_dict = {
                "sys_host": "%(hostname)s",
                "sys_name": "%(name)s",
                "sys_module": "%(module)s",
            }

        if exclude_attrs is not None:
            self._exc_attrs = set(exclude_attrs)
            self._fmt_dict = None
            self._formatter = self._format_by_exclusion
            self.usesTime = super().usesTime
        else:
            self._exc_attrs = None
            if not fmt:
                self._fmt_dict = basic_fmt_dict
                self._formatter = self._format_by_dict
                self.usesTime = self._format_by_dict_uses_time
            else:
                if callable(fmt):
                    self._formatter = fmt
                    self.usesTime = fmt.usesTime
                else:
                    self._fmt_dict = fmt
                    self._formatter = self._format_by_dict
                    self.usesTime = self._format_by_dict_uses_time

        if format_json:
            self._format_msg = self._format_msg_json
        else:
            self._format_msg = self._format_msg_default

        self.hostname = socket.gethostname()

        self.fill_missing_fmt_key = fill_missing_fmt_key

    def format(self, record):
        # Compute attributes handled by parent class.
        super().format(record)
        # Add ours
        record.hostname = self.hostname

        # Apply format
        data = self._formatter(record)

        self._structuring(data, record)
        return data

    def usesTime(self):
        """This method is substituted on construction based on settings for performance reasons"""

    def _structuring(self, data, record):
        """Melds `msg` into `data`.

        :param data: dictionary to be sent to fluent server
        :param msg: :class:`LogRecord`'s message to add to `data`.
          `msg` can be a simple string for backward compatibility with
          :mod:`logging` framework, a JSON encoded string or a dictionary
          that will be merged into dictionary generated in :meth:`format.
        """
        msg = record.msg

        if isinstance(msg, dict):
            self._add_dic(data, msg)
        elif isinstance(msg, str):
            self._add_dic(data, self._format_msg(record, msg))
        else:
            self._add_dic(data, {"message": msg})

    def _format_msg_json(self, record, msg):
        try:
            json_msg = json.loads(str(msg))
            if isinstance(json_msg, dict):
                return json_msg
            else:
                return self._format_msg_default(record, msg)
        except ValueError:
            return self._format_msg_default(record, msg)

    def _format_msg_default(self, record, msg):
        return {"message": super().format(record)}

    def _format_by_exclusion(self, record):
        data = {}
        for key, value in record.__dict__.items():
            if key not in self._exc_attrs:
                data[key] = value
        return data

    def _format_by_dict(self, record):
        data = {}
        for key, value in self._fmt_dict.items():
            try:
                if self.__style:
                    value = self.__style(value).format(record)
                else:
                    value = value % record.__dict__
            except KeyError as exc:
                value = None
                if not self.fill_missing_fmt_key:
                    raise exc

            data[key] = value
        return data

    def _format_by_dict_uses_time(self):
        if self.__style:
            search = self.__style.asctime_search
        else:
            search = "%(asctime)"
        return any([value.find(search) >= 0 for value in self._fmt_dict.values()])

    @staticmethod
    def _add_dic(data, dic):
        for key, value in dic.items():
            if isinstance(key, str):
                data[key] = value


class FluentHandler(logging.Handler):
    """
    Logging Handler for fluent.
    """

    def __init__(
        self,
        tag,
        host="localhost",
        port=24224,
        timeout=3.0,
        verbose=False,
        buffer_overflow_handler=None,
        msgpack_kwargs=None,
        nanosecond_precision=False,
        **kwargs,
    ):
        self.tag = tag
        self._host = host
        self._port = port
        self._timeout = timeout
        self._verbose = verbose
        self._buffer_overflow_handler = buffer_overflow_handler
        self._msgpack_kwargs = msgpack_kwargs
        self._nanosecond_precision = nanosecond_precision
        self._kwargs = kwargs
        self._sender = None
        logging.Handler.__init__(self)

    def getSenderClass(self):
        return sender.FluentSender

    @property
    def sender(self):
        if self._sender is None:
            self._sender = self.getSenderInstance(
                tag=self.tag,
                host=self._host,
                port=self._port,
                timeout=self._timeout,
                verbose=self._verbose,
                buffer_overflow_handler=self._buffer_overflow_handler,
                msgpack_kwargs=self._msgpack_kwargs,
                nanosecond_precision=self._nanosecond_precision,
                **self._kwargs,
            )
        return self._sender

    def getSenderInstance(
        self,
        tag,
        host,
        port,
        timeout,
        verbose,
        buffer_overflow_handler,
        msgpack_kwargs,
        nanosecond_precision,
        **kwargs,
    ):
        sender_class = self.getSenderClass()
        return sender_class(
            tag,
            host=host,
            port=port,
            timeout=timeout,
            verbose=verbose,
            buffer_overflow_handler=buffer_overflow_handler,
            msgpack_kwargs=msgpack_kwargs,
            nanosecond_precision=nanosecond_precision,
            **kwargs,
        )

    def emit(self, record):
        data = self.format(record)
        _sender = self.sender
        return _sender.emit_with_time(
            None,
            sender.EventTime(record.created)
            if _sender.nanosecond_precision
            else int(record.created),
            data,
        )

    def close(self):
        self.acquire()
        try:
            try:
                if self._sender is not None:
                    self._sender.close()
                    self._sender = None
            finally:
                super().close()
        finally:
            self.release()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
