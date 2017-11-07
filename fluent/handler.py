# -*- coding: utf-8 -*-

import logging
import socket
import sys

try:
    import simplejson as json
except ImportError:  # pragma: no cover
    import json

try:
    basestring
except NameError:  # pragma: no cover
    basestring = (str, bytes)

from fluent import sender


class FluentRecordFormatter(logging.Formatter, object):
    """ A structured formatter for Fluent.

    Best used with server storing data in an ElasticSearch cluster for example.

    :param fmt: a dict with format string as values to map to provided keys.
    :param datefmt: strftime()-compatible date/time format string.
    :param style: '%', '{' or '$' (used only with Python 3.2 or above)
    :param fill_missing_fmt_key: if True, do not raise a KeyError if the format
        key is not found. Put None if not found.s
    """
    def __init__(self, fmt=None, datefmt=None, style='%', fill_missing_fmt_key=False):
        super(FluentRecordFormatter, self).__init__(None, datefmt)

        if sys.version_info[0:2] >= (3, 2) and style != '%':
            self.__style, basic_fmt_dict = {
                '{': (logging.StrFormatStyle, {
                    'sys_host': '{hostname}',
                    'sys_name': '{name}',
                    'sys_module': '{module}',
                }),
                '$': (logging.StringTemplateStyle, {
                    'sys_host': '${hostname}',
                    'sys_name': '${name}',
                    'sys_module': '${module}',
                }),
            }[style]
        else:
            self.__style = None
            basic_fmt_dict = {
                'sys_host': '%(hostname)s',
                'sys_name': '%(name)s',
                'sys_module': '%(module)s',
            }

        if not fmt:
            self._fmt_dict = basic_fmt_dict
        else:
            self._fmt_dict = fmt

        self.hostname = socket.gethostname()

        self.fill_missing_fmt_key = fill_missing_fmt_key

    def format(self, record):
        # Only needed for python2.6
        if sys.version_info[0:2] <= (2, 6) and self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)

        # Compute attributes handled by parent class.
        super(FluentRecordFormatter, self).format(record)
        # Add ours
        record.hostname = self.hostname

        # Apply format
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

        self._structuring(data, record)
        return data

    def usesTime(self):
        return any([value.find('%(asctime)') >= 0
                    for value in self._fmt_dict.values()])

    def _structuring(self, data, record):
        """ Melds `msg` into `data`.

        :param data: dictionary to be sent to fluent server
        :param msg: :class:`LogRecord`'s message to add to `data`.
          `msg` can be a simple string for backward compatibility with
          :mod:`logging` framework, a JSON encoded string or a dictionary
          that will be merged into dictionary generated in :meth:`format.
        """
        msg = record.msg

        if isinstance(msg, dict):
            self._add_dic(data, msg)
        elif isinstance(msg, basestring):
            try:
                json_msg = json.loads(str(msg))
                if isinstance(json_msg, dict):
                    self._add_dic(data, json_msg)
                else:
                    self._add_dic(data, {'message': str(json_msg)})
            except ValueError:
                msg = record.getMessage()
                self._add_dic(data, {'message': msg})
        else:
            self._add_dic(data, {'message': msg})

    @staticmethod
    def _add_dic(data, dic):
        for key, value in dic.items():
            if isinstance(key, basestring):
                data[str(key)] = value


class FluentHandler(logging.Handler):
    '''
    Logging Handler for fluent.
    '''
    def __init__(self,
                 tag,
                 host='localhost',
                 port=24224,
                 timeout=3.0,
                 verbose=False,
                 buffer_overflow_handler=None,
                 msgpack_kwargs=None,
                 nanosecond_precision=False):

        self.tag = tag
        self.sender = self.getSenderInstance(tag,
                                             host=host, port=port,
                                             timeout=timeout, verbose=verbose,
                                             buffer_overflow_handler=buffer_overflow_handler,
                                             msgpack_kwargs=msgpack_kwargs,
                                             nanosecond_precision=nanosecond_precision)
        logging.Handler.__init__(self)

    def getSenderClass(self):
        return sender.FluentSender

    def getSenderInstance(self, tag, host, port, timeout, verbose,
                          buffer_overflow_handler, msgpack_kwargs,
                          nanosecond_precision):
        sender_class = self.getSenderClass()
        return sender_class(tag,
                            host=host, port=port,
                            timeout=timeout, verbose=verbose,
                            buffer_overflow_handler=buffer_overflow_handler,
                            msgpack_kwargs=msgpack_kwargs,
                            nanosecond_precision=nanosecond_precision)

    def emit(self, record):
        data = self.format(record)
        return self.sender.emit(None, data)

    def close(self):
        self.acquire()
        try:
            self.sender._close()
            logging.Handler.close(self)
        finally:
            self.release()
