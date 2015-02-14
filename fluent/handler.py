# -*- coding: utf-8 -*-

import logging
import socket

try:
    import simplejson as json
except ImportError:  # pragma: no cover
    import json

try:
    basestring
except NameError:  # pragma: no cover
    basestring = (str, bytes)

from fluent import sender


class FluentRecordFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None):
        super(FluentRecordFormatter, self).__init__(fmt, datefmt)
        if isinstance(fmt, dict):
            pass
        elif isinstance(fmt, str):
            try:
                self._fmt = json.loads(str(fmt))
            except ValueError:
                self._fmt = self.default_format()
        else:
            self._fmt = self.default_format()

    def format(self, record):
        self.format_data(record)
        data = dict([(key, value % record.__dict__) for key, value in self._fmt.items()])
        self._structuring(data, record.msg)
        return data

    def _structuring(self, data, msg):
        if isinstance(msg, dict):
            self._add_dic(data, msg)
        elif isinstance(msg, basestring):
            try:
                self._add_dic(data, json.loads(str(msg)))
            except ValueError:
                self._add_dic(data, {'message': msg})

    @staticmethod
    def _add_dic(data, dic):
        for key, value in dic.items():
            if isinstance(key, basestring):
                data[str(key)] = value

    def format_data(self, record):
        record.message = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)

    def usesTime(self):
        for _, value in self._fmt.items():
            if value.find('%(asctime)') >= 0:
                return True
        return False

    def default_format(self):
        return {
            'sys_host': socket.gethostname(),
            'sys_name': '%(name)s',
            'sys_module': '%(module)s'
        }

class FluentHandler(logging.Handler):
    '''
    Logging Handler for fluent.
    '''
    def __init__(self,
                 tag,
                 host='localhost',
                 port=24224,
                 timeout=3.0,
                 verbose=False):

        self.tag = tag
        self.sender = sender.FluentSender(tag,
                                          host=host, port=port,
                                          timeout=timeout, verbose=verbose)
        logging.Handler.__init__(self)

    def emit(self, record):
        data = self.format(record)
        self.sender.emit(None, data)

    def close(self):
        self.acquire()
        try:
            self.sender._close()
            logging.Handler.close(self)
        finally:
            self.release()
