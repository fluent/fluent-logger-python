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
        self.hostname = socket.gethostname()
        self.datefmt = datefmt
        if isinstance(fmt, dict):
            self._fmt = fmt
        elif isinstance(fmt, str):
            try:
                self._fmt = json.loads(str(fmt))
            except ValueError:
                self._fmt = self.default_format()
        else:
            self._fmt = self.default_format()

    def format(self, record):
        data = self.format_data(record)
        self._structuring(data, record.msg)
        return data

    def _structuring(self, data, msg):
        if isinstance(msg, dict):
            self._add_dic(data, msg)
        elif isinstance(msg, str):
            try:
                self._add_dic(data, json.loads(str(msg)))
            except ValueError:
                pass

    @staticmethod
    def _add_dic(data, dic):
        for key, value in dic.items():
            if isinstance(key, basestring):
                data[str(key)] = value

    def format_data(self, record):
        data = {}
        for k, i in self._fmt.iteritems():
            if i in record.__dict__.keys():
                if i == 'exc_info' and record.exc_info:
                    data[k] = self.formatException(record.exc_info)
                else:
                    if i == 'msg' and isinstance(record.msg, dict):
                        pass
                    elif i == 'msg' and isinstance(record.msg, str):
                        try:
                            json.loads(str(record.msg))
                        except ValueError:
                            data[k] = record.__dict__[i]
                    else:
                        data[k] = record.__dict__[i]
            else:
                data[k] = i
        return data

    def usesTime(self):
        usesTime = False
        if isinstance(self._fmt, dict):
            usesTime = "asctime" in self._fmt
        elif isinstance(self._fmt, str):
            usesTime = self._fmt.find("asctime") >= 0
        return usesTime

    def default_format(self):
        return {
            'sys_host': self.hostname,
            'sys_name': 'name',
            'sys_module': 'module',
            'message': 'msg'
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
