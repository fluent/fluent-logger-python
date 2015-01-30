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


class FluentRecordFormatter(object):
    def __init__(self):
        self.hostname = socket.gethostname()

    def format(self, record):
        data = {'sys_host': self.hostname,
                'sys_name': record.name,
                'sys_module': record.module,
                # 'sys_lineno': record.lineno,
                # 'sys_levelno': record.levelno,
                # 'sys_levelname': record.levelname,
                # 'sys_filename': record.filename,
                # 'sys_funcname': record.funcName,
                # 'sys_exc_info': record.exc_info,
                }
        # if 'sys_exc_info' in data and data['sys_exc_info']:
        #    data['sys_exc_info'] = self.formatException(data['sys_exc_info'])

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
