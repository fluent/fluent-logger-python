import logging
import os
import sys
import msgpack
import socket
import threading
import traceback

try:
    import json
except ImportError:
    import simplejson as json

from fluent import sender

class FluentRecordFormatter(object):
    def __init__(self):
        self.hostname = socket.gethostname()

    def format(self, record):
        data = {
          'sys_host' : self.hostname,
          'sys_name' : record.name,
          'sys_module' : record.module,
          # 'sys_lineno' : record.lineno,
          # 'sys_levelno' : record.levelno,
          # 'sys_levelname' : record.levelname,
          # 'sys_filename' : record.filename,
          # 'sys_funcname' : record.funcName,
          # 'sys_exc_info' : record.exc_info,
        }
        # if 'sys_exc_info' in data and data['sys_exc_info']:
        #    data['sys_exc_info'] = self.formatException(data['sys_exc_info'])

        self._structuring(data, record)
        return data

    def _structuring(self, data, record):
        log_data = self._get_log_data(record)

        traceback = self._get_traceback(record)
        if traceback:
            log_data['traceback'] = traceback

        self._add_dic(data, log_data)

    def _get_log_data(self, record):
        if isinstance(record.msg, dict):
            data = record.msg
        else:
            message = record.getMessage()
            try:
                parsed_value = json.loads(message)
            except ValueError, e:
                data = {'message': message}
            else:
                if not isinstance(parsed_value, dict):
                    data = {'message': parsed_value}
        return data

    def _get_traceback(self, record):
        if not record.exc_info:
            return None
        tb = traceback.format_exception(*record.exc_info)
        return "".join(tb)

    def _add_dic(self, data, dic):
        for k, v in dic.items():
            if isinstance(k, str) or isinstance(k, unicode):
                data[str(k)] = v

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
        self.fmt = FluentRecordFormatter()
        logging.Handler.__init__(self)

    def emit(self, record):
        if record.levelno < self.level: return
        data = self.fmt.format(record)
        self.sender.emit(None, data)

    def _close(self):
        self.sender._close()
