import logging
import os
import sys, urllib
import msgpack
import socket
import threading
import json

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

        self._structuring(data, record.msg)
        return data

    def _structuring(self, data, msg):
        if isinstance(msg, dict):
            self._add_dic(data, msg)
        elif isinstance(msg, str):
            try:
                self.add_dic(data, json.loads(str(msg)))
            except:
                pass

    def _add_dic(self, data, dic):
        for k, v in dic.items():
            if isinstance(k, str):
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
