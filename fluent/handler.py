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
    """
    def __init__(self, fmt=None, datefmt=None):
        super(FluentRecordFormatter, self).__init__(None, datefmt)

        if not fmt:
            self._fmt_dict = {
                'sys_host': '%(hostname)s',
                'sys_name': '%(name)s',
                'sys_module': '%(module)s',
            }
        else:
            self._fmt_dict = fmt

        self.hostname = socket.gethostname()

    def format(self, record):
        # Only needed for python2.6
        if sys.version_info[0:2] <= (2, 6) and self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)

        # Compute attributes handled by parent class.
        super(FluentRecordFormatter, self).format(record)
        # Add ours
        record.hostname = self.hostname
        # Apply format
        data = dict([(key, value % record.__dict__)
                     for key, value in self._fmt_dict.items()])

        self._structuring(data, record.msg)
        return data

    def usesTime(self):
        return any([value.find('%(asctime)') >= 0
                    for value in self._fmt_dict.values()])

    def _structuring(self, data, msg):
        """ Melds `msg` into `data`.

        :param data: dictionary to be sent to fluent server
        :param msg: :class:`LogRecord`'s message to add to `data`.
          `msg` can be a simple string for backward compatibility with
          :mod:`logging` framework, a JSON encoded string or a dictionary
          that will be merged into dictionary generated in :meth:`format.
        """
        if isinstance(msg, dict):
            self._add_dic(data, msg)
        elif isinstance(msg, basestring):
            try:
                self._add_dic(data, json.loads(str(msg)))
            except ValueError:
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
