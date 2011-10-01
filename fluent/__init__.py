import json
import urllib2

from fluent import logger

global_logger = None

def setup(tag, **kwargs):
    host = kwargs.has_key("host") and kwargs['host'] or 'localhost'
    port = kwargs.has_key("port") and kwargs['port'] or 24224

    global global_logger
    global_logger = logger.FluentLogger(tag, host=host, port=port)

class Event:
    def __init__(self, label, data, **kwargs):
        global global_logger
        self.logger = kwargs.has_key("logger") and kwargs['logger'] or global_logger

        if not isinstance(data, dict) :
            raise Exception("data must be dict")

        self.logger.emit(label, data)
