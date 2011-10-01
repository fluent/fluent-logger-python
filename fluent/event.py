from fluent import logger

class Event(object):
    def __init__(self, label, data, **kwargs):
        if not isinstance(data, dict) :
            raise Exception("data must be dict")
        l = ('logger' in kwargs) and kwargs['logger'] or logger.get_global_logger()
        l.emit(label, data)
