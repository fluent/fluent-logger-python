from fluent import logger

class Event:
    def __init__(self, label, data, **kwargs):
        if not isinstance(data, dict) :
            raise Exception("data must be dict")
        l = kwargs.has_key("logger") and kwargs['logger'] or logger.get_global_logger()
        l.emit(label, data)
