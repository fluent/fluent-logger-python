from fluent import sender
import time

class Event(object):
    def __init__(self, label, data, **kwargs):
        if not isinstance(data, dict) :
            raise Exception("data must be dict")
        s = kwargs.get('sender', sender.get_global_sender())
        timestamp = kwargs.get('time', int(time.time()))
        s.emit_with_time(label, timestamp, data)
