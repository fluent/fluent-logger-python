from fluent import sender
import time

class Event(object):
    def __init__(self, label, data, **kwargs):
        assert isinstance(data, dict), 'data must be a dict'
        s = kwargs.get('sender', sender.get_global_sender())
        timestamp = kwargs.get('time', int(time.time()))
        s.emit_with_time(label, timestamp, data)
