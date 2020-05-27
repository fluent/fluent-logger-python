# -*- coding: utf-8 -*-

import time

from fluent import sender


class Event(object):
    def __init__(self, label, data, **kwargs):
        assert isinstance(data, dict), 'data must be a dict'
        sender_ = kwargs.get('sender', sender.get_global_sender())
        timestamp = kwargs.get('time', int(time.time()))
        sender_.emit_with_time(label, timestamp, data)
