from fluent import sender

class Event(object):
    def __init__(self, label, data, **kwargs):
        if not isinstance(data, dict) :
            raise Exception("data must be dict")
        l = ('sender' in kwargs) and kwargs['sender'] or sender.get_global_sender()
        l.emit(label, data)
