from fluent import sender


class Event:
    def __init__(self, label, data, **kwargs):
        assert isinstance(data, dict), "data must be a dict"
        sender_ = kwargs.get("sender", sender.get_global_sender())
        timestamp = kwargs.get("time", None)
        if timestamp is not None:
            sender_.emit_with_time(label, timestamp, data)
        else:
            sender_.emit(label, data)
