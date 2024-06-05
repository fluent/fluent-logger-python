from fluent import asyncsender, handler, sender


class FluentHandler(handler.FluentHandler):
    """
    Asynchronous Logging Handler for fluent.
    """

    def getSenderInstance(self, **kwargs):
        try:
            return super().getSenderInstance(**kwargs)
        except RuntimeError:
            return sender.FluentSender(**kwargs)

    def getSenderClass(self):
        return asyncsender.FluentSender

    def close(self):
        self.acquire()
        try:
            try:
                self.sender.close()
            finally:
                super().close()
        finally:
            self.release()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
