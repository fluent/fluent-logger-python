from fluent import asyncsender, handler


class FluentHandler(handler.FluentHandler):
    """
    Asynchronous Logging Handler for fluent.
    """

    def getSenderClass(self):
        return asyncsender.FluentSender
