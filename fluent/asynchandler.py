# -*- coding: utf-8 -*-

from fluent import asyncsender
from fluent import handler


class FluentHandler(handler.FluentHandler):
    '''
    Asynchronous Logging Handler for fluent.
    '''

    def getSenderClass(self):
        return asyncsender.FluentSender

    def close(self):
        try:
            self.sender.close()
        finally:
            super(FluentHandler, self).close()
