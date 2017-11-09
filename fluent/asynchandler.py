# -*- coding: utf-8 -*-

from fluent import asyncsender
from fluent import handler
from fluent.handler import FluentRecordFormatter


class FluentHandler(handler.FluentHandler):
    '''
    Asynchronous Logging Handler for fluent.
    '''

    def getSenderClass(self):
        return asyncsender.FluentSender

    def close(self):
        self.sender.close()
        super(FluentHandler, self).close()
