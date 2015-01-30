# -*- coding: utf-8 -*-

import unittest

from fluent import event, sender


sender.setup(server='localhost', tag='app')


class TestEvent(unittest.TestCase):
    def test_logging(self):
        # send event with tag app.follow
        event.Event('follow', {
            'from': 'userA',
            'to':   'userB'
        })

        # send event with tag app.follow, with timestamp
        event.Event('follow', {
            'from': 'userA',
            'to':   'userB'
        }, time=int(0))
