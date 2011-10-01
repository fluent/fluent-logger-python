import unittest

import fluent
from fluent import Event

fluent.setup(server='localhost', tag='app')

class TestHandler(unittest.TestCase):
    def testLogging(self):
        # send event with tag app.follow
        Event('follow', {
          'from': 'userA',
          'to':   'userB'
        })
