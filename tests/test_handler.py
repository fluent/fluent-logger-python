import unittest

import fluent
from fluent import event, logger

logger.setup(server='localhost', tag='app')

class TestHandler(unittest.TestCase):
    def testLogging(self):
        # send event with tag app.follow
        event.Event('follow', {
          'from': 'userA',
          'to':   'userB'
        })
