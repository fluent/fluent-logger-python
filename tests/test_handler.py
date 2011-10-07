import unittest
import mockserver
import logging
import fluent.handler
import msgpack

class TestLogger(unittest.TestCase):
    def setUp(self):
        super(TestLogger, self).setUp()
        for port in xrange(10000, 20000):
            try:
                self._server = mockserver.MockRecvServer(port)
                self._port = port
                break
            except IOError, e:
                pass

    def get_data(self):
        return self._server.get_recieved()

    def test_simple(self):
        h = fluent.handler.FluentHandler('app.follow', port=self._port)

        logging.basicConfig(level=logging.INFO)
        l = logging.getLogger('fluent.test')
        l.addHandler(h)
        l.info({
            'from': 'userA',
            'to': 'userB'
        })
        h._close()

        data = self.get_data()
        #print
        #eq = self.assertEqual
        #eq(1, len(data))
        #eq(3, len(data[0]))
        #eq('test.foo', data[0][0])
        #eq({'bar':'baz'}, data[0][2])
        #self.assert_(data[0][1])
        #self.assert_(isinstance(data[0][1], int))
