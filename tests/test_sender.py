from __future__ import print_function
import unittest
from tests import mockserver
import fluent.sender
import msgpack

class TestSender(unittest.TestCase):
    def setUp(self):
        super(TestSender, self).setUp()
        for port in range(10000, 20000):
            try:
                self._server = mockserver.MockRecvServer(port)
                break
            except IOError as e:
                print(e)
                pass
        self._sender = fluent.sender.FluentSender(
                tag='test',
                port=port,
                )

    def get_data(self):
        return self._server.get_recieved()

    def test_simple(self):
        sender = self._sender
        sender.emit('foo', {'bar': 'baz'})
        sender._close()
        data = self.get_data()
        eq = self.assertEqual
        eq(1, len(data))
        eq(3, len(data[0]))
        eq('test.foo', data[0][0])
        eq({'bar':'baz'}, data[0][2])
        self.assert_(data[0][1])
        self.assert_(isinstance(data[0][1], int))
