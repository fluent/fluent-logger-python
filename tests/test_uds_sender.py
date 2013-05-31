from __future__ import print_function
import unittest
from tests import mockserver
import fluent.sender
import os


class TestSender(unittest.TestCase):
    def setUp(self):
        super(TestSender, self).setUp()
        socket_path = os.path.abspath('uds_socket')
        server_address = 'unix://' + socket_path

        # Make sure the socket does not already exist
        try:
            os.unlink(socket_path)
        except OSError:
            if os.path.exists(socket_path):
                raise

        self._server = mockserver.MockRecvServer(host=server_address)
        self._sender = fluent.sender.FluentSender(tag='test', host=server_address)

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
        eq({'bar': 'baz'}, data[0][2])
        self.assert_(data[0][1])
        self.assert_(isinstance(data[0][1], int))
