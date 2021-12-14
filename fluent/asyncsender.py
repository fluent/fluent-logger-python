# -*- coding: utf-8 -*-

import asyncio
import threading
from queue import Queue, Full, Empty
import time
import traceback

from fluent import sender
from fluent.sender import EventTime

__all__ = ["EventTime", "FluentSender"]

DEFAULT_QUEUE_MAXSIZE = 100
DEFAULT_QUEUE_CIRCULAR = False

_TOMBSTONE = object()

_global_sender = None


def _set_global_sender(sender):  # pragma: no cover
    """ [For testing] Function to set global sender directly
    """
    global _global_sender
    _global_sender = sender


def setup(tag, **kwargs):  # pragma: no cover
    global _global_sender
    _global_sender = FluentSender(tag, **kwargs)


def get_global_sender():  # pragma: no cover
    return _global_sender


def close():  # pragma: no cover
    get_global_sender().close()


class FluentSender(sender.FluentSender):
    def __init__(self,
                 tag,
                 host='localhost',
                 port=24224,
                 bufmax=1 * 1024 * 1024,
                 timeout=3.0,
                 verbose=False,
                 buffer_overflow_handler=None,
                 nanosecond_precision=False,
                 msgpack_kwargs=None,
                 queue_maxsize=DEFAULT_QUEUE_MAXSIZE,
                 queue_circular=DEFAULT_QUEUE_CIRCULAR,
                 queue_overflow_handler=None,
                 **kwargs):
        """
        :param kwargs: This kwargs argument is not used in __init__. This will be removed in the next major version.
        """
        super(FluentSender, self).__init__(tag=tag, host=host, port=port, bufmax=bufmax, timeout=timeout,
                                           verbose=verbose, buffer_overflow_handler=buffer_overflow_handler,
                                           nanosecond_precision=nanosecond_precision,
                                           msgpack_kwargs=msgpack_kwargs,
                                           **kwargs)
        self._queue_maxsize = queue_maxsize
        self._queue_circular = queue_circular
        if queue_circular and queue_overflow_handler:
            self._queue_overflow_handler = queue_overflow_handler
        else:
            self._queue_overflow_handler = self._queue_overflow_handler_default

        self._thread_guard = threading.Event()  # This ensures visibility across all variables
        self._closed = False

        self._queue = Queue(maxsize=queue_maxsize)
        self._send_thread = threading.Thread(target=self._send_loop,
                                             name="AsyncFluentSender %d" % id(self))
        self._send_thread.daemon = True
        self._send_thread.start()

    async def __aenter__(self):
        return self

    async def __aexit__(self, typ, value, traceback):
        try:
            await self.close()
        except Exception as e:  # pragma: no cover
            self.last_error = e

    async def _reconnect(self):
        if not self.socket:
            try:
                if self.host.startswith('unix://'):
                    reader, writer = await asyncio.open_connection(host=self.host[len('unix://'):], port=self.port)
                else:
                    reader, writer = await asyncio.open_connection(host=self.host, port=self.port)
            except Exception as e:
                try:
                    writer.close()
                except Exception:  # pragma: no cover
                    pass
                raise e
            else:
                self.socket = writer

    async def emit(self, label, data):
        if self.nanosecond_precision:
            cur_time = EventTime(time.time())
        else:
            cur_time = int(time.time())
        return await self.emit_with_time(label, cur_time, data)

    async def emit_with_time(self, label, timestamp, data):
        if self.nanosecond_precision and isinstance(timestamp, float):
            timestamp = EventTime(timestamp)
        try:
            bytes_ = self._make_packet(label, timestamp, data)
        except Exception as e:
            self.last_error = e
            bytes_ = self._make_packet(label, timestamp,
                                       {"level": "CRITICAL",
                                        "message": "Can't output to log",
                                        "traceback": traceback.format_exc()})
        return await self._send(bytes_)

    async def close(self, flush=True):
        if self.socket:
            self.socket.close()

    @property
    def queue_maxsize(self):
        return self._queue_maxsize

    @property
    def queue_blocking(self):
        return not self._queue_circular

    @property
    def queue_circular(self):
        return self._queue_circular

    async def _send(self, bytes_):
        await self._reconnect()
        self.socket.write(bytes_)
        await self.socket.drain()
        return True

    def _send_loop(self):
        send_internal = super(FluentSender, self)._send_internal

        try:
            while True:
                bytes_ = self._queue.get(block=True)
                if bytes_ is _TOMBSTONE:
                    break

                send_internal(bytes_)
        finally:
            self._close()

    def _queue_overflow_handler_default(self, discarded_bytes):
        pass
