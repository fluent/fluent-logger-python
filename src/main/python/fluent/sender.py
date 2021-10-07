# -*- coding: utf-8 -*-
import logging
import os
import socket
from base64 import b64encode
from collections import deque, OrderedDict as odict
from errno import EWOULDBLOCK, EINPROGRESS
from hashlib import sha512
from os.path import abspath
from queue import Queue, Full, Empty
from select import select
from subprocess import Popen
from threading import Thread, Condition, RLock, Semaphore, BoundedSemaphore
from time import sleep
from time import time
from urllib.parse import urlparse
from uuid import uuid1

from msgpack import Packer
from msgpack.fallback import Unpacker

DEFAULT_SCHEME = "tcp"

CLOSED = b""
EOF = CLOSED
NEW_OP = b"0"

OP_READ = 1
OP_WRITE = 2
OP_CLOSE = 3

EPC_READY = 0
EPC_HELO = 1
EPC_PONG = 2

TOMBSTONE = object()

_endpoint_registry = {}


def to_bytes(s):
    if isinstance(s, str):
        return s.encode("utf-8")
    return s


def _register_endpoint(schemes, endpoint, force_overwrite=False):
    if isinstance(schemes, str):
        schemes = (schemes,)

    for scheme in schemes:
        if scheme in _endpoint_registry and not force_overwrite:
            raise RuntimeError("endpoint %s is already registered with %r" % (scheme, endpoint))
        _endpoint_registry[scheme] = endpoint


def _find_endpoint(scheme):
    """
    ``scheme`` - ``Endpoint`` only handles ``scheme``
    ``scheme``+``subscheme`` - ``Endpoint`` only handles that specific chain of schemes overwriting the wildcard
    ``scheme``+ - ``Endpoint`` handles all schemes that start with ``scheme``
    :param scheme:
    :return:
    """
    endpoint = _endpoint_registry.get(scheme)
    if not endpoint:
        for r_scheme in _endpoint_registry:
            if r_scheme[-1] == "+" and scheme == r_scheme[:-1] or scheme.startswith(r_scheme):
                endpoint = _endpoint_registry[r_scheme]
                break

    return endpoint


def endpoint(url, **kwargs):
    p_url = urlparse(url, scheme=DEFAULT_SCHEME)
    endpoint = _find_endpoint(p_url.scheme)
    if not endpoint:
        raise ValueError("No endpoint found for %s" % url)

    return endpoint(**kwargs)


class QueueDict(odict):
    def __init__(self, maxsize=0):
        self.maxsize = maxsize
        self._init(maxsize)

        # mutex must be held whenever the queue is mutating.  All methods
        # that acquire mutex must release it before returning.  mutex
        # is shared between the three conditions, so acquiring and
        # releasing the conditions also acquires and releases mutex.
        self.mutex = RLock()

        # Notify not_empty whenever an item is added to the queue; a
        # thread waiting to get is notified then.
        self.not_empty = Condition(self.mutex)

        # Notify not_full whenever an item is removed from the queue;
        # a thread waiting to put is notified then.
        self.not_full = Condition(self.mutex)

        # Notify all_tasks_done whenever the number of unfinished tasks
        # drops to zero; thread waiting to join() is notified to resume
        self.all_tasks_done = Condition(self.mutex)
        self.unfinished_tasks = 0

    def task_done(self):
        '''Indicate that a formerly enqueued task is complete.
        Used by Queue consumer threads.  For each get() used to fetch a task,
        a subsequent call to task_done() tells the queue that the processing
        on the task is complete.
        If a join() is currently blocking, it will resume when all items
        have been processed (meaning that a task_done() call was received
        for every item that had been put() into the queue).
        Raises a ValueError if called more times than there were items
        placed in the queue.
        '''
        with self.all_tasks_done:
            unfinished = self.unfinished_tasks - 1
            if unfinished <= 0:
                if unfinished < 0:
                    raise ValueError('task_done() called too many times')
                self.all_tasks_done.notify_all()
            self.unfinished_tasks = unfinished

    def join(self):
        '''Blocks until all items in the Queue have been gotten and processed.
        The count of unfinished tasks goes up whenever an item is added to the
        queue. The count goes down whenever a consumer thread calls task_done()
        to indicate the item was retrieved and all work on it is complete.
        When the count of unfinished tasks drops to zero, join() unblocks.
        '''
        with self.all_tasks_done:
            while self.unfinished_tasks:
                self.all_tasks_done.wait()

    def qsize(self):
        '''Return the approximate size of the queue (not reliable!).'''
        with self.mutex:
            return self._qsize()

    def empty(self):
        '''Return True if the queue is empty, False otherwise (not reliable!).
        This method is likely to be removed at some point.  Use qsize() == 0
        as a direct substitute, but be aware that either approach risks a race
        condition where a queue can grow before the result of empty() or
        qsize() can be used.
        To create code that needs to wait for all queued tasks to be
        completed, the preferred technique is to use the join() method.
        '''
        with self.mutex:
            return not self._qsize()

    def full(self):
        '''Return True if the queue is full, False otherwise (not reliable!).
        This method is likely to be removed at some point.  Use qsize() >= n
        as a direct substitute, but be aware that either approach risks a race
        condition where a queue can shrink before the result of full() or
        qsize() can be used.
        '''
        with self.mutex:
            return 0 < self.maxsize <= self._qsize()

    def put(self, key, block=True, timeout=None, value=None):
        '''Put an item into the queue.
        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until a free slot is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Full exception if no free slot was available within that time.
        Otherwise ('block' is false), put an item on the queue if a free slot
        is immediately available, else raise the Full exception ('timeout'
        is ignored in that case).
        '''
        with self.not_full:
            if self.maxsize > 0:
                if not block:
                    if self._qsize() >= self.maxsize:
                        raise Full
                elif timeout is None:
                    while self._qsize() >= self.maxsize:
                        self.not_full.wait()
                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                else:
                    endtime = time() + timeout
                    while self._qsize() >= self.maxsize:
                        remaining = endtime - time()
                        if remaining <= 0.0:
                            raise Full
                        self.not_full.wait(remaining)
            self._put(key, value)
            self.unfinished_tasks += 1
            self.not_empty.notify()

    def get(self, block=True, timeout=None):
        '''Remove and return an item from the queue.
        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        '''
        with self.not_empty:
            if not block:
                if not self._qsize():
                    raise Empty
            elif timeout is None:
                while not self._qsize():
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = time() + timeout
                while not self._qsize():
                    remaining = endtime - time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
            item = self._get()
            self.not_full.notify()
            return item

    def pop(self, key, block=True, timeout=None):
        '''Pop and return a specific item from the queue.
        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        '''
        with self.not_empty:
            if not block:
                item = self._pop(key)
            elif timeout is None:
                while not self._qsize():
                    self.not_empty.wait()
                    item = self._pop(key)
                    if item:
                        break
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = time() + timeout
                while not self._qsize():
                    remaining = endtime - time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
                    item = self._pop(key)
                    if item:
                        break

            self.not_full.notify()
            return item

    def put_nowait(self, item):
        '''Put an item into the queue without blocking.
        Only enqueue the item if a free slot is immediately available.
        Otherwise raise the Full exception.
        '''
        return self.put(item, block=False)

    def get_nowait(self):
        '''Remove and return an item from the queue without blocking.
        Only get an item if one is immediately available. Otherwise
        raise the Empty exception.
        '''
        return self.get(block=False)

    # Override these methods to implement other queue organizations
    # (e.g. stack or priority queue).
    # These will only be called with appropriate locks held

    # Initialize the queue representation
    def _init(self, maxsize):
        self.queue = odict()

    def _qsize(self):
        return len(self.queue)

    # Put a new item in the queue
    def _put(self, key, value):
        self.queue[key] = value

    # Get an item from the queue
    def _get(self):
        return self.queue.popitem(False)

    def _pop(self, key):
        return self.queue.pop(key, None)


qdict = QueueDict


class Endpoint:
    """An ``Endpoint`` represents a single FluentD server or a server cluster that operates cohesively as one unit.
    Endpoint may have multiple ``EndpointConnection``s that may come and go as cluster nodes are spun up and die.
    """

    self_fqdn = to_bytes(socket.getfqdn())

    def __init__(self, _url, shared_key=None, username=None, password=None):
        self.url = _url
        self.shared_key = shared_key
        self.username = username
        self.password = password

        self.connections = odict()
        self.sender_c = None

    def attach(self, sender_c):
        self.sender_c = sender_c

    def addrs(self):
        """Returns all socket addresses """
        raise NotImplementedError

    def connection(self):
        raise NotImplementedError

    def refresh_connections(self):
        """Called by SenderConnection when it's time to refresh the connections"""

        s_addrs = set(self.addrs())

        removed_addrs = self.connections.keys() - s_addrs
        new_addrs = s_addrs - self.connections.keys()

        for addr in removed_addrs:
            self.connections.pop(addr).close()

        for new_addr in new_addrs:
            conn = self.connection()(new_addr, self)
            self.connections[new_addr] = conn
            conn.connect()

        return removed_addrs, new_addrs


class InetEndpoint(Endpoint):
    default_port = 24224

    def __init__(self, *args, prefer_ipv6=False, **kwargs):
        super(InetEndpoint, self).__init__(*args, **kwargs)
        netloc_hosts = self.url.netloc.split(",")
        self.netloc_addrs = [addr if len(addr) > 1 else (addr[0], self.default_port) for addr in
                             (hp.split(":") for hp in netloc_hosts)]
        self.prefer_ipv6 = prefer_ipv6
        self.addr_family_kind_proto = {}

    def addrs(self):
        results = []
        for addr in self.netloc_addrs:
            host_addrs = socket.getaddrinfo(host=addr[0], port=addr[1], **self.addr_family_kind_proto)
            if self.prefer_ipv6:
                ipv6_host_addrs = [host_addr for host_addr in host_addrs if host_addr[0] == socket.AF_INET6]
                if ipv6_host_addrs:
                    host_addrs = ipv6_host_addrs
            else:
                ipv4_host_addrs = [host_addr for host_addr in host_addrs if host_addr[0] == socket.AF_INET]
                if ipv4_host_addrs:
                    host_addrs = ipv4_host_addrs

            results.extend(host_addrs)

        return results


class TcpEndpoint(InetEndpoint):
    def __init__(self, *args, **kwargs):
        super(TcpEndpoint, self).__init__(*args, **kwargs)
        self.addr_family_kind_proto = {"type": socket.SOCK_STREAM,
                                       "proto": socket.IPPROTO_TCP}

    def connection(self):
        return TcpConnection


_register_endpoint("tcp+", TcpEndpoint)


class EndpointConnection(Thread):
    """One of the connections established for a specific ``Endpoint``. """

    def __init__(self, addr, endpoint):
        super(EndpointConnection, self, ).__init__(name="EPC %r" % (addr,), daemon=True)

        self.addr = addr
        self.endpoint = endpoint
        self.logger = endpoint.sender_c.logger
        self.sender_c = endpoint.sender_c
        self.sock = self._socket(addr)  # type: socket.socket
        self._fileno = self.sock.fileno()
        self._unpacker = None
        self._eventq = Queue()  # queue of messages to be processed by the connection, in order
        self._writeq = deque()  # data to be written into a socket, in order

        self._shared_key_salt = None
        self._nonce = None
        self._keep_alive = False

        if endpoint.shared_key or endpoint.username:
            self.state = EPC_HELO
        else:
            self.state = EPC_READY

    def connect(self):
        self.sock.setblocking(False)
        addr = self._connect_addr()
        self.logger.debug("Establishing connection to %s", addr)
        self._connect(addr)
        self._fileno = self.sock.fileno()
        self.start()

    def fileno(self):
        return self._fileno

    def on_read(self):
        try:
            data = self._recv()
        except socket.error as e:
            if e.errno == EWOULDBLOCK or e.errno == EINPROGRESS:
                return True
            raise

        if data == b"\x00":
            # This is just HEARTBEAT, skip
            logger.debug("Received HEARTBEAT from %s", self._connect_addr)
            return True

        unpacker = self._unpacker
        if not unpacker:
            unpacker = self._unpacker = Unpacker(encoding='utf-8')

        unpacker.feed(data)
        obj = None
        for obj in unpacker:
            self.logger.debug("On %s received: %s", self, obj)
            self._eventq.put(obj)

        self._unpacker = None

        if obj is None and data == EOF:
            if self._keep_alive:
                log = self.logger.warning
            else:
                log = self.logger.debug
            log("Connection %s remote closed while reading", self)

            self.schedule_close()
            return False

        return True

    def on_write(self):
        try:
            data = self._writeq.popleft()
        except IndexError:
            return False

        bytes_left = len(data)
        bytes_sent = -1
        while bytes_left and bytes_sent:
            try:
                bytes_sent = self._send(data)
                if not bytes_sent:
                    self.logger.warning("Connection %s remote closed unexpectedly while writing", self)
                    self.schedule_close()
                    return False
                bytes_left -= bytes_sent
                if bytes_left:
                    data = data[bytes_sent:]
            except socket.error as e:
                if e.errno == EWOULDBLOCK:
                    break
                raise

        if bytes_left:  # We tried to write everything but couldn't and received a 0-byte send
            self._writeq.appendleft(data)

        return True

    def schedule_close(self):
        self.logger.debug("Scheduling close on %s", self)
        self.sender_c.schedule_op(OP_CLOSE, self)

    def close(self):
        if self.sock.fileno() < 0:
            return

        self.logger.debug("Closing %s", self)
        self.sender_c.schedule_op(OP_READ, self, False)
        self.sender_c.schedule_op(OP_WRITE, self, False)
        try:
            try:
                try:
                    self.sock.shutdown(socket.SHUT_RDWR)
                except socket.error:  # pragma: no cover
                    pass
            finally:
                try:
                    self.sock.close()
                except socket.error:  # pragma: no cover
                    pass
        finally:
            self._eventq.put(TOMBSTONE)

    def send(self, data):
        self._writeq.append(data)
        self.sender_c.schedule_op(OP_WRITE, self)

    def ping_from_helo(self, obj):
        shared_key_salt = None
        shared_key_hexdigest = None
        password_digest = ""

        self._keep_alive = obj[1].get("keepalive", False)

        if self.endpoint.shared_key:
            self._shared_key_salt = shared_key_salt = os.urandom(16)
            self._nonce = nonce = obj[1]["nonce"]
            digest = sha512()
            digest.update(shared_key_salt)
            digest.update(self.endpoint.self_fqdn)
            digest.update(nonce)
            digest.update(to_bytes(self.endpoint.shared_key))
            shared_key_hexdigest = digest.hexdigest()

        if self.endpoint.username:
            digest = sha512()
            digest.update(obj[1]["auth"])
            digest.update(to_bytes(self.endpoint.username))
            digest.update(to_bytes(self.endpoint.password))
            password_digest = digest.hexdigest()

        data = ["PING", self.endpoint.self_fqdn, shared_key_salt, shared_key_hexdigest,
                self.endpoint.username or "", password_digest]
        msg = Packer(use_bin_type=True).pack(data)
        return msg

    def verify_pong(self, obj):
        try:
            if not obj[1]:
                self.logger.warning("Authentication failed for %s: %s", self, obj[2])
                return False
            else:
                # Authenticate server
                digest = sha512()
                digest.update(self._shared_key_salt)
                digest.update(to_bytes(obj[3]))
                digest.update(self._nonce)
                digest.update(to_bytes(self.endpoint.shared_key))
                my_shared_key_hexdigest = digest.hexdigest()
                if my_shared_key_hexdigest != obj[4]:
                    self.logger.warning("Server hash didn't match: %r vs %r", my_shared_key_hexdigest, obj[4])
                    return False
                return True
        except Exception as e:
            self.logger.error("Unknown error while validating PONG", exc_info=e)
            return False

    def send_msg(self, tag, time, record, ack=False):
        options = {"size": 1}
        if ack:
            options["chunk"] = b64encode(uuid1().bytes)
        data = [tag, int(time), record, options]
        self.logger.debug("Sending %r", data)
        msg = Packer(use_bin_type=True).pack(data)
        self.send(msg)

    def send_msgs(self, tag, entries, ack=False):
        options = {"size": len(entries)}
        if ack:
            options["chunk"] = b64encode(uuid1().bytes)
        data = [tag, entries, options]
        self.logger.debug("Sending %r", data)
        msg = Packer(use_bin_type=True).pack(data)
        self.send(msg)

    def run(self):
        eventq = self._eventq
        while True:
            obj = eventq.get(block=True)
            if obj is TOMBSTONE:
                return
            if not obj:
                logger.warning("Unexpected empty packet received from %s: %s", self.sock.getpeername(), obj)
                self.close()
                return
            if isinstance(obj, (list, tuple)):  # Array
                msg_type = obj[0]
                if msg_type == "HELO":
                    if self.state != EPC_HELO:
                        logger.warning("Unexpected HELO received from %s: %s", self.sock.getpeername(), obj)
                        self.close()
                        return
                    self.send(self.ping_from_helo(obj))
                    self.state = EPC_PONG
                elif msg_type == "PONG":
                    if self.state != EPC_PONG:
                        logger.warning("Unexpected PONG received from %s: %s", self.sock.getpeername(), obj)
                        self.close()
                        return
                    if not self.verify_pong(obj):
                        self.close()
                        return
                    self.state = EPC_READY
                    self.logger.info("Ready!")
            else:  # Dict
                chunk_id = obj.get("ack", None)
                if not chunk_id:
                    logger.warning("Unexpected response received from %s: %s", self.sock.getpeername(), obj)
                    self.close()
                    return
                self.sender_c.ack_chunk(chunk_id)

    def _socket(self, addr):
        raise NotImplementedError

    def _connect_addr(self):
        raise NotImplementedError

    def _connect(self, addr):
        raise NotImplementedError

    def _recv(self):
        raise NotImplementedError

    def _send(self, data):
        raise NotImplementedError


class StreamConnection(EndpointConnection):
    def __init__(self, addr, endpoint, bufsize):
        if addr[1] != socket.SOCK_STREAM:
            raise ValueError("Socket type %s cannot be used with %s" % (addr[1], self.__class__.name))
        super(StreamConnection, self).__init__(addr, endpoint)
        self.bufsize = bufsize

    def _socket(self, addr):
        return socket.socket(addr[0], addr[1], addr[2])

    def _connect_addr(self):
        return self.addr[4]

    def _connect(self, addr):
        try:
            self.sock.connect(addr)
        except socket.error as e:
            if not (e.errno == EWOULDBLOCK or e.errno == EINPROGRESS):
                raise

    def _recv(self):
        return self.sock.recv(self.bufsize)

    def _send(self, data):
        return self.sock.send(data)


class TcpConnection(StreamConnection):
    def __init__(self, addr, endpoint, bufsize):
        if addr[0] not in (socket.AF_INET, socket.AF_INET6):
            raise ValueError("Address family %s cannot be used with %s" % (addr[0], self.__class__.name))
        super(TcpConnection, self).__init__(addr, endpoint, bufsize)


if False:  # pragma: no branch
    class UdpConnection(EndpointConnection):
        def __init__(self, addr, endpoint, maxsize, bind_to):
            if addr[1] != socket.SOCK_DGRAM:
                raise ValueError("Socket type %s cannot be used with %s" % (addr[1], self.__class__.name))
            super(UdpConnection, self).__init__(addr, endpoint)
            self.maxsize = maxsize
            self.bind_to = bind_to
            self.remote_addr = self.addr[4]

        def _socket(self, addr):
            return socket.socket(addr[0], addr[1], addr[2])

        def _connect_addr(self):
            return self.remote_addr

        def _connect(self, addr):
            self.sock.bind((self.bind_to, 0))
            self.sock.connect(self.remote_addr)

        def _recv(self):
            data, _ = self.sock.recvfrom(self.maxsize)
            return data

        def _send(self, data):
            return self.sock.sendto(data, self.remote_addr)


class UnixConnection(StreamConnection):
    def __init__(self, addr, endpoint, bufsize):
        if addr[0] != socket.AF_UNIX:
            raise ValueError("Address family %s cannot be used with %s" % (addr[0], self.__class__.name))
        super(UnixConnection, self).__init__(addr, endpoint, bufsize)

    def _socket(self, addr):
        return socket.socket(addr[0], addr[1])


class MsgMeta:
    __slots__ = ["deadline", "retries"]

    def __init__(self, deadline):
        self.deadline = deadline
        self.retries = 0


class AsyncLogStore:
    def __init__(self, queue_maxsize, queue_circular, send_timeout):
        self.send_timeout = send_timeout
        self._queue_circular = queue_circular
        self._queue = qdict(maxsize=queue_maxsize)
        self._acks = set()
        self.mutex = self._queue.mutex
        self.ack_received = Condition(self.mutex)

    def post(self, msg):
        remaining = self.send_timeout
        t1 = time()
        msg_meta = MsgMeta(t1 + remaining)

        if self.mutex.acquire(timeout=remaining):
            try:
                if self._queue_circular and self._queue.full():
                    # discard oldest
                    try:
                        self._queue.get(block=False)
                    except Empty:  # pragma: no cover
                        pass
                t2 = time()
                remaining -= t2 - t1
                t1 = t2
                try:
                    self._queue.put(msg,
                                    block=not self._queue_circular,
                                    timeout=remaining,
                                    value=msg_meta)
                except Full:
                    return False
                t2 = time()
                remaining -= t2 - t1
                t1 = t2


            finally:
                self.mutex.release()
        else:
            return False

    def ack(self, msg):
        with self.mutex:
            self._acks[msg] = 1
            self.ack_received.notify_all()


class SyncLogStore:
    def __init__(self, send_timeout, at_least_once):
        self.send_timeout = send_timeout
        self.at_least_once = at_least_once
        self.send_sem = BoundedSemaphore()
        self.pending_sem = Semaphore(0)
        self.mutex = RLock()
        self.delivered = Condition(lock=self.mutex)
        self.available = Condition(lock=self.mutex)
        self.msg = None

        self.logger = None

    def post(self, tag, ts, payload):
        msg = (tag, ts, payload)

        remaining = self.send_timeout
        t1 = time()
        deadline = t1 + remaining
        if self.send_sem.acquire(timeout=remaining):
            try:
                with self.mutex:
                    remaining = deadline - time()
                    self.msg = msg
                    self.pending_sem.release()
                    if self.at_least_once:
                        return self.delivered.wait(remaining)
            finally:
                self.send_sem.release()

    def ack(self, msg):
        with self.mutex:
            if msg is self.msg:
                self.delivered.notify()
            else:
                self.logger.warning("Message ACK delivered after timeout", extra={"message", msg})

    def next(self, timeout=None):
        remaining = timeout

        with self.mutex:
            while True:
                if self.msg is not None:
                    return self.msg
                if timeout and remaining > 0:
                    self.available.wait(timeout=remaining)
                else:
                    self.available.wait()


class SenderConnection(Thread):
    def __init__(self,
                 endpoints,
                 ha_strategy,
                 log_store,
                 refresh_period,
                 logger):
        """
        Internal Sender connection that maintains an aggregate connection for the Sender.
        The Sender may be connected to various servers and clusters via different protocols over multiple endpoints.
        How endpoints are treated depends on a strategy specified, which is transparent to the Sender.
        :param endpoints: iterable of Endpoint
        :param ha_strategy: logic underlying selection of the endpoint for sending
        :param log_store: a store for log messages allowing to customize store behavior
        :param refresh_period: how often to refresh the endpoints
        :param logger: internal Fluent logger
        """
        super(SenderConnection, self).__init__(name=self.__class__.name, daemon=True)

        self.endpoints = endpoints
        self.ha_strategy = ha_strategy
        self.log_store = log_store
        self.refresh_period = refresh_period
        self.logger = logger

        self.endpoint_connections = {}

        self._close_pending = deque()
        self._open_pending = deque()

        self.mutex = RLock()

        self.wakeup_sock_r, self.wakeup_sock_w = socket.socketpair()
        self.wakeup_sock_r.setblocking(False)

        self.op_queue = deque()

    def refresh_endpoints(self):
        for endpoint in self.endpoints:
            endpoint.refresh_connections()

    def schedule_op(self, op, conn, enable=True):
        self.op_queue.append((enable, op, conn))
        self.wakeup_sock_w.send(NEW_OP)

    def close(self, timeout=None):
        self.wakeup_sock_w.close()
        self.join(timeout)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def ack_chunk(self, chunk_id):
        logger.debug("Acknowledging chunk %r", chunk_id)

    def send(self, tag, ts, payload):
        return self.log_store.post(tag, ts, payload)

    def run(self):
        logger = self.logger
        r_int = set()
        w_int = set()

        refresh_period = self.refresh_period

        wakeup_sock_r = self.wakeup_sock_r
        r_int.add(wakeup_sock_r)

        op_queue = self.op_queue
        # last_ts = time.time()

        with wakeup_sock_r, self.wakeup_sock_w:
            while r_int or w_int:
                r_ready, w_ready, _ = select(r_int, w_int, (), timeout=refresh_period)
                for r in r_ready:
                    if r is wakeup_sock_r:
                        while True:
                            try:
                                cmds = r.recv(2048)
                            except socket.error as e:
                                if e.errno == EWOULDBLOCK:
                                    break
                            if cmds == b"":
                                r_int.remove(r)
                                break
                            # Handle exception here
                            for cmd in cmds:
                                cmd = bytes((cmd,))
                                if cmd == CLOSED:
                                    r_int.remove(r)
                                    break
                                elif cmd == NEW_OP:
                                    enable, op, conn = op_queue.pop()
                                    if op == OP_READ:
                                        if enable:
                                            r_int.add(conn)
                                        else:
                                            r_int.discard(conn)
                                    elif op == OP_WRITE:
                                        if enable:
                                            w_int.add(conn)
                                        else:
                                            w_int.discard(conn)
                                    elif op == OP_CLOSE:
                                        conn.close()
                    else:
                        keep = False
                        try:
                            keep = r.on_read()
                        except Exception as e:
                            with r:
                                logger.warning("Read error on %s", r, exc_info=e)

                        if not keep:
                            r_int.remove(r)

                for w in w_ready:
                    keep = False
                    try:
                        keep = w.on_write()
                    except Exception as e:
                        with w:
                            logger.warning("Write error on %s", w, exc_info=e)

                    if not keep:
                        w_int.remove(w)

                r_ready.clear()
                w_ready.clear()


if __name__ == '__main__':
    logger = logging.getLogger("fluent")
    logger.propagate = False
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(1)

    with Popen(["docker", "run", "-i",
                "-p", "24224:24224", "-p", "24224:24224/udp",
                "-p", "24225:24225", "-p", "24225:24225/udp",
                "-p", "24226:24226", "-p", "24226:24226/udp",
                "-v", "%s:/fluentd/log" % abspath("../tests"),
                "-v", "%s:/fluentd/etc/fluent.conf" % abspath("../tests/fluent.conf"),
                "-v", "%s:/var/run/fluent" % abspath("../tests/fluent_sock"),
                "fluent/fluentd:v1.1.0"]) as docker:
        sleep(5)
        log_store = SyncLogStore(send_timeout=3.0, at_least_once=True)
        with SenderConnection([endpoint("tcp://localhost:24224")], None, log_store, 5.0, logger) as conn:
            conn.send("tag-name", time(), {"value-x": "a", "value-y": 1})
            conn.send_msgs("tag-name", ((int(time()), {"value-x": "a", "value-y": 1}),
                                        (int(time()), {"value-x": "m", "value-b": 200})))
            sleep(3)

        docker.terminate()
