A Python structured logger for Fluentd/Fluent Bit
=================================================

Many web/mobile applications generate huge amount of event logs (c,f.
login, logout, purchase, follow, etc). To analyze these event logs could
be really valuable for improving the service. However, the challenge is
collecting these logs easily and reliably.

`Fluentd <https://github.com/fluent/fluentd>`__ and `Fluent Bit <https://fluentbit.io/>`__ solves that problem by
having: easy installation, small footprint, plugins, reliable buffering,
log forwarding, etc.

**fluent-logger-python** is a Python library, to record the events from
Python application.

Requirements
------------

-  Python 3.7+
- ``msgpack``
- **IMPORTANT**: Version 0.8.0 is the last version supporting Python 2.6, 3.2 and 3.3
- **IMPORTANT**: Version 0.9.6 is the last version supporting Python 2.7 and 3.4
- **IMPORTANT**: Version 0.10.0 is the last version supporting Python 3.5 and 3.6

Installation
------------

This library is distributed as 'fluent-logger' python package. Please
execute the following command to install it.

.. code:: sh

    $ pip install fluent-logger

Configuration
-------------

Fluentd daemon must be launched with a tcp source configuration:

::

    <source>
      type forward
      port 24224
    </source>

To quickly test your setup, add a matcher that logs to the stdout:

::

    <match app.**>
      type stdout
    </match>

Usage
-----

FluentSender Interface
~~~~~~~~~~~~~~~~~~~~~~

`sender.FluentSender` is a structured event logger for Fluentd.

By default, the logger assumes fluentd daemon is launched locally. You
can also specify remote logger by passing the options.

.. code:: python

    from fluent import sender

    # for local fluent
    logger = sender.FluentSender('app')

    # for remote fluent
    logger = sender.FluentSender('app', host='host', port=24224)

For sending event, call `emit` method with your event. Following example will send the event to
fluentd, with tag 'app.follow' and the attributes 'from' and 'to'.

.. code:: python

    # Use current time
    logger.emit('follow', {'from': 'userA', 'to': 'userB'})

    # Specify optional time
    cur_time = int(time.time())
    logger.emit_with_time('follow', cur_time, {'from': 'userA', 'to':'userB'})

To send events with nanosecond-precision timestamps (Fluent 0.14 and up),
specify `nanosecond_precision` on `FluentSender`.

.. code:: python

    # Use nanosecond
    logger = sender.FluentSender('app', nanosecond_precision=True)
    logger.emit('follow', {'from': 'userA', 'to': 'userB'})
    logger.emit_with_time('follow', time.time(), {'from': 'userA', 'to': 'userB'})

You can detect an error via return value of `emit`. If an error happens in `emit`, `emit` returns `False` and get an error object using `last_error` method.

.. code:: python

    if not logger.emit('follow', {'from': 'userA', 'to': 'userB'}):
        print(logger.last_error)
        logger.clear_last_error() # clear stored error after handled errors

If you want to shutdown the client, call `close()` method.

.. code:: python

    logger.close()

Event-Based Interface
~~~~~~~~~~~~~~~~~~~~~

This API is a wrapper for `sender.FluentSender`.

First, you need to call ``sender.setup()`` to create global `sender.FluentSender` logger
instance. This call needs to be called only once, at the beginning of
the application for example.

Initialization code of Event-Based API is below:

.. code:: python

    from fluent import sender

    # for local fluent
    sender.setup('app')

    # for remote fluent
    sender.setup('app', host='host', port=24224)

Then, please create the events like this. This will send the event to
fluentd, with tag 'app.follow' and the attributes 'from' and 'to'.

.. code:: python

    from fluent import event

    # send event to fluentd, with 'app.follow' tag
    event.Event('follow', {
      'from': 'userA',
      'to':   'userB'
    })

`event.Event` has one limitation which can't return success/failure result.

Other methods for Event-Based Interface.

.. code:: python

    sender.get_global_sender # get instance of global sender
    sender.close # Call FluentSender#close

Handler for buffer overflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can inject your own custom proc to handle buffer overflow in the event of connection failure. This will mitigate the loss of data instead of simply throwing data away.

.. code:: python

    import msgpack
    from io import BytesIO

    def overflow_handler(pendings):
        unpacker = msgpack.Unpacker(BytesIO(pendings))
        for unpacked in unpacker:
            print(unpacked)

    logger = sender.FluentSender('app', host='host', port=24224, buffer_overflow_handler=overflow_handler)

You should handle any exception in handler. fluent-logger ignores exceptions from ``buffer_overflow_handler``.

This handler is also called when pending events exist during `close()`.

Python logging.Handler interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This client-library also has ``FluentHandler`` class for Python logging
module.

.. code:: python

    import logging
    from fluent import handler

    custom_format = {
      'host': '%(hostname)s',
      'where': '%(module)s.%(funcName)s',
      'type': '%(levelname)s',
      'stack_trace': '%(exc_text)s'
    }

    logging.basicConfig(level=logging.INFO)
    l = logging.getLogger('fluent.test')
    h = handler.FluentHandler('app.follow', host='host', port=24224, buffer_overflow_handler=overflow_handler)
    formatter = handler.FluentRecordFormatter(custom_format)
    h.setFormatter(formatter)
    l.addHandler(h)
    l.info({
      'from': 'userA',
      'to': 'userB'
    })
    l.info('{"from": "userC", "to": "userD"}')
    l.info("This log entry will be logged with the additional key: 'message'.")

You can also customize formatter via logging.config.dictConfig

.. code:: python

    import logging.config
    import yaml

    with open('logging.yaml') as fd:
        conf = yaml.load(fd)

    logging.config.dictConfig(conf['logging'])

You can inject your own custom proc to handle buffer overflow in the event of connection failure. This will mitigate the loss of data instead of simply throwing data away.

.. code:: python

    import msgpack
    from io import BytesIO

    def overflow_handler(pendings):
        unpacker = msgpack.Unpacker(BytesIO(pendings))
        for unpacked in unpacker:
            print(unpacked)

A sample configuration ``logging.yaml`` would be:

.. code:: python

    logging:
        version: 1

        formatters:
          brief:
            format: '%(message)s'
          default:
            format: '%(asctime)s %(levelname)-8s %(name)-15s %(message)s'
            datefmt: '%Y-%m-%d %H:%M:%S'
          fluent_fmt:
            '()': fluent.handler.FluentRecordFormatter
            format:
              level: '%(levelname)s'
              hostname: '%(hostname)s'
              where: '%(module)s.%(funcName)s'

        handlers:
            console:
                class : logging.StreamHandler
                level: DEBUG
                formatter: default
                stream: ext://sys.stdout
            fluent:
                class: fluent.handler.FluentHandler
                host: localhost
                port: 24224
                tag: test.logging
                buffer_overflow_handler: overflow_handler
                formatter: fluent_fmt
                level: DEBUG
            none:
                class: logging.NullHandler

        loggers:
            amqp:
                handlers: [none]
                propagate: False
            conf:
                handlers: [none]
                propagate: False
            '': # root logger
                handlers: [console, fluent]
                level: DEBUG
                propagate: False

Asynchronous Communication
~~~~~~~~~~~~~~~~~~~~~~~~~~

Besides the regular interfaces - the event-based one provided by ``sender.FluentSender`` and the python logging one
provided by ``handler.FluentHandler`` - there are also corresponding asynchronous versions in ``asyncsender`` and
``asynchandler`` respectively. These versions use a separate thread to handle the communication with the remote fluentd
server. In this way the client of the library won't be blocked during the logging of the events, and won't risk going
into timeout if the fluentd server becomes unreachable. Also it won't be slowed down by the network overhead.

The interfaces in ``asyncsender`` and ``asynchandler`` are exactly the same as those in ``sender`` and ``handler``, so it's
just a matter of importing from a different module.

For instance, for the event-based interface:

.. code:: python

    from fluent import asyncsender as sender

    # for local fluent
    sender.setup('app')

    # for remote fluent
    sender.setup('app', host='host', port=24224)

    # do your work
    ...

    # IMPORTANT: before program termination, close the sender
    sender.close()

or for the python logging interface:

.. code:: python

    import logging
    from fluent import asynchandler as handler

    custom_format = {
      'host': '%(hostname)s',
      'where': '%(module)s.%(funcName)s',
      'type': '%(levelname)s',
      'stack_trace': '%(exc_text)s'
    }

    logging.basicConfig(level=logging.INFO)
    l = logging.getLogger('fluent.test')
    h = handler.FluentHandler('app.follow', host='host', port=24224, buffer_overflow_handler=overflow_handler)
    formatter = handler.FluentRecordFormatter(custom_format)
    h.setFormatter(formatter)
    l.addHandler(h)
    l.info({
      'from': 'userA',
      'to': 'userB'
    })
    l.info('{"from": "userC", "to": "userD"}')
    l.info("This log entry will be logged with the additional key: 'message'.")

    ...

    # IMPORTANT: before program termination, close the handler
    h.close()

**NOTE**: please note that it's important to close the sender or the handler at program termination. This will make
sure the communication thread terminates and it's joined correctly. Otherwise the program won't exit, waiting for
the thread, unless forcibly killed.

Circular queue mode
+++++++++++++++++++

In some applications it can be especially important to guarantee that the logging process won't block under *any*
circumstance, even when it's logging faster than the sending thread could handle (*backpressure*). In this case it's
possible to enable the `circular queue` mode, by passing `True` in the `queue_circular` parameter of
``asyncsender.FluentSender`` or ``asynchandler.FluentHandler``. By doing so the thread doing the logging won't block
even when the queue is full, the new event will be added to the queue by discarding the oldest one.

**WARNING**: setting `queue_circular` to `True` will cause loss of events if the queue fills up completely! Make sure
that this doesn't happen, or it's acceptable for your application.


Testing
-------

Testing can be done using `pytest <https://docs.pytest.org>`__.

.. code:: sh

    $ pytest tests


Release
-------

.. code:: sh

    $ # Download dist.zip for release from GitHub Action artifact.
    $ unzip -d dist dist.zip
    $ pipx twine upload dist/*


Contributors
------------

Patches contributed by `those
people <https://github.com/fluent/fluent-logger-python/contributors>`__.

License
-------

Apache License, Version 2.0
