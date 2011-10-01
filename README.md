A Python structured logger for Fluent
=====================================

Many web/mobile applications generate huge amount of event logs (c,f. login, logout, purchase, follow, etc). To analyze these event logs could be really valuable for improving the service. However, the challenge is collecting these logs seasily and reliably.

[Fluent](http://github.com/fluent/fluent) solves that problem by having: easy installation, small footprint, plugins, reliable buffering, log forwarding, etc.

**fluent-logger-python** is a Python library, to record the events from Python application.

Installation
------------

This library is distributed as 'fluent-logger' python package. Please execute the following command to install it.

    $ pip install fluent-logger

Configuration
-------------

Fluent daemon must be lauched with the following configuration:

    <source>
      type tcp
      port 24224
    </source>

Use
---

First, you need to call logger.setup() to create global logger instance. This call needs to be called only once, at the beggining of the application for example.

By default, the logger assumes fluent daemon is launched locally. You can also specify remote logger by passing the options.

    from fluent import logger
    
    # for local fluent
    logger.setup('app')
    
    # for remote fluent
    logger.setup('app', server='host', port='24224')

Then, please create the events like this. This will send the event to fluent, with tag 'app.follow' and the attributes 'from' and 'to'.

    from fluent import event

    # send event with tag app.follow                                                                                          
    event.Event('follow', {
      'from': 'userA',
      'to':   'userB'
    })

License
-------

Apache License, Version 2.0
