# A Python structured logger for Fluentd


Many web/mobile applications generate huge amount of event logs (c,f. login, logout, purchase, follow, etc). To analyze these event logs could be really valuable for improving the service. However, the challenge is collecting these logs easily and reliably.

[Fluentd](http://github.com/fluent/fluentd) solves that problem by having: easy installation, small footprint, plugins, reliable buffering, log forwarding, etc.

**fluent-logger-python** is a Python library, to record the events from Python application.

## Installation

This library is distributed as 'fluent-logger' python package. Please execute the following command to install it.

    $ pip install fluent-logger

## Configuration

Fluent daemon must be lauched with the following configuration:

    <source>
      type tcp
      port 24224
    </source>

    <match app.**>
      type stdout
    </match>

## Usage

### Event-Based Interface

First, you need to call logger.setup() to create global logger instance. This call needs to be called only once, at the beggining of the application for example.

By default, the logger assumes fluent daemon is launched locally. You can also specify remote logger by passing the options.

    from fluent import sender
    
    # for local fluent
    sender.setup('app')
    
    # for remote fluent
    sender.setup('app', host='host', port=24224)

Then, please create the events like this. This will send the event to fluent, with tag 'app.follow' and the attributes 'from' and 'to'.

    from fluent import event

    # send event to fluentd, with 'app.follow' tag
    event.Event('follow', {
      'from': 'userA',
      'to':   'userB'
    })

### Python logging.Handler interface

This client-library also has FluentHanler class for Python logging module.

    import logging
    from fluent import handler
    
    logging.basicConfig(level=logging.INFO)
    l = logging.getLogger('fluent.test')
    l.addHandler(handler.FluentHandler('app.follow', host='host', port=24224))
    l.info({
      'from': 'userA',
      'to': 'userB'
    })
    
## Contributors

* [Ken Sato](https://github.com/ksato9700)
* [Naoki Inada](http://github.com/methane)

## License

Apache License, Version 2.0
