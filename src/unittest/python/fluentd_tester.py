import os
import tempfile
from collections import namedtuple

import docker

"""
    with Popen(["docker", "run", "-i",
                "-p", "24224:24224", "-p", "24224:24224/udp",
                "-p", "24225:24225", "-p", "24225:24225/udp",
                "-p", "24226:24226", "-p", "24226:24226/udp",
                "-v", "%s:/fluentd/log" % abspath("../tests"),
                "-v", "%s:/fluentd/etc/fluent.conf" % abspath("../tests/fluent.conf"),
                "-v", "%s:/var/run/fluent" % abspath("../tests/fluent_sock"),
                "fluent/fluentd:v1.1.0"]) as docker:
"""
PROTOS_SUPPORTED = ("tcp", "tcp+tls", "udp", "unix", "unix+tls")
FluentConfig = namedtuple("FluentConfig", ["proto", "proto_confs", "port"], defaults=[{}, None])


class FluentInstance:
    def __init__(self, config, *, port_generator, docker, data_dir):
        if config.proto not in PROTOS_SUPPORTED:
            raise ValueError("proto must be one of %r" % (PROTOS_SUPPORTED,))

        self.config = config
        if config.proto.startswith("unix"):
            self.port = tempfile.mktemp()
        else:
            self.port = config.port or port_generator()
        self.config_dir = tempfile.TemporaryDirectory()
        self.docker = docker
        self.data_dir = data_dir

    def start(self):
        pass

    def cleanup(self):
        self.config_dir.cleanup()
        if self.config.proto.startswith("unix"):
            os.unlink(self.config.proto)


class FluentDockerTester:
    def __init__(self, fluent_image, configs, *, base_port=24224):
        def port_generator():
            nonlocal base_port
            try:
                return base_port
            finally:
                base_port += 1

        self.docker = docker.from_env()
        self.data_dir = tempfile.TemporaryDirectory()
        self.fluent_image = fluent_image
        self.instances = {}
        for config in configs:
            self.instances[config] = FluentInstance(config,
                                                    port_generator=port_generator,
                                                    docker=self.docker,
                                                    data_dir=self.data_dir)

    def setUp(self):
        pass

    def tearDown(self):
        for config in self.instances:
            config.cleanup()
