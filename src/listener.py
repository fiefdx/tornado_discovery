# -*- coding: utf-8 -*-

import logging

import tornado.tcpserver

LOG = logging.getLogger(__name__)


class BaseListener(tornado.tcpserver.TCPServer):
    def __init__(self, connection_cls, ssl_options = None, **kwargs):
        LOG.info("DiscoveryListener start")
        self.connection_cls = connection_cls
        tornado.tcpserver.TCPServer.__init__(self, ssl_options = ssl_options, **kwargs)

    def handle_stream(self, stream, address):
        LOG.debug("Incoming connection from %r", address)
        self.connection_cls(stream, address)
