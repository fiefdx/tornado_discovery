# -*- coding: utf-8 -*-

import logging
import json

import tornado
import tornado.tcpclient
from tornado import gen
from tornado.ioloop import IOLoop

from .common import crc32sum, Command, Status, Message

LOG = logging.getLogger(__name__)


class BaseRegistrant(object):
    def __init__(self, host, port, config, retry_interval = 10, reconnect = True):
        self.host = host
        self.port = port
        self.config = config
        self.retry_interval = retry_interval
        self.heartbeat_interval = self.config.get("heartbeat_interval")
        self.heartbeat_timeout = self.config.get("heartbeat_timeout")
        self.reconnect = reconnect
        self.tcpclient = tornado.tcpclient.TCPClient()
        self.periodic_heartbeat = None
        self._stream = None

    @gen.coroutine
    def connect(self, delay = False):
        try:
            if delay == False:
                stream = yield self.tcpclient.connect(self.host, self.port)
                self._on_connect(stream)
            else:
                LOG.info("Connect to Server failed: Retry %s seconds later ...", self.retry_interval)
                IOLoop.instance().add_timeout(IOLoop.time(IOLoop.instance()) + self.retry_interval ,self.connect)
        except Exception as e:
            LOG.exception(e)
            if self.reconnect == True:
                LOG.info("Connect to Server failed: Retry %s seconds later ...", self.retry_interval)
                IOLoop.instance().add_timeout(IOLoop.time(IOLoop.instance()) + self.retry_interval ,self.connect)

    def _on_connect(self, stream):
        LOG.info("Client on connect")
        self._stream = stream
        self._stream.set_close_callback(self._on_close)
        LOG.debug("self.stream: %s: %s", type(self._stream), self._stream.fileno())
        IOLoop.instance().add_callback(self.register_service)
        self.periodic_heartbeat = tornado.ioloop.PeriodicCallback(
            self.heartbeat_service, 
            self.heartbeat_interval * 1000
        )
        self.periodic_heartbeat.start()

    @gen.coroutine
    def read_message(self):
        data = {"command": Command.error, "data": "Client received wrong message!"}
        msg = yield self._stream.read_until(Message.msg_end)
        data_string, data_crc32 = msg.strip().split(Message.msg_sp)
        if crc32sum(data_string) == data_crc32:
            data = json.loads(data_string.decode("utf-8"))
        raise gen.Return(data)

    @gen.coroutine
    def send_message(self, data):
        try:
            data_string = json.dumps(data).encode("utf-8")
            data_crc32 = crc32sum(data_string)
            LOG.debug("Send: %s", data)
            msg = b"%s%s%s%s" % (data_string, Message.msg_sp, data_crc32, Message.msg_end)
            yield self._stream.write(msg)
        except Exception as e:
            LOG.exception(e)

    @gen.coroutine
    def register_service(self):
        try:
            data = {"command": Command.register, "data": self.config.to_dict()}
            self.send_message(data)
            data = yield self.read_message()
            if not self.config.has_key("node_id"):
                self.config.set("node_id", data["data"]["node_id"])
                LOG.info("Received new node_id: %s", data["data"]["node_id"])
            LOG.info("Client Register Received Message: %s", data)
        except Exception as e:
            LOG.exception(e)

    @gen.coroutine
    def unregister_service(self):
        try:
            data = {"command": Command.unregister, "data": "Client Unregister Service!"}
            self.send_message(data)
            data = yield self.read_message()
            LOG.info("Client Received Message: %s", data)
        except Exception as e:
            LOG.exception(e)

    @gen.coroutine
    def heartbeat_service(self):
        try:
            data = {"command": Command.heartbeat, "data": self.config.to_dict()}
            self.send_message(data)
            data = yield self.read_message()
            if data["data"]["status"] == Status.success:
                LOG.debug("Client Received Heartbeat Message: %s", data["data"])
            else:
                LOG.error("Client Received Heartbeat Message: %s", data["data"])
        except Exception as e:
            LOG.exception(e)

    def close(self):
        try:
            if self._stream:
                self._stream.set_close_callback(None)
            self.reconnect = False
            if self.periodic_heartbeat:
                self.periodic_heartbeat.stop()
            IOLoop.instance().add_timeout(IOLoop.time(IOLoop.instance()) + 5 ,
                                          lambda :(self._stream.close() if self._stream else None, 
                                                   self.tcpclient.close(), 
                                                   LOG.info("Close Client!")))
        except Exception as e:
            LOG.exception(e)

    def _on_close(self):
        try:
            LOG.info("Client closed by Server refused!")
            self._stream.close()
            if self.periodic_heartbeat:
                self.periodic_heartbeat.stop()
            if self.reconnect:
                LOG.info("Reconnect to Server ...")
                self.connect(delay = True)
            else:
                LOG.info("Close Client!")
                self.tcpclient.close()
        except Exception as e:
            LOG.exception(e)
