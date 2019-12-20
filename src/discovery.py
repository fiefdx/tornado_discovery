# -*- coding: utf-8 -*-

import os
import logging
import json
import binascii
import uuid
from uuid import uuid4

import tornado.tcpserver
import tornado.tcpclient
import tornado.iostream
from tornado import gen
from tornado.ioloop import IOLoop

from .common import crc32sum, Command, Status, Message

LOG = logging.getLogger(__name__)


class Connection(object):
    clients = set()
    msg_end = b"\r\n\r\n\r\n"
    msg_sp = b"\r\n\r\n"
    status = Status.red

    def __init__(self, stream, address):
        Connection.clients.add(self)
        self._stream = stream
        self._address = address
        self._stream.set_close_callback(self._on_close)
        self._status = Status.connected
        self.info = {}
        self._heartbeat_timeout = None
        self._on_connect()
        LOG.info("Client (%s) Register", self._address)

    @classmethod
    def get_cluster_status(cls):
        result = False
        try:
            if len(Connection.clients) == 0:
                Connection.status = Status.red
            else:
                Connection.status = Status.green
            result = Connection.status
        except Exception as e:
            LOG.exception(e)
        return result

    @gen.coroutine
    def read_message(self):
        data = {"command": Command.error, "data": Message.received_wrong_msg}
        msg = yield self._stream.read_until(Connection.msg_end)
        data_string, data_crc32 = msg.strip().split(Connection.msg_sp)
        if crc32sum(data_string) == data_crc32:
            data = json.loads(data_string.decode("utf-8"))
            LOG.debug("Received: %s", data)
        raise gen.Return(data)

    @gen.coroutine
    def send_message(self, data, refuse_connect_flag = False):
        try:
            data_string = json.dumps(data).encode("utf-8")
            data_crc32 = crc32sum(data_string)
            LOG.debug("Send: %s", data)
            msg = b"%s%s%s%s" % (data_string, Connection.msg_sp, data_crc32, Connection.msg_end)
            yield self._stream.write(msg)
            if refuse_connect_flag:
                self._refuse_connect()
        except Exception as e:
            LOG.exception(e)

    def broadcast_message(self, data):
        for client in Connection.clients:
            client.send_message(data)

    def status(self):
        LOG.debug("Writing(%s): %s", self._address, self._stream.writing())
        LOG.debug("Reading(%s): %s", self._address, self._stream.reading())

    @gen.coroutine
    def _on_connect(self):
        try:
            while True:
                refuse_connect_flag = False
                data = yield self.read_message()
                send_data = {}
                # register
                if "command" in data and data["command"] == Command.register:
                    self.info = data["data"]
                    if self.info["http_host"] == "0.0.0.0":
                        self.info["http_host"] = self._address[0]
                    # no node_id
                    if "node_id" not in self.info or self.info["node_id"] == None:
                        node_id = str(uuid4())
                        send_data = {
                            "command": Command.register,
                            "data": {
                                "status": Status.success,
                                "message": Status.success,
                                "node_id": node_id,
                            }
                        }
                        self.info["node_id"] = node_id
                        self._status = Status.registered
                    # register with node_id
                    else:
                        send_data = {
                            "command": Command.register,
                            "data": {
                                "status": Status.success,
                                "message": Status.success,
                                "node_id": self.info["node_id"],
                            }
                        }
                        self._status = Status.registered
                elif "command" in data and data["command"] == Command.heartbeat:
                    self.info = data["data"]
                    if self.info["http_host"] == "0.0.0.0":
                        self.info["http_host"] = self._address[0]
                    if self._status == Status.registered:
                        send_data = {
                            "command": Command.heartbeat,
                            "data": {
                                "status": Status.success,
                                "message": Status.success,
                            }
                        }
                        if self._heartbeat_timeout:
                            IOLoop.instance().remove_timeout(self._heartbeat_timeout)
                        self._heartbeat_timeout = IOLoop.instance().add_timeout(
                            IOLoop.time(IOLoop.instance()) + self.info["heartbeat_timeout"],
                            self._remove_connection
                            )
                    else:
                        send_data = {
                            "command": Command.heartbeat,
                            "data": {
                                "status": Status.failure,
                                "message": "invalid node_id: %s" % self.info["node_id"],
                            }
                        }
                        refuse_connect_flag = True
                else:
                    LOG.error("Client(%s) invaild message error: %s", self._address, data)
                    send_data = {
                        "command": Command.error,
                        "data": {
                            "status": Status.failure,
                            "message": "Unknown Command!",
                        }
                    }
                self.send_message(send_data, refuse_connect_flag = refuse_connect_flag)
        except tornado.iostream.StreamClosedError:
            LOG.info("Closed: %s", self._address)
        except Exception as e:
            LOG.exception(e)

    def _remove_connection(self):
        if self in Connection.clients:
            Connection.clients.remove(self)
        self._stream.close()
        LOG.warning("Client(%s) node_id: %s heartbeat_timeout", self._address, self.info["node_id"])

    def _refuse_connect(self):
        if self._heartbeat_timeout:
            IOLoop.instance().remove_timeout(self._heartbeat_timeout)
        if self in Connection.clients:
            Connection.clients.remove(self)
        self._stream.close()
        LOG.warning("Refuse(%s) node_id: %s connect", self._address, self.info["node_id"])

    def _on_close(self):
        if self._heartbeat_timeout:
            IOLoop.instance().remove_timeout(self._heartbeat_timeout)
        if self in Connection.clients:
            Connection.clients.remove(self)
        self._stream.close()
        LOG.info("Client(%s) closed", self._address)


class DiscoveryListener(tornado.tcpserver.TCPServer):
    def __init__(self, connection_cls, ssl_options = None, **kwargs):
        LOG.info("DiscoveryListener start")
        self.connection_cls = connection_cls
        tornado.tcpserver.TCPServer.__init__(self, ssl_options = ssl_options, **kwargs)

    def handle_stream(self, stream, address):
        LOG.debug("Incoming connection from %r", address)
        self.connection_cls(stream, address)


class DiscoveryRegistrant(object):
    def __init__(self, host, port, config, retry_interval = 10, reconnect = True):
        self.host = host
        self.port = port
        self.config = config
        self.retry_interval = retry_interval
        self.reconnect = reconnect
        self.tcpclient = tornado.tcpclient.TCPClient()
        self.periodic_heartbeat = None
        self._stream = None
        self.node_id = None

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
            self.config.get("heartbeat_interval") * 1000
        )
        self.periodic_heartbeat.start()

    @gen.coroutine
    def read_message(self):
        data = {"command": Command.error, "data": "Client received wrong message!"}
        msg = yield self._stream.read_until(Connection.msg_end)
        data_string, data_crc32 = msg.strip().split(Connection.msg_sp)
        if crc32sum(data_string) == data_crc32:
            data = json.loads(data_string.decode("utf-8"))
        raise gen.Return(data)

    @gen.coroutine
    def send_message(self, data):
        try:
            data_string = json.dumps(data).encode("utf-8")
            data_crc32 = crc32sum(data_string)
            LOG.info("Send: %s", data)
            msg = b"%s%s%s%s" % (data_string, Connection.msg_sp, data_crc32, Connection.msg_end)
            yield self._stream.write(msg)
        except Exception as e:
            LOG.exception(e)

    @gen.coroutine
    def register_service(self):
        try:
            data = {"command": Command.register, "data": {"host": self.host, "port": self.port}}
            self.send_message(data)
            data = yield self.read_message()
            if self.node_id == None:
                self.node_id = data["data"]["node_id"]
                self.config.set("node_id", self.node_id)
                LOG.info("Received new node_id: %s", self.node_id)
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
            data = {"command": Command.heartbeat, "data": {"host": self.host, "port": self.port, "node_id": self.node_id}}
            self.send_message(data)
            data = yield self.read_message()
            if data["data"]["status"] == Status.success:
                LOG.info("Client Received Heartbeat Message: %s", data["data"])
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
