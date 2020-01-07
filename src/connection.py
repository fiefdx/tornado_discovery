# -*- coding: utf-8 -*-

import json
import logging
import uuid
from uuid import uuid4

import tornado.iostream
from tornado import gen
from tornado.ioloop import IOLoop

from .common import crc32sum, Command, Status, Message

LOG = logging.getLogger(__name__)


class BaseConnection(object):
    clients = set()
    status = Status.red

    def __init__(self, stream, address):
        BaseConnection.clients.add(self)
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
            if len(BaseConnection.clients) == 0:
                BaseConnection.status = Status.red
            else:
                BaseConnection.status = Status.green
            result = BaseConnection.status
        except Exception as e:
            LOG.exception(e)
        return result

    @gen.coroutine
    def read_message(self):
        data = {"command": Command.error, "data": Message.received_wrong_msg}
        msg = yield self._stream.read_until(Message.msg_end)
        data_string, data_crc32 = msg.strip().split(Message.msg_sp)
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
            msg = b"%s%s%s%s" % (data_string, Message.msg_sp, data_crc32, Message.msg_end)
            yield self._stream.write(msg)
            if refuse_connect_flag:
                self._refuse_connect()
        except Exception as e:
            LOG.exception(e)

    def broadcast_message(self, data):
        for client in BaseConnection.clients:
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
        if self in BaseConnection.clients:
            BaseConnection.clients.remove(self)
        self._stream.close()
        LOG.warning("Client(%s) node_id: %s heartbeat_timeout", self._address, self.info["node_id"])

    def _refuse_connect(self):
        if self._heartbeat_timeout:
            IOLoop.instance().remove_timeout(self._heartbeat_timeout)
        if self in BaseConnection.clients:
            BaseConnection.clients.remove(self)
        self._stream.close()
        LOG.warning("Refuse(%s) node_id: %s connect", self._address, self.info["node_id"])

    def _on_close(self):
        if self._heartbeat_timeout:
            IOLoop.instance().remove_timeout(self._heartbeat_timeout)
        if self in BaseConnection.clients:
            BaseConnection.clients.remove(self)
        self._stream.close()
        LOG.info("Client(%s) closed", self._address)
