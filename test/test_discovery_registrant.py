# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import logging

import tornado.ioloop

from tornado_discovery.config import BaseConfig
from tornado_discovery.registrant import BaseRegistrant

import logger

LOG = logging.getLogger(__name__)



if __name__ == "__main__":
    logger.config_logging(file_name = "test_discovery_registrant.log",
                          log_level = "DEBUG",
                          dir_name = "logs",
                          day_rotate = False,
                          when = "D",
                          interval = 1,
                          max_size = 20,
                          backup_count = 5,
                          console = True)

    LOG.debug("test start")
    
    try:
        config = BaseConfig()
        config.from_dict({"heartbeat_interval": 1, "heartbeat_timeout": 10, "http_host": "127.0.0.1", "http_port": 8001})
        registrant = BaseRegistrant("127.0.0.1", 6001, config)
        tornado.ioloop.IOLoop.instance().add_callback(registrant.connect)
        tornado.ioloop.IOLoop.instance().start()
    except Exception as e:
        LOG.exception(e)

    LOG.debug("test end")
