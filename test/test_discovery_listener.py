# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import logging

import tornado.ioloop

from tornado_discovery.connection import BaseConnection
from tornado_discovery.listener import BaseListener

import logger

LOG = logging.getLogger(__name__)



if __name__ == "__main__":
    logger.config_logging(file_name = "test_discovery_listener.log",
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
        listener = BaseListener(BaseConnection)
        listener.listen(6001)
        tornado.ioloop.IOLoop.instance().start()
    except Exception as e:
        LOG.exception(e)

    LOG.debug("test end")
