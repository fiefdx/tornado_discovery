# -*- coding: utf-8 -*-

import binascii


def crc32sum(data, crc = None):
    '''
    data is bytes
    crc is a CRC32 bytes
    '''
    result = ""
    if crc == None:
        result = b"%08X" % (binascii.crc32(data) & 0xffffffff)
    else:
        crc = int(crc, 16)
        result = b"%08X" % (binascii.crc32(data, crc) & 0xffffffff)
    return result


class Command(object):
    register = "REGISTER"
    unregister = "UNREGISTER"
    heartbeat = "HEARTBEAT"
    error = "ERROR"
    warning = "WARNING"
    message = "MESSAGE"


class Status(object):
    success = "SUCCESS"
    failure = "FAILURE"
    green = "GREEN"
    yellow = "YELLOW"
    red = "RED"
    connected = "CONNECTED"
    registered = "REGISTERED"


class Message(object):
    msg_end = b"\r\n\r\n\r\n"
    msg_sp = b"\r\n\r\n"
    received_wrong_msg = "Server received wrong message!"
