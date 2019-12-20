# -*- coding: utf-8 -*-

class BaseConfig(object):
    def __init__(self):
        self.config = {}

    def has_key(self, key):
        return key in self.config

    def update(self, key, value):
        self.config.update({key: value})

    def get(self, key):
        return self.config[key]

    def set(self, key, value):
        self.config[key] = value

    def delete(self, key):
        del(self.config[key])

    def to_dict(self):
        return self.config

    def from_dict(self, data):
        self.config = data
