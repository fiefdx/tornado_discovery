# -*- coding: utf-8 -*-
'''
@summary: service discovery based on tornado
@author: fiefdx
'''

from setuptools import setup

setup(
	name = 'tornado_discovery',
    version = '0.0.2',
    author = 'fiefdx',
    author_email = 'fiefdx@163.com',
    package_dir = {'tornado_discovery': 'src'},
    packages = ['tornado_discovery'],
    install_requires = [
        "tornado >= 5.1.1"
    ]
)
