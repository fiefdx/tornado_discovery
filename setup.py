# -*- coding: utf-8 -*-
'''
@summary: service discovery based on tornado
@author: fiefdx
'''

from setuptools import setup

from src import __version__

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name = 'tornado_discovery',
    version = __version__,
    description = "service discovery based on tornado",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = "https://github.com/fiefdx/tornado_discovery",
    author = 'fiefdx',
    author_email = 'fiefdx@163.com',
    package_dir = {'tornado_discovery': 'src'},
    packages = ['tornado_discovery'],
    install_requires = [
        "tornado >= 5.1.1"
    ],
    license = "MIT",
    classifiers = [
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ]
)
