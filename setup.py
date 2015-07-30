#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name="Azusa",
    version="0.1.2",
    description="Wrapping module of Azkaban v2.5 API & Job creator",
    author="Tasuku OKUDA",
    author_email="okdtsk@gmail.com",
    url="http://github.com/okdtsk/Azukaban-azusa",
    packages=find_packages(),
    install_requires=[
        "beautifulsoup4",
        "networkx",
        "requests",
    ],
    extra_requires={
        'doc': ['sphinx', 'sphinx_rtd_theme']
    }
)
