#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: sagarraichandani
"""


import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()
    
setuptools.setup(
    name='gservice-api',
    version='0.0.3',
    author='Sagar Raichandani',
    author_email='raichandanisagar@gmail.com',
    description='Python package that makes working with Google APIs simpler.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/raichandanisagar/gservice-api',
    license='MIT',
    packages=['gservice_api_tools'],
    install_requires=['google-api-core','google-api-python-client','google-auth',
                      'google-auth-httplib2','google-auth-oauthlib','googleapis-common-protos',
                      'pandas']
    
)