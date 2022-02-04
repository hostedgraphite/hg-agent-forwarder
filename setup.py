#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

requirements = [
    'jsonschema<=3.2.0',
    'requests<=2.27.1',
    'supervisor<=4.2.4',
    'PyYAML<=6.0',
    'multitail2 @ git+ssh://git@github.com/metricfire/python-multitail2.git@v2.0.0',
]

setup(
    name='hg-agent-forwarder',
    version='2.0.0',
    description='Metric forwarder script for the Hosted Graphite agent.',
    long_description='Metric forwarder script for the Hosted Graphite agent.',
    author='Metricfire',
    author_email='maintainer@metricfire.com',
    url='https://github.com/metricfire/hg-agent-forwarder',
    packages=find_packages(),
    install_requires=requirements,
    test_suite='tests',
    include_package_data=True,
    zip_safe=False,
    keywords='hg-agent-forwarder',
)
