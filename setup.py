#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

requirements = [
    'jsonschema==2.6.0',
    'requests==2.18.1',
    'rfc3987==1.3.7',  # For 'uri' format validation in jsonschema
    'supervisor==3.3.1',
    'PyYAML==3.12',
    'wheel',
    'multitail2',
]

test_requirements = [
    'httmock',
    'coverage',
    'mock',
]

dependency_links = [
]

setup(
    name='hg-agent-forwarder',
    version='1.0.2',
    description='Metric forwarder script for the Hosted Graphite agent.',
    long_description='Metric forwarder script for the Hosted Graphite agent.',
    author='Metricfire',
    author_email='maintainer@metricfire.com',
    url='https://github.com/metricfire/hg-agent-forwarder',
    packages=find_packages(),
    package_data={},
    scripts=[],
    install_requires=requirements,
    dependency_links=dependency_links,
    tests_require=test_requirements,
    test_suite='tests',
    include_package_data=True,
    zip_safe=False,
    keywords='hg-agent-forwarder',
    classifiers=[
    ],
)
