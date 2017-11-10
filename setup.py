#!/usr/bin/python

from os import path

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

README = path.abspath(path.join(path.dirname(__file__), 'README.rst'))
desc = 'A Python logging handler for Fluentd event collector'

setup(
  name='fluent-logger',
  version='0.6.0',
  description=desc,
  long_description=open(README).read(),
  package_dir={'fluent': 'fluent'},
  packages=['fluent'],
  install_requires=['msgpack-python'],
  author='Kazuki Ohta',
  author_email='kazuki.ohta@gmail.com',
  url='https://github.com/fluent/fluent-logger-python',
  download_url='http://pypi.python.org/pypi/fluent-logger/',
  license='Apache License, Version 2.0',
  classifiers=[
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 3',
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
  ],
  test_suite='tests'
)
