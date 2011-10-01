#!/usr/bin/python

from distutils.core import setup
from os import path

README = path.abspath(path.join(path.dirname(__file__), 'README.md'))
desc = 'A Python logging handler for Fluent event collector'

setup(
  name='fluent-logger',
  version='0.1.0',
  description=desc,
  long_description=open(README).read(),
  package_dir={'fluent': 'fluent'},
  packages=['fluent'],
  install_requires=['msgpack-python'],
  author='Kazuki Ohta',
  author_email='kazuki.ohta@gmail.com',
  url='https://github.com/kzk/fluent-logger-python',
  download_url='http://pypi.python.org/pypi/fluent-logger/',
  license='MIT',
  classifiers=[
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 3',
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
  ]
)
