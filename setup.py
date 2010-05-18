#!/usr/bin/env python

from distutils.core import setup

setup(
    name='Norm',
    version='0.1',
    description='Simple MySQLdb ORM',
    author='Josh Marshall',
    author_email='catchjosh@gmail.com',
    packages=['Norm',],
    requires=['MySQLdb',]
)


