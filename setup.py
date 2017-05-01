#!/usr/bin/env python

from setuptools import setup


setup(
    name='metags',
    version='0.0.1',
    description='Searchable metadata for unique data identified by url.',
    long_description=open('README.md').read(),
    author='Sam Bourne',
    packages=['metags', 'metags.storage', 'metags.plugins'],
)
