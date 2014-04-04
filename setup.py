#!/usr/bin/env python

from setuptools import setup

setup(name='reportor',
      version='0.2',
      description='reporting framework',
      author='Chris AtLee',
      author_email='catlee@mozilla.com',
      packages=['reportor'],
      install_requires=['pyyaml', 'argparse', 'lockfile','boto','furl','requests'],
      scripts=['scripts/reportor'],
      )
