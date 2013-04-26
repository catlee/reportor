#!/usr/bin/env python

from setuptools import setup

setup(name='reportor',
      version='0.1',
      description='reporting framework',
      author='Chris AtLee',
      author_email='catlee@mozilla.com',
      #packages=['reportor.py'],
      install_requires=['pyyaml', 'argparse'],
      scripts=['reportor.py'],
      )
