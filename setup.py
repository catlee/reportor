#!/usr/bin/env python

from setuptools import setup

setup(name='reportor',
      version='0.3',
      description='reporting framework',
      author='Chris AtLee',
      author_email='catlee@mozilla.com',
      packages=['reportor'],
      install_requires=[
          'pyyaml', 'argparse', 'lockfile', 'boto', 'furl', 'requests',
          'pyOpenSSL', 'ndg-httpsclient', 'pyasn1', 'beautifulsoup4',
          'boto', 'pytz',
      ],
      scripts=['scripts/reportor'],
      )
