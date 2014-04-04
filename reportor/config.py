from ConfigParser import RawConfigParser
import os

def load_config(filename=None):
    if filename is None:
        if 'REPORTOR_CREDS' in os.environ:
            filename = os.environ['REPORTOR_CREDS']
        else:
            raise Exception("Need a filename")
    config = RawConfigParser()
    config.read([filename])
    return config
