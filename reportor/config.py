from ConfigParser import RawConfigParser
def load_config(filename):
    config = RawConfigParser()
    config.read([filename])
    return config
