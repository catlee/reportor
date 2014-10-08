#!/usr/bin/env python
import socket
import time
import reportor.config

import logging
log = logging.getLogger(__name__)


class GraphiteSubmitter(object):
    def __init__(self, host, port, prefix):
        self.host = host
        self.port = port
        self.prefix = prefix
        self._sock = socket.create_connection((host, port))

    def submit(self, name, value, timestamp=None):
        line = "%s.%s %s" % (self.prefix, name, value)
        if not timestamp:
            timestamp = int(time.time())

        if timestamp:
            line += " %i" % timestamp

        log.debug(str(line))
        self._sock.sendall(line + "\n")

    def wait(self):
        self._sock.close()


def graphite_from_config():
    config = reportor.config.load_config()
    if not config.has_section('graphite'):
        return None
    g = GraphiteSubmitter(
        host=config.get('graphite', 'hostname'),
        port=config.getint('graphite', 'port'),
        prefix=config.get('graphite', 'prefix'),
    )
    # Make sure we sleep to let metrics get through. cf.
    # https://bugzilla.mozilla.org/show_bug.cgi?id=1025145
    #time.sleep(1)
    return g
