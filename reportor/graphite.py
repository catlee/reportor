#!/usr/bin/env python
import socket
import time
import reportor.config

import logging
log = logging.getLogger(__name__)


class GraphiteSubmitter(object):
    def __init__(self, hosts):
        """hosts is a list of (hostname, port, prefix) tuples"""
        self.hosts = hosts

        self._socks = []
        for host, port, prefix in hosts:
            s = socket.create_connection((host, port))
            self._socks.append((s, prefix))

    def __str__(self):
        return "GraphiteSubmitter: %s" % str( [(host, port) for (host, port, _) in self.hosts] )

    def submit(self, name, value, timestamp=None):
        if not timestamp:
            timestamp = int(time.time())

        for s, prefix in self._socks:
            line = "%s.%s %s %i\n" % (prefix, name, value, timestamp)
            s.sendall(line)

    def wait(self):
        for s, _ in self._socks:
            s.close()


def graphite_from_config():
    config = reportor.config.load_config()
    if not config.has_section('graphite'):
        return None

    # hosts = graphite.mozilla.org:2003:test.catlee.reportor, foo.place.com:2003:prefix
    hosts = []
    for hoststr in config.get('graphite', 'hosts').split(','):
        hoststr = hoststr.strip()
        host, port, prefix = hoststr.split(":")
        port = int(port)
        hosts.append((host, port, prefix))

    g = GraphiteSubmitter(hosts)
    # Make sure we sleep to let metrics get through. cf.
    # https://bugzilla.mozilla.org/show_bug.cgi?id=1025145
    time.sleep(1)
    return g
