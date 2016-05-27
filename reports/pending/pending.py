#!/usr/bin/env python
import requests
from collections import Counter
import logging

PENDING_URL = "http://buildapi.pvt.build.mozilla.org/buildapi/pending?format=json"
RUNNING_URL = "http://buildapi.pvt.build.mozilla.org/buildapi/running?format=json"
ALLTHETHINGS_URL = "https://secure.pub.build.mozilla.org/builddata/reports/allthethings.json"


def pool_for_builder(allthethings, buildername):
    try:
        pool_id = allthethings['builders'][buildername]['slavepool']
        pool = allthethings['slavepools'][pool_id]
        first_name = pool[0]
        # Strip off final -XXX
        pool = first_name.rsplit("-", 1)[0]
        return pool
    except:
        logging.warning('%s: unknown pool', buildername)
        logging.debug('%s: unknown pool', buildername, exc_info=True)
        return 'unknown'

if __name__ == '__main__':
    import reportor.graphite
    import json
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

    logging.info('Fetching pending info from %s', PENDING_URL)
    pending = requests.get(PENDING_URL).json()
    logging.info('Fetching running info from %s', RUNNING_URL)
    running = requests.get(RUNNING_URL).json()
    logging.info('Fetching allthethings from %s', ALLTHETHINGS_URL)
    allthethings = requests.get(ALLTHETHINGS_URL).json()

    pending_by_pool = Counter()
    running_by_pool = Counter()

    for pool in allthethings['slavepools'].values():
        pending_by_pool[pool[0].rsplit('-', 1)[0]] = 0
        running_by_pool[pool[0].rsplit('-', 1)[0]] = 0

    for branch, revisions in pending['pending'].items():
        for rev, builds in revisions.items():
            for b in builds:
                buildername = b['buildername']
                pool = pool_for_builder(allthethings, buildername)
                pending_by_pool[pool] += 1
    print json.dumps(pending_by_pool)

    for branch, revisions in running['running'].items():
        for rev, builds in revisions.items():
            for b in builds:
                buildername = b['buildername']
                pool = pool_for_builder(allthethings, buildername)
                running_by_pool[pool] += 1
    print json.dumps(running_by_pool)

    logging.info('Submitting to graphite')
    g = reportor.graphite.graphite_from_config()
    for pool, count in pending_by_pool.items():
        g.submit('releng.pending.{}'.format(pool), count)
    for pool, count in running_by_pool.items():
        g.submit('releng.running.{}'.format(pool), count)
