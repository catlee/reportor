#!/usr/local/bin/python
from datetime import datetime, timedelta
import sqlalchemy as sa
import simplejson as json
import time
import requests

import reportor.db

def avg(l):
    return sum(l)/float(len(l))

_masters = None
def getMasters():
    global _masters
    if _masters:
        return _masters
    _masters = requests.get("http://hg.mozilla.org/build/tools/raw-file/default/buildfarm/maintenance/production-masters.json").json()
    return _masters

def getMaster(db_name):
    for m in getMasters():
        if m['db_name'] == db_name:
            return m

def add_master_links(running):
    for build in running:
        m = getMaster(build['claimed_by_name'])
        d = m.copy()
        d.update(build)
        link = "http://{hostname}:{http_port}/builders/{buildername}/builds/{number}".format(**d)
        build['master_link'] = link

def find_long_running(db, starttime):
    q = sa.text("""
        SELECT buildrequests.*, builds.start_time, builds.finish_time, builds.number FROM
            buildrequests, builds
        WHERE
            buildrequests.complete = 0 AND
            builds.brid = buildrequests.id AND
            builds.finish_time IS NULL AND
            builds.start_time < :starttime
        """)

    results = db.execute(q, starttime=starttime)
    return results

def find_long_pending(db, starttime):
    q = sa.text("""
        SELECT buildrequests.* FROM
            buildrequests
        WHERE
            buildrequests.complete = 0 AND
            buildrequests.claimed_at = 0 AND
            buildrequests.submitted_at < :starttime
        """)

    results = db.execute(q, starttime=starttime)
    return results


scheduler_db = reportor.db.db_from_config('scheduler_db')
yesterday = time.time() - 12*3600
results = {}
results['long_running'] = list(dict(row) for row in find_long_running(scheduler_db, yesterday))
results['long_pending'] = list(dict(row) for row in find_long_pending(scheduler_db, yesterday))

# Add master_link to long_running
add_master_links(results['long_running'])

print json.dumps(results)
