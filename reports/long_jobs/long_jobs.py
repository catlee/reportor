#!/usr/local/bin/python
from datetime import datetime, timedelta
import sqlalchemy as sa
import simplejson as json
import time

import reportor.db

def avg(l):
    return sum(l)/float(len(l))

def find_long_running(db, starttime):
    q = sa.text("""
        SELECT buildrequests.*, builds.start_time, builds.number FROM
            buildrequests, builds
        WHERE
            buildrequests.complete = 0 AND
            builds.brid = buildrequests.id AND
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
yesterday = time.time() - 86400
results = {}
results['long_running'] = list(dict(row) for row in find_long_running(scheduler_db, yesterday))
results['long_pending'] = list(dict(row) for row in find_long_pending(scheduler_db, yesterday))

print json.dumps(results)
