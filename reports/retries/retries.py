#!/usr/bin/env python
import os
import requests
import datetime
import json
import time
import logging
import sqlalchemy as sa
from reportor.utils import td2s, dt2ts

def get_jobs(db, branch, start, end):
    q = sa.text("""
    SELECT buildrequests.id, buildrequests.submitted_at, count(builds.id) as c
    FROM buildsets, buildrequests, builds WHERE
        buildrequests.buildsetid = buildsets.id AND
        builds.brid = buildrequests.id AND
        buildrequests.buildername like :branch AND
        buildsets.submitted_at >= :start AND
        buildsets.submitted_at < :end AND
        buildrequests.complete = 1

        GROUP BY buildrequests.id
        ORDER BY submitted_at ASC
        """)
    builds = db.execute(q, start=start, end=end, branch='%{0}%'.format(branch))
    return list(builds)

if __name__ == '__main__':
    import reportor.db
    db = reportor.db.db_from_config('scheduler_db')

    now = time.time()
    start = now - 45*86400

    s = time.time()

    jobs = get_jobs(db, 'mozilla-inbound', start, now)
    results = []
    for j in jobs:
        results.append((j.submitted_at, j.c))

    e = time.time()
    report = {
            "data": results,
            "report_start": s,
            "report_elapsed": e-s,
            "data_start": start,
            "data_end": now,
            }
    print json.dumps(report)
