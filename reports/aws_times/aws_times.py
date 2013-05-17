#!/usr/local/bin/python
from datetime import datetime, timedelta
import sqlalchemy as sa
import simplejson as json
import functools
import time
import urllib
import re

import reportor.db

def avg(l):
    return sum(l)/float(len(l))

def get_builds(db, start, end):
    q = sa.text("""
            SELECT slaves.name as slave_name, builders.name as builder_name, builds.* FROM
                builds, slaves, builders
            WHERE
                builds.slave_id = slaves.id AND
                builds.builder_id = builders.id AND
                builds.starttime >= :start AND
                builds.endtime < :end
            ORDER BY
                builds.starttime ASC
            """)
    return db.execute(q, start=datetime.utcfromtimestamp(start), end=datetime.utcfromtimestamp(end))

def is_ec2(build):
    return "-ec2" in build.slave_name


def td2s(td):
    return td.days * 86400 + td.seconds + td.microseconds/1000000.0


if __name__ == "__main__":
    t = time.time()
    status_db = reportor.db.db_from_config('status_db')
    start = time.time()-7*86400
    end = time.time()
    builds = get_builds(status_db, start, end)

    total_time = 0.0
    total_jobs = 0
    ec2_time = 0.0
    ec2_jobs = 0
    for b in builds:
        total_jobs += 1
        total_time += td2s(b.endtime - b.starttime)
        if is_ec2(b):
            ec2_jobs += 1
            ec2_time += td2s(b.endtime - b.starttime)

    report = {
            "total_time": total_time,
            "total_jobs": total_jobs,
            "ec2_time": ec2_time,
            "ec2_jobs": ec2_jobs,
            "report_start": t,
            "report_run": time.time() - t,
            "start": start,
            "end": end,
            }
    print json.dumps(report)
