#!/usr/local/bin/python
from datetime import datetime, timedelta
import sqlalchemy as sa
import simplejson as json
import functools
import time
import urllib
import re

import reportor.db
import reportor.graphite
from reportor.utils import td2s, avg

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
    return ("-ec2" in build.slave_name) or ("-spot" in build.slave_name)


def get_infra_metrics(start, end):
    status_db = reportor.db.db_from_config('status_db')

    builds = get_builds(status_db, start, end)

    retval = dict(
        total_time=0.0,
        total_jobs=0,
        ec2_time=0.0,
        ec2_jobs=0,
        )
    for b in builds:
        retval['total_jobs'] += 1
        retval['total_time'] += td2s(b.endtime - b.starttime)
        if is_ec2(b):
            retval['ec2_jobs'] += 1
            retval['ec2_time'] += td2s(b.endtime - b.starttime)

    return retval

def report_times(metrics, t):
    graphite = reportor.graphite.graphite_from_config()
    if graphite:
        ec2_time = metrics['ec2_time']
        total_time = metrics['total_time']
        ec2_jobs = metrics['ec2_jobs']
        total_jobs = metrics['total_jobs']

        graphite.submit(
                "infra_metrics.aws.time",
                ec2_time,
                t)
        graphite.submit(
                "infra_metrics.aws.num_jobs",
                ec2_jobs,
                t)
        graphite.submit(
                "infra_metrics.inhouse.time",
                total_time - ec2_time,
                t)
        graphite.submit(
                "infra_metrics.inhouse.num_jobs",
                total_jobs - ec2_jobs,
                t)


if __name__ == "__main__":
    now = time.time()

    catchup = False
    if catchup:
        # Re-play 60 days of data
        start = now - (60 * 86400)
        end = start + 86400
        while end < now:
            metrics = get_infra_metrics(start, end)
            report_times(metrics, end)
            start += 86400
            end += 86400

    start = now - 86400
    end = now

    metrics = get_infra_metrics(start, end)

    report = {
            "total_time": metrics['total_time'],
            "total_jobs": metrics['total_jobs'],
            "ec2_time": metrics['ec2_time'],
            "ec2_jobs": metrics['ec2_jobs'],
            "report_start": now,
            "report_run": time.time() - now,
            "start": start,
            "end": end,
            }

    report_times(metrics, now)
    print json.dumps(report)
