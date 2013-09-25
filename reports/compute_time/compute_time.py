#!/usr/local/bin/python
from datetime import datetime, timedelta
from collections import defaultdict
import time

import simplejson as json
import sqlalchemy as sa

from reportor.utils import dt2ts

def get_builds(db, start, end):
    q = sa.text("""
        SELECT DISTINCT
            buildrequests.buildername, buildrequests.claimed_by_name, builds.start_time, builds.finish_time
        FROM
            buildrequests, builds
        WHERE
            builds.brid = buildrequests.id AND
            builds.start_time >= :start AND
            builds.finish_time <= :end IS NOT NULL
        """)
    return db.execute(q, start=start, end=end)


def round_down_to(n, r):
    """Round n down to the nearest multiple of r"""
    return int(int(n/r) * r)


def count_time(builds, interval=3600):
    """
    Counts the compute time of builds running per interval
    Returns a dictionary of interval times (rounded to the nearest interval)
    mapped to number of hours for jobs that started then
    """
    result = defaultdict(int)
    for build in builds:
        s = build.start_time
        e = build.finish_time
        elapsed = (e-s)
        t = round_down_to(s, interval)
        result[t] += elapsed / 3600.0
    return dict(result)


if __name__ == '__main__':
    import reportor.db
    db = reportor.db.db_from_config('scheduler_db')

    end = time.time()
    start = end - 7*86400

    report = {
            'report_start': time.time(), # When we started generating the report
            'data_start': start,
            'data_end': end,
            }

    builds = get_builds(db, start, end)
    count = count_time(builds)
    report['report_end'] = time.time() # When we finished generating the report
    report['data'] = sorted(count.items())
    print json.dumps(report)
