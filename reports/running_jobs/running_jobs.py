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
            (builds.finish_time <= :end OR builds.finish_time IS NULL)
        """)
    return db.execute(q, start=start, end=end)


def round_down_to(n, r):
    """Round n down to the nearest multiple of r"""
    return int(int(n/r) * r)


def count_builds(builds, interval=3600):
    """
    Counts the number of builds running per interval
    Returns a dictionary of interval times (rounded to the nearest interval)
    mapped to number of jobs active in that time.
    """
    result = defaultdict(int)
    for build in builds:
        # Convert datetimes to seconds
        s = build.start_time
        e = int(build.finish_time or time.time())
        for t in range(s, e, interval):
            # Round t down to nearest interval
            t = round_down_to(t, interval)
            result[t] += 1
        # If our end time falls into a new interval, add that too
        e = round_down_to(e, interval)
        if e != t:
            result[e] += 1
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
    count = count_builds(builds)
    report['report_end'] = time.time() # When we finished generating the report
    report['data'] = sorted(count.items())
    print json.dumps(report)
