#!/usr/local/bin/python
from datetime import datetime, timedelta
import sqlalchemy as sa
import simplejson as json
import functools
import time
import urllib
import re

import reportor.db

def get_builds(db, start, end):
    #builders.name LIKE "%nightly%" AND
    q = sa.text("""
            SELECT builders.name, builds.* FROM
                builds, builders
            WHERE
                builds.result IN (0,1) AND
                builds.builder_id = builders.id AND
                builders.name LIKE "%mozilla-central%" AND
                builders.name NOT LIKE "%l10n%" AND
                builds.starttime >= :start AND
                builds.endtime < :end
            ORDER BY
                builds.starttime ASC
            """)
    return db.execute(q, start=datetime.utcfromtimestamp(start), end=datetime.utcfromtimestamp(end))


def td2s(td):
    return td.days * 86400 + td.seconds + td.microseconds/1000000.0


def avg(l):
    return sum(l) / float(len(l))


def nth_percentile(l, n=50):
    i = int(len(l) * (n / 100.0))
    return sorted(l)[i]


if __name__ == "__main__":
    from collections import defaultdict
    t = time.time()
    status_db = reportor.db.db_from_config('status_db')
    end = time.time()
    start = end - 180*86400
    builds = get_builds(status_db, start, end)

    times = defaultdict(list)

    for b in builds:
        times[b.name].append((int(b.starttime.strftime("%s")), int(td2s(b.endtime - b.starttime))))

    report = {
            "times": times,
            "report_start": t,
            "report_run": time.time() - t,
            "start": start,
            "end": end,
            }
    print json.dumps(report)
