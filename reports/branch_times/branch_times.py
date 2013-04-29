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

scheduler_db = reportor.db.db_from_config('scheduler_db')

q = sa.text("""
        SELECT * FROM
            buildrequests, builds
        WHERE
            buildrequests.id = builds.brid AND
            buildrequests.buildername LIKE :buildername AND
            builds.start_time > :starttime AND
            builds.finish_time IS NOT NULL
        """)

builds = scheduler_db.execute(q, buildername = '%mozilla-central%', starttime = time.time() - 21*86400)

print builds.rowcount, "builds"

ignore = ["pgo", "nightly", "valgrind", "shark", "code coverage", "xulrunner", "hg bundle", "blocklist update", "l10n", "dxr"]

builder_times = {}
for build in builds:
    if any(i in build.buildername for i in ignore):
        continue
    builder_times.setdefault(build.buildername, []).append(build.finish_time - build.start_time)

s = 0.0

builder_times = [(avg(times), b) for b, times in builder_times.items()]
builder_times.sort(key=lambda x:x[1])
for a, builder in builder_times:
    s += a
    print builder, a

print len(builder_times), "builders"
print s/3600.0, "average time per push (hours)"
