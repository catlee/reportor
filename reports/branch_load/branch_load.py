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
        SELECT builds.brid, buildrequests.buildername, sourcestamps.branch, builds.start_time, builds.finish_time FROM
            buildrequests, builds, buildsets, sourcestamps
        WHERE
            buildrequests.id = builds.brid AND
            builds.start_time > :starttime AND
            builds.finish_time IS NOT NULL AND
            buildsets.id = buildrequests.buildsetid AND
            buildsets.sourcestampid = sourcestamps.id
        """)

starttime = time.time()-14*86400
builds = scheduler_db.execute(q, starttime=starttime)

branches = ['mozilla-central', 'mozilla-inbound', 'try-comm', 'try', 'mozilla-aurora',
        'mozilla-esr10', 'comm-aurora', 'fx-team', 'elm', 'mozilla-beta', 'mozilla-release',
        'comm-beta', 'l10n-central', 'services-central', 'comm-esr10', 'comm-release',
        'profiling', 'ionmonkey', 'alder', 'build-system', 'comm-central', 'addon-sdk', 'ux', 'birch', 'oak', 'cedar',
        'mozilla-b2g18', 'mozilla-b2g18_v1_0_1', 'graphics']

def get_branch(build):
    if '/' in build.branch:
        return build.branch.split('/')[-1]
    for b in branches:
        if b in build.branch:
            return b
    #return build.branch.split("-")[0]
    #print branchname
    return 'other'

branch_times = {}
breqs = set()
for build in builds:
    if build.branch is None:
        #print build.buildername
        continue
    #if not ('unittest' in build.branch or 'talos' in build.branch):
        #continue
    branch = get_branch(build)
    if branch not in branch_times:
        branch_times[branch] = 0.0

    # Don't count the same build multiple times
    if (build.brid, build.start_time) in breqs:
        continue
    breqs.add((build.brid, build.start_time))
    branch_times[branch] += (build.finish_time - build.start_time)

total = float(sum(branch_times.values()))
times = branch_times.items()
times.sort(key=lambda x: x[1])

print "%% time spent in build/test pool since %s" % (datetime.fromtimestamp(starttime))
for branch, time in reversed(times):
    print "%.2f%% %s" % (100.0*time/total, branch)
