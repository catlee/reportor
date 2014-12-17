#!/usr/bin/env python
"""
Calculate the amount of time we spend doing jobs in various branches
"""
from datetime import datetime
import sqlalchemy as sa
import simplejson as json
import time
import urllib
from collections import defaultdict

import reportor.db
import reportor.graphite


def get_builds(starttime, endtime):
    """
    Get a list of builds between starttime and endtime
    """
    scheduler_db = reportor.db.db_from_config('scheduler_db')
    query = sa.text("""
            SELECT builds.brid, buildrequests.buildername, sourcestamps.branch, sourcestamps.revision, builds.start_time, builds.finish_time FROM
                buildrequests, builds, buildsets, sourcestamps
            WHERE
                buildrequests.id = builds.brid AND
                builds.start_time > :starttime AND
                builds.start_time <= :endtime AND
                builds.finish_time IS NOT NULL AND
                buildsets.id = buildrequests.buildsetid AND
                buildsets.sourcestampid = sourcestamps.id
            """)

    builds = scheduler_db.execute(query, starttime=starttime, endtime=endtime)
    return builds


BRANCHES = None


def get_branches():
    """Get a list of all available branch names"""
    global BRANCHES
    if BRANCHES:
        return BRANCHES
    url = "http://hg.mozilla.org/build/tools/raw-file/default/buildfarm/maintenance/production-branches.json"
    branches = json.load(urllib.urlopen(url))
    BRANCHES = branches.keys()
    return BRANCHES


def get_branch(build):
    """Get the branch for a given build"""
    if '/' in build.branch:
        return build.branch.split('/')[-1]
    for b in get_branches():
        if b in build.branch:
            return b
    return 'other'


def get_times(starttime, endtime):
    """Get the time spent per branch in the given time window"""
    # branch -> time spent in jobs for that branch
    times_by_branch = defaultdict(float)
    # (branch, rev) -> (push time, time spent in jobs for that revision)
    times_by_rev = defaultdict(lambda: [None, 0.0])

    # Make sure we submit 0 for all branches in case there's no data
    for branch in get_branches():
        times_by_branch[branch] = 0.0

    breqs = set()
    for build in get_builds(starttime, endtime):
        if build.branch is None:
            #print build.buildername
            continue
        #if not ('unittest' in build.branch or 'talos' in build.branch):
            #continue
        branch = get_branch(build)

        # Don't count the same build multiple times
        if (build.brid, build.start_time) in breqs:
            continue
        breqs.add((build.brid, build.start_time))
        elapsed = build.finish_time - build.start_time
        times_by_branch[branch] += elapsed

        # TODO: ignore self-serve/nightly/etc. things here?
        rev = build.revision[:12] if build.revision else None
        push_time = times_by_rev[branch, rev][0]
        if push_time is None:
            times_by_rev[branch, rev][0] = build.start_time
        else:
            times_by_rev[branch, rev][0] = min(times_by_rev[branch, rev][0], build.start_time)
        times_by_rev[branch, rev][1] += elapsed

    return times_by_branch, times_by_rev


def report_branch_times(times_by_branch, starttime, endtime):
    total = float(sum(times_by_branch.values()))
    times = times_by_branch.items()
    times.sort(key=lambda x: x[1])

    print "%% time spent in build/test pool from {0} to {1}".format(
        datetime.fromtimestamp(starttime),
        datetime.fromtimestamp(endtime),
    )

    graphite = reportor.graphite.graphite_from_config()

    for branch, branch_time in reversed(times):
        print "%.2f%% %s" % (100.0 * branch_time / total, branch)
        if graphite:
            graphite.submit(
                "branch_time.{0}".format(branch),
                branch_time,
                endtime,
            )


def report_rev_times(times_by_rev):
    # TODO: split out EC2 times here too?
    graphite = reportor.graphite.graphite_from_config()

    for (branch, rev), (rev_starttime, rev_cputime) in times_by_rev.iteritems():
        if graphite:
            graphite.submit(
                "rev_time.{0}".format(branch),
                rev_cputime,
                rev_starttime,
            )


def main():
    catchup = False

    now = time.time()

    if catchup:
        # Re-play 60 days of data
        starttime = now - (60 * 86400)
        endtime = starttime + 86400
        while endtime < now:
            times_by_branch, times_by_rev = get_times(starttime, endtime)
            report_branch_times(times_by_branch, starttime, endtime)
            report_rev_times(times_by_rev)
            starttime += 86400
            endtime += 86400

    starttime = now - 86400
    times_by_branch, times_by_rev = get_times(starttime, now)
    report_branch_times(times_by_branch, starttime, now)
    report_rev_times(times_by_rev)

if __name__ == '__main__':
    main()
