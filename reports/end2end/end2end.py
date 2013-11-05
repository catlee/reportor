#!/usr/bin/env python
import os
import requests
import datetime
import json
import time
import logging
import sqlalchemy as sa


# Get the pushlog for the branch
def get_pushes(branch, startdate, enddate, full=False):
    baseurl = "http://hg.mozilla.org/{branch}/json-pushes".format(branch=branch)
    params = {"startdate": startdate, "enddate": enddate}
    if full:
        params['full'] = '1'
    r = requests.get(baseurl, params=params)
    pushes = r.json()
    # Throw away the push ids...they're not useful
    return pushes.values()


def get_job_info(db, branch, revision):
    q = sa.text("""
        SELECT
            buildrequests.submitted_at, buildrequests.complete_at,
            buildsets.reason
        FROM buildrequests, buildsets, sourcestamps WHERE
        buildrequests.buildsetid = buildsets.id AND
        buildsets.sourcestampid = sourcestamps.id AND
        sourcestamps.branch LIKE :branch AND
        sourcestamps.revision LIKE :revision""")
    builds = db.execute(q, branch='%{0}%'.format(branch),
                        revision='{0}%'.format(revision))
    return list(builds)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--branch", required=True, dest="branch")

    start_time = datetime.date.today() - datetime.timedelta(days=60)
    today = datetime.date.today()
    start_time = start_time.strftime("%Y-%m-%d")
    today = today.strftime("%Y-%m-%d")
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s - %(message)s")

    import reportor.db
    db = reportor.db.db_from_config('scheduler_db')

    s = time.time()
    options = parser.parse_args()
    logging.debug("loading pushes...")
    pushes = get_pushes(options.branch, start_time, today, full=True)

    results = []

    for p in pushes:
        rev = p['changesets'][-1]['node'][:12]
        if 'DONTBUILD' in p['changesets'][-1]['desc']:
            continue
        all_jobs = get_job_info(db, os.path.basename(options.branch), rev)
        auto_jobs = [j for j in all_jobs if "Self-serve" not in j.reason]

        start = p['date']

        # Filter out jobs that were submitted more than 24 hours after the push
        auto_jobs = [j for j in auto_jobs if j.submitted_at < start + 86400]

        if not auto_jobs:
            # They all got coalesced away?
            continue

        end = max(j.complete_at for j in auto_jobs)
        if end < start:
            # Aaah, weirdness!
            continue
        results.append((rev, start, end))

    e = time.time()
    report = {
        "branch": options.branch,
        "data": results,
        "report_start": s,
        "report_elapsed": e - s,
        "data_start": start_time,
        "data_end": today,
    }
    print json.dumps(report)
