#!/usr/bin/env python
import requests
import datetime
import json
import time
import logging
import re

# Get the pushlog for hte
def get_pushes(branch, startdate, enddate, full=False):
    baseurl = "http://hg.mozilla.org/{branch}/json-pushes".format(branch=branch)
    params = {"startdate": startdate, "enddate": enddate}
    if full:
        params['full'] = '1'
    r = requests.get(baseurl, params=params)
    pushes = r.json()
    # Throw away the push ids...they're not useful
    return pushes.values()


def get_job_info(branch, revision):
    #baseurl = "https://hg.mozilla.org/{branch}/json-pushes".format(branch=branch)
    baseurl = "http://buildapi01.build.scl1.mozilla.com/buildapi/self-serve/{branch}/rev/{revision}".format(branch=branch, revision=revision)
    for i in range(10):
        try:
            r = requests.get(baseurl, params={'format': 'json'})
            r.json()
            break
        except Exception:
            time.sleep(5)
            continue
    else:
        raise ValueError("Couldn't load data from %s" % baseurl)

    t = 0
    n = 0

    if "status" in r.json() and r.json()['status'] == 'FAILED':
        return 0, 0

    for build in r.json():
        try:
            if not build.get("starttime") or not build.get("endtime"):
                continue
            elapsed = build['endtime'] - build['starttime']
            assert elapsed >= 0
            t += elapsed
            n += 1
        except:
            logging.exception("couldn't handle the build! %s", build)
            raise

    return t, n


if __name__ == '__main__':
    lastweek = datetime.date.today() - datetime.timedelta(days=7)
    today = datetime.date.today()
    lastweek = lastweek.strftime("%Y-%m-%d")
    today = today.strftime("%Y-%m-%d")
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(message)s")

    logging.debug("loading pushes...")
    # Use full=True to get changeset descriptions and files
    pushes = get_pushes("try", lastweek, today, full=True)

    users = set(p['user'] for p in pushes)
    results = {'fromdate': lastweek, 'todate': today, 'scores': [], 'generated': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    t = time.time()
    for u in users:
        logging.debug("processing %s", u)
        user_pushes = [p for p in pushes if p['user'] == u]

        total_try_time = 0
        total_jobs = 0
        revisions = []
        for p in user_pushes:
            logging.debug("getting times for %s %s", u, p['changesets'][-1]['node'][:12])
            # Figure out how much time we spent on this push
            try:
                try_time, num_jobs = get_job_info("try", p['changesets'][-1]['node'][:12])
                try_match = re.search("try:.*$", p['changesets'][-1]['desc'])
                if try_match:
                    try_syntax = try_match.group(0)
                else:
                    try_syntax = ""
                revisions.append( {"hours": try_time/3600.0, "revision": p['changesets'][-1]['node'][:12], "jobs": num_jobs, "try_syntax": try_syntax} )
                total_try_time += try_time
                total_jobs += num_jobs
            except:
                logging.exception("couldn't handle %s!", p)
                raise

        results['scores'].append({"hours": total_try_time/3600.0, "jobs": total_jobs, "user": u, "pushes": revisions})

    e = time.time()
    results['reporttime'] = int(e-t)
    logging.debug("writing results...")
    print json.dumps(results)
