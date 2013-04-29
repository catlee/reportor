#!/usr/bin/env python
import time
import json
from datetime import datetime
import functools
import collections
import logging
log = logging.getLogger(__name__)

import requests
import sqlalchemy as sa
import reportor.db

_branch_cache = None

def get_branch(build):
    branch = build.branch
    if branch is None or branch == 'None':
        return None

    global _branch_cache
    if _branch_cache is None:
        url = "http://hg.mozilla.org/build/tools/raw-file/5aaa1bc1053e/buildfarm/maintenance/production-branches.json"
        log.info("Downloading %s", url)
        _branch_cache = requests.get(url).json()

    for b in _branch_cache.keys():
        if b in branch:
            return b


_hidden_cache = {}
def get_hidden_builders(branch):
    if branch in _hidden_cache:
        return _hidden_cache[branch]
    url = "https://tbpl.mozilla.org/php/getBuilders.php?branch={0}".format(branch)
    log.info("Getting %s", url)
    data = requests.get(url).json()
    retval = set(b['name'] for b in data if b['hidden'])
    _hidden_cache[branch] = retval
    return retval


def td2s(td):
    return td.days * 86400 + td.seconds + td.microseconds/1000000.0


def is_hidden(build):
    branch = get_branch(build)
    if branch is None:
        return True
    hidden = get_hidden_builders(branch)
    return build.builder_name in hidden


def get_builds(db, starttime, endtime):
    q = sa.text("""
        SELECT
            builds.id, builders.name AS builder_name, builds.starttime, builds.endtime, builds.result, sourcestamps.branch, sourcestamps.revision
        FROM
            builds, builders, sourcestamps
        WHERE
            builds.builder_id = builders.id AND
            builds.source_id = sourcestamps.id AND
            builds.starttime >= :starttime AND
            builds.endtime < :endtime
        ORDER BY
            builds.starttime ASC
            """)
    return db.execute(q, starttime=datetime.utcfromtimestamp(starttime), endtime=datetime.utcfromtimestamp(endtime))


zerodict = functools.partial(collections.defaultdict, int)

reportdict = functools.partial(collections.defaultdict, lambda: {'sum': 0.0, 'builders': zerodict()})

def main():
    logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
    logging.getLogger("requests").setLevel(logging.WARN)

    now = time.time()
    start = now - 7*86400
    end = now
    db = reportor.db.db_from_config('status_db')

    # Mapping of branch name to {'sum': total, 'builders': {buildername: builder_time}} dicst
    total_hidden_time = reportdict()
    failed_hidden_time = reportdict()
    for build in get_builds(db, start, end):
        if not build.endtime or not build.starttime:
            continue

        if 'fuzzer' in build.builder_name:
            continue
        if 'l10n' in build.builder_name:
            continue

        #print get_branch(build), build.branch, build.builder_name
        #continue

        if not is_hidden(build):
            continue
        branch = get_branch(build)
        #if branch is None:
            #print branch, build.branch, build.builder_name

        e = td2s(build.endtime - build.starttime)

        total_hidden_time[branch]['sum'] += e
        total_hidden_time[branch]['builders'][build.builder_name] += e
        if build.result != 0:
            failed_hidden_time[branch]['sum'] += e
            failed_hidden_time[branch]['builders'][build.builder_name] += e

    report = {
            'start': start,
            'end': end,
            'report_run': now,
            'report_elapsed': time.time() - now,
            'total_hidden_time': total_hidden_time,
            'failed_hidden_time': failed_hidden_time,
            }
    print json.dumps(report)

if __name__ == "__main__":
    main()
