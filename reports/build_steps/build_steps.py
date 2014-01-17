#!/usr/local/bin/python
from datetime import datetime, timedelta
import sqlalchemy as sa
import csv
import gzip
import time

import reportor.db

def get_steps(db, starttime, endtime):
    q = sa.text("""
        SELECT
            builds.id as build_id,
            builders.name as builder_name,
            UNIX_TIMESTAMP(builds.starttime) as build_starttime,
            UNIX_TIMESTAMP(builds.endtime) as build_endtime,
            slaves.name as slave_name,
            steps.id as step_id,
            steps.name as step_name,
            steps.description as step_description,
            steps.order as step_order,
            UNIX_TIMESTAMP(steps.starttime) as step_starttime,
            UNIX_TIMESTAMP(steps.endtime) as step_endtime,
            status as step_status
        FROM steps, builds, builders, slaves WHERE
            steps.build_id = builds.id AND
            builds.builder_id = builders.id AND
            builds.slave_id = slaves.id AND
            builds.starttime > :starttime AND
            builds.starttime <= :endtime
        ORDER BY
            builds.id ASC,
            steps.order ASC
        """)
    result = db.execute(q, starttime=starttime, endtime=endtime)
    return result

if __name__ == '__main__':
    status_db = reportor.db.db_from_config('status_db')

    f = gzip.open('steps.csv.gz', 'wb')
    writer = csv.writer(f)

    endtime = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    starttime = endtime - timedelta(days=1)

    print time.asctime(), "Getting steps from %s to %s" % (starttime, endtime)
    steps = get_steps(status_db, starttime, endtime)
    print time.asctime(), "Writing data"
    # Write the header
    step = steps.fetchone()
    writer.writerow(step.keys())
    writer.writerow(step)
    for step in steps:
        writer.writerow(step)
    f.close()
