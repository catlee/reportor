#!/usr/bin/env python
import zipfile
import csv
from datetime import datetime
from cStringIO import StringIO
import tempfile

import pytz

import boto.s3

import logging
log = logging.getLogger(__name__)


def extract_zip(filename):
    zf = zipfile.ZipFile(filename, 'r')
    infos = zf.infolist()
    return zf.read(infos[0])


def parse_csv(data):
    csvfile = StringIO(data)
    reader = csv.DictReader(csvfile)
    return reader


def get_by_account(reader, accountid):
    for row in reader:
        if row['LinkedAccountId'] == accountid:
            yield row


def sort_by_date(rows):
    # Filter out rows without dates
    rows = [row for row in rows if row['UsageEndDate']]
    return sorted(rows, key=lambda row: parse_date(row['UsageEndDate']))


def parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")


def cost_by_date(rows):
    last = None
    time_cost = 0.0
    US_Eastern = pytz.timezone("US/Eastern")
    for row in rows:
        # This time is in US/Eastern (I think). Convert it to UTC.
        t = parse_date(row['UsageEndDate'])
        t = US_Eastern.localize(t)

        cost = float(row['BlendedCost'])

        if 'Recurring Fee' in row['ItemDescription']:
            continue

        if last is None:
            last = t
            time_cost += cost
            continue

        if t != last:
            yield last, time_cost
            last = t
            time_cost = 0.0
        time_cost += cost
    yield last, time_cost


def get_recent_billing_data(region, bucketname, accountnumber, date=None):
    log.debug("connecting to %s", region)
    conn = boto.s3.connect_to_region(region)
    log.debug("getting bucket %s", bucketname)
    bucket = conn.get_bucket(bucketname)
    if date is None:
        date = datetime.today()
    keyname = "{0}-aws-billing-detailed-line-items-{1}.csv.zip".format(accountnumber, date.strftime("%Y-%m"))
    log.info("getting key %s/%s/%s", region, bucketname, keyname)
    key = bucket.get_key(keyname)
    tmpfile = tempfile.TemporaryFile()
    key.get_contents_to_file(tmpfile)
    tmpfile.seek(0)
    return tmpfile


def main():
    import reportor.graphite
    import reportor.config
    from reportor.utils import dt2ts
    import os
    logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
    log.info("fetching data")

    config = reportor.config.load_config()
    billing_region = config.get('billing', 'region')
    billing_bucket = config.get('billing', 'bucketname')
    billing_account = config.get('billing', 'account')
    subaccount = config.get('billing', 'subaccount')
    os.environ['AWS_ACCESS_KEY_ID'] = config.get('billing', 'aws_account_id')
    os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('billing', 'aws_secret_key')
    fp = get_recent_billing_data(billing_region, billing_bucket, billing_account)

    log.info("extracting data")
    data = extract_zip(fp)

    log.info("parsing data")
    rows = parse_csv(data)

    rows = sort_by_date(get_by_account(rows, subaccount))

    graphite = reportor.graphite.graphite_from_config()
    if not graphite:
        print "no graphite!"
    for t, cost in cost_by_date(rows):
        if graphite:
            print t, cost
            graphite.submit(
                "ec2_cost",
                cost,
                dt2ts(t),
            )

if __name__ == '__main__':
    main()
