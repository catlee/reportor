#!/usr/bin/env python
from datetime import datetime, timedelta
import requests
from reportor.utils import dt2ts, td2s
from collections import defaultdict

TREESTATUS_URL = "https://treestatus.mozilla.org/{tree}/logs"

TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


def load_treestatus(tree, all=False):
    params = {}
    if all:
        params['all'] = '1'
    r = requests.get(TREESTATUS_URL.format(tree=tree), params=params).json()
    return r


def parse_time(t):
    return datetime.strptime(t, TIME_FORMAT)


def get_stats(events, start, end):
    last = None
    # Figure out the stats for the previous 24 hours
    stats = defaultdict(float)
    for event in events:
        e_time = parse_time(event['when'])
        # This is before our window started, continue along, but reset the time
        # of the event to the start of the window so we can get use it to
        # calculate our duration later
        if e_time < start:
            last = event.copy()
            last['when'] = start.strftime(TIME_FORMAT)
            continue

        done = False
        if e_time > end:
            # Cap this event to the end of our range, and then exit
            e_time = end
            done = True

        last_state = last['action']
        elapsed = td2s(e_time - parse_time(last['when']))
        assert elapsed >= 0
        stats[last_state] += elapsed
        last = event
        if done:
            break

    return stats


def main():
    import argparse

    import reportor.graphite

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--tree", dest="tree", required=True, help="which tree to fetch")
    parser.add_argument("--catch-up", dest="catch_up", action="store_true", help="re-submit all historical data")

    args = parser.parse_args()

    events = load_treestatus(args.tree, all=args.catch_up)
    # Sort by `when`
    events.sort(key=lambda e: e['when'])

    # Add an event for "now"
    now = events[-1].copy()
    now['when'] = datetime.utcnow().strftime(TIME_FORMAT)
    events.append(now)

    graphite = reportor.graphite.graphite_from_config()

    # For the past week, figure out the stats for the previous 24-hour period
    # in 15 minute increments!
    now = datetime.utcnow()
    # Round it down to even 15 minute boundaries
    now = now.replace(second=0, microsecond=0, minute=now.minute - (now.minute % 15))

    start = parse_time(events[0]['when'])
    # Round up to even 15 minute boundary
    start = start.replace(second=0, microsecond=0, minute=start.minute + (15 - start.minute % 15))
    end = start + timedelta(days=1)
    while end < now:
        stats = get_stats(events, start, end)
        assert sum(stats.values()) == 86400
        for k, v in stats.items():
            state = k.replace(" ", "_")
            print end, state, v
            if graphite:
                graphite.submit(
                    "uptime.{0}.{1}".format(args.tree, state),
                    v,
                    dt2ts(end),
                )

        start += timedelta(seconds=60 * 15)
        end = start + timedelta(days=1)

    exit()

if __name__ == '__main__':
    main()
