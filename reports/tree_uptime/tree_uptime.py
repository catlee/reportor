#!/usr/bin/env python
from datetime import datetime, timedelta
import requests
from reportor.utils import dt2ts, td2s

TREESTATUS_URL = "https://treestatus.mozilla.org/{tree}/logs"


def load_treestatus(tree, all=False):
    params = {'all': '1'}
    r = requests.get(TREESTATUS_URL.format(tree=tree), params=params).json()
    return r


def parse_time(t):
    return datetime.strptime(t, "%Y-%m-%dT%H:%M:%S")


def crystalball(events):
    """
    Normally to determine event duration you need to keep track of the previous
    event and compare it to the current event.

    crystalball takes an iterable of events and yields the same events, each
    with an added 'elapsed' field which represents how long the event lasted.

    This will yield 1 fewer events than exists in the orignal data, since we
    don't know the duration of the final event.
    """
    last = None
    for e in events:
        if last is None:
            last = e.copy()
            continue
        last['elapsed'] = parse_time(e['when']) - parse_time(last['when'])
        assert td2s(last['elapsed']) >= 0
        yield last
        last = e.copy()


def daybreak(events):
    """
    Takes a list of events and splits up any that cross day boundaries.
    """
    oneday = timedelta(days=1)
    last = None
    for e in events:
        if last:
            last_t = parse_time(last['when'])
            t = parse_time(e['when'])

            d = last_t
            while d.date() != t.date():
                next_d = (d + oneday).replace(hour=0, minute=0, second=0)
                next_e = last.copy()
                next_e['when'] = next_d.strftime("%Y-%m-%dT%H:%M:%S")
                yield next_e
                d = next_d

        yield e
        last = e


def main():
    import argparse
    from collections import defaultdict

    import reportor.graphite

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--tree", dest="tree", required=True, help="which tree to fetch")

    args = parser.parse_args()

    events = load_treestatus(args.tree)
    # Sort by `when`
    events.sort(key=lambda e: e['when'])

    # Add an event for "now"
    now = events[-1].copy()
    now['when'] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    events.append(now)

    # Add up total times per day
    # day -> mapping of state name to elapsed time
    times_per_day = defaultdict(lambda: defaultdict(float))

    for e in crystalball(daybreak(events)):
        #print e['when'], e['action'], e['elapsed'], e['tags']
        t = parse_time(e['when'])
        names = []
        if e['tags']:
            for tag in e['tags']:
                names.append('{action}.{tag}'.format(action=e['action'], tag=tag))
        else:
            names.append(e['action'])

        elapsed = e['elapsed']

        for n in names:
            times_per_day[t.date()][n] += td2s(elapsed)
            assert times_per_day[t.date()][n] >= 0

    graphite = reportor.graphite.graphite_from_config()

    for day in sorted(times_per_day.keys()):
        for state, time in sorted(times_per_day[day].iteritems()):
            day = datetime(day.year, day.month, day.day)
            print day, state, time
            if graphite:
                graphite.submit(
                    "uptime.{0}.{1}".format(args.tree, state),
                    time,
                    dt2ts(day),
                )


if __name__ == '__main__':
    main()
