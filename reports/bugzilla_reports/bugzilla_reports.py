#!/usr/bin/env python
import time
import logging
log = logging.getLogger(__name__)

import requests
from bs4 import BeautifulSoup


def fixed_last_week():
    params = [
        ("chfield", "bug_status"),
        ("query_format", "advanced"),
        #("chfieldfrom", "2014-09-30"),
        #("chfieldfrom", "2014-10-01"),
        ("chfieldfrom", "-1w"),
        ("chfieldto", "Now"),
        ("chfieldvalue", "RESOLVED"),
        ("bug_status", "RESOLVED"),
        ("bug_status", "VERIFIED"),
        ("bug_status", "CLOSED"),
        ("product", "Release Engineering"),
        ("include_fields", "id,status,summary"),
    ]

    r = requests.get("https://bugzilla.mozilla.org/rest/bug", params=params)
    return len(r.json()['bugs'])


def get_activity(username):
    params = [
        ("who", username),
        ("id", "user_activity.html"),
        ("action", "run"),
        ("from", "-7d"),
        ("sort", "when"),
    ]
    u = "https://bugzilla.mozilla.org/page.cgi"
    r = requests.get(u, params=params)

    soup = BeautifulSoup(r.text)

    report = soup.find(id='report')
    if not report:
        return 0
    links = report.find_all("a", class_="bz_bug_link")
    bugs = set()
    for link in links:
        bug = link['href']
        # Strip off fragment
        bug = bug.split("#")[0]
        bugs.add(bug)
    return len(bugs)


def main():
    logging.basicConfig(level=logging.DEBUG)
    import reportor.graphite
    g = reportor.graphite.graphite_from_config()
    if g:
        log.info("Submitting to %s", g)

    now = time.time()
    num_fixed = fixed_last_week()
    print "%i bugs fixed last week" % num_fixed
    if g:
        g.submit("bugs.fixed", num_fixed, now)

    users = [
        'bhearsum@mozilla.com',
        'bugspam.Callek@gmail.com',
        'catlee@mozilla.com',
        'coop@mozilla.com',
        'hwine@mozilla.com',
        'jlund@mozilla.com',
        'kmoir@mozilla.com',
        'mgervasini@mozilla.com',
        'mshal@mozilla.com',
        'nthomas@mozilla.com',
        'pmoore@mozilla.com',
        'rail@mozilla.com',
        'sbruno@mozilla.com',
        'winter2718@gmail.com',
    ]
    for u in users:
        n = get_activity(u)
        #shortname = u.split("@")[0]
        u = u.replace(".", "-")
        print u, n
        if g:
            g.submit("bugs.active.%s" % u, n, now)


if __name__ == '__main__':
    main()
