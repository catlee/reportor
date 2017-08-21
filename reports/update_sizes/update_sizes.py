#!/usr/bin/env python
"""
Fetch and report sizes for complete and partial updates, and full installers
For different branches and platforms
"""
import requests
from datetime import datetime, date, timedelta

from reportor.utils import date2ts

AUS_API_ROOT = 'https://aus-api.mozilla.org/api/v1'


def get_size(url):
    response = requests.head(url, allow_redirects=True)
    response.raise_for_status()
    return int(response.headers['content-length'])


def url_exists(url):
    response = requests.head(url, allow_redirects=True)
    return response.status_code == 200


def get_taskId(branch, platform, d):
    # Find the task first
    if platform.startswith('android'):
        product = 'mobile'
    else:
        product = 'firefox'
    return get_nightly_taskId(branch, platform, d, product)


def get_rule_mapping(rule_id):
    rule = requests.get('{}/rules/{}'.format(AUS_API_ROOT, rule_id)).json()
    return rule['mapping']


def get_release_blob(blob_name):
    blob = requests.get('{}/releases/{}'.format(AUS_API_ROOT, blob_name)).json()
    return blob


def get_blobs_by_prefix(blob_prefix):
    blobs = requests.get('{}/releases'.format(AUS_API_ROOT)).json()
    return [r['name'] for r in blobs['releases'] if r['name'].startswith(blob_prefix)]


def format_bouncer_url(url, platform, locale):
    platform = {
        'win32': 'win',
        'macosx64': 'osx',
    }.get(platform, platform)
    url = url.replace('%OS_BOUNCER%', platform)
    url = url.replace('%LOCALE%', locale)
    return url


def get_sizes_from_blob(blob, channel):
    fileUrls = blob['fileUrls'].get(channel, blob['fileUrls']['*'])
    complete_url_b = fileUrls['completes']['*']
    partial_urls_b = []
    for name, url in fileUrls['partials'].items():
        # Ignore beta names
        if channel == 'release' and '.0b' in name:
            continue
        partial_urls_b.append(url)
    retval = {}
    for platform in ('linux64', 'win64', 'win32', 'macosx64'):
        url = format_bouncer_url(complete_url_b, platform, 'en-US')
        size = get_size(url)

        retval[platform] = dict(
            complete_size=size,
            installer_size=None,
            partial_size=None,
        )

        installer_url = url.replace('-complete', '-SSL')
        if url_exists(installer_url):
            retval[platform]['installer_size'] = get_size(installer_url)

        partial_sizes = []
        for url in partial_urls_b:
            url = format_bouncer_url(url, platform, 'en-US')
            size = get_size(url)
            partial_sizes.append(size)
        if partial_sizes:
            retval[platform]['partial_size'] = min(partial_sizes)
    return retval


def get_sizes_from_nightly_blob(blob, channel):
    platforms = [
        ('macosx64', 'Darwin_x86_64-gcc3-u-i386-x86_64'),
        ('linux64', 'Linux_x86_64-gcc3'),
        ('win32', 'WINNT_x86-msvc'),
        ('win64', 'WINNT_x86_64-msvc'),
    ]
    retval = {}
    for platform, aus_platform in platforms:
        complete_url = blob['platforms'][aus_platform]['locales']['en-US']['completes'][0]['fileUrl']
        size = get_size(complete_url)

        retval[platform] = dict(
            complete_size=size,
            partial_size=None,
        )
        if 'partials' not in blob['platforms'][aus_platform]['locales']['en-US']:
            continue
        partial_urls = [p['fileUrl'] for p in blob['platforms'][aus_platform]['locales']['en-US']['partials']]
        partial_sizes = []
        for url in partial_urls:
            size = get_size(url)
            partial_sizes.append(size)
        if partial_sizes:
            retval[platform]['partial_size'] = min(partial_sizes)
    return retval


def get_beta_update_sizes():
    blob_name = get_rule_mapping('firefox-beta')
    blob = get_release_blob(blob_name)
    update_sizes = get_sizes_from_blob(blob, 'beta')
    return update_sizes


def get_release_update_sizes():
    blob_name = get_rule_mapping('firefox-release')
    blob = get_release_blob(blob_name)
    update_sizes = get_sizes_from_blob(blob, 'release')
    return update_sizes


def get_nightly_update_sizes(branch, date):
    blob_prefix = 'Firefox-{branch}-nightly-{date}'.format(date=date, branch=branch)
    blob_name = get_blobs_by_prefix(blob_prefix)[0]
    blob = get_release_blob(blob_name)
    update_sizes = get_sizes_from_nightly_blob(blob, 'nightly')
    return update_sizes


def get_release_sizes(graphite, branches):
    for branch in branches:
        if branch == 'mozilla-beta':
            sizes = get_beta_update_sizes()
        elif branch == 'mozilla-release':
            sizes = get_release_update_sizes()
        else:
            print "Don't know how to handle branch", branch
            continue

        for platform, sizes in sizes.items():
            print platform, sizes
            if sizes['installer_size']:
                graphite.submit('release_sizes.{}.{}.installer'.format(branch, platform), sizes['installer_size'])
            if sizes['complete_size']:
                graphite.submit('release_sizes.{}.{}.complete'.format(branch, platform), sizes['complete_size'])
            if sizes['partial_size']:
                graphite.submit('release_sizes.{}.{}.partial'.format(branch, platform), sizes['partial_size'])


def get_nightly_installer_sizes(branch):
    platforms = ['linux64', 'macosx64', 'win32', 'win64']
    retval = {}
    for p in platforms:
        url = format_bouncer_url(
            'https://download.mozilla.org/?product=firefox-nightly-latest-ssl&os=%OS_BOUNCER%&lang=%LOCALE%',
            p,
            'en-US',
        )
        retval[p] = {'installer_size': get_size(url)}
    return retval


def get_nightly_sizes(graphite, branches):
    oneday = timedelta(days=1)
    today = date.today()

    days = 5

    for branch in branches:
        d = today
        sizes = get_nightly_installer_sizes(branch)
        ts = date2ts(d)
        for platform, sizes in sizes.items():
            print platform, sizes
            if sizes['installer_size']:
                graphite.submit('release_sizes.{}.{}.installer'.format(branch, platform), sizes['installer_size'], ts)

        for i in range(days):
            d = today - i * oneday
            ts = date2ts(d)
            sizes = get_nightly_update_sizes(branch, d.strftime('%Y%m%d'))
            if not sizes:
                continue
            for platform, sizes in sizes.items():
                print platform, sizes
                if sizes['complete_size']:
                    graphite.submit('release_sizes.{}.{}.complete'.format(branch, platform), sizes['complete_size'], ts)
                if sizes['partial_size']:
                    graphite.submit('release_sizes.{}.{}.partial'.format(branch, platform), sizes['partial_size'], ts)

def main():
    import reportor.graphite
    graphite = reportor.graphite.graphite_from_config()

    nightly_branches = ['mozilla-central']
    release_branches = ['mozilla-beta', 'mozilla-release']

    get_release_sizes(graphite, release_branches)
    get_nightly_sizes(graphite, nightly_branches)

if __name__ == '__main__':
    main()
