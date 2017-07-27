#!/usr/bin/env python
"""
Fetch and report sizes for complete and partial updates, and full installers
For different branches and platforms
"""

import requests
from datetime import datetime, date, timedelta

from reportor.utils import date2ts


def get_size(url):
    response = requests.head(url, allow_redirects=True)
    response.raise_for_status()
    return int(response.headers['content-length'])


def get_taskId(branch, platform, d):
    # Find the task first
    if platform.startswith('android'):
        product = 'mobile'
    else:
        product = 'firefox'
    return get_nightly_taskId(branch, platform, d, product)


def get_nightly_taskId(branch, platform, d, product):
    d = d.strftime('%Y.%m.%d')
    url = 'https://index.taskcluster.net/v1/task/gecko.v2.{branch}.nightly.{d}.latest.{product}.{platform}-opt'.format(branch=branch, platform=platform, d=d, product=product)
    response = requests.get(url)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    taskId = response.json()['taskId']
    return taskId


def get_release_taskId(branch, platform, d, product):
    d = d.strftime('%Y.%m.%d')
    url = 'https://index.taskcluster.net/v1/namespaces/gecko.v2.{branch}.pushdate.{d}'.format(branch=branch, platform=platform, d=d)
    response = requests.post(url, data='{}', headers={'Content-Type': 'application/json'})
    if response.status_code == 404:
        return None
    response.raise_for_status()
    namespaces = response.json()['namespaces']
    # Pick the latest one
    for namespace in sorted([n['namespace'] for n in namespaces], reverse=True):
        url = 'https://index.taskcluster.net/v1/task/{namespace}.firefox.{platform}-opt'.format(namespace=namespace, platform=platform)
        response = requests.get(url)
        if response.status_code == 404:
            continue
        response.raise_for_status()
        taskId = response.json()['taskId']
        return taskId


def get_artifacts(taskId):
    # List the artifacts
    url = 'https://queue.taskcluster.net/v1/task/{taskId}/artifacts'.format(taskId=taskId)
    artifacts = requests.get(url).json()['artifacts']
    return artifacts


def get_installer_artifact(artifacts):
    for a in artifacts:
        if a['name'].endswith('.installer.exe'):
            return a['name']
        elif a['name'].endswith('linux-x86_64.tar.bz2') or a['name'].endswith('linux-i686.tar.bz2'):
            return a['name']
        elif a['name'].endswith('target.tar.bz2'):
            return a['name']
        elif a['name'].endswith('.mac.dmg'):
            return a['name']
        elif a['name'].endswith('android-arm.apk'):
            return a['name']
        elif a['name'].endswith('android-i386.apk'):
            return a['name']
        elif a['name'].endswith('target.apk'):
            return a['name']


def get_complete_artifact(artifacts):
    for a in artifacts:
        if a['name'].endswith('.complete.mar'):
            return a['name']


def get_buildprops(taskId):
    url = get_artifact_url(taskId, 'public/build/buildbot_properties.json')
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json()

    url = get_artifact_url(taskId, 'public/build/balrog_props.json')
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json()


def get_targetjson(taskId):
    url = get_artifact_url(taskId, 'public/build/target.json')
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json()


def get_artifact_url(taskId, artifact):
    return 'https://queue.taskcluster.net/v1/task/{taskId}/artifacts/{artifact}'.format(taskId=taskId, artifact=artifact)


def get_partial_url(branch, platform, version, buildid, lastbuildid):
    if branch == 'mozilla-central':
        return 'https://s3.amazonaws.com/mozilla-nightly-updates/{branch}/{buildid}/Firefox-{branch}-{version}-{platform}-en-US-{lastbuildid}-{buildid}.partial.mar'.format(branch=branch, platform=platform, buildid=buildid, lastbuildid=lastbuildid, version=version)
    else:
        # http://releases.mozilla.com/pub/firefox/releases/55.0b1/update/win64/en-US/firefox-54.0-55.0b1.partial.mar
        return 'https://releases.mozilla.com/pub/firefox/releases/{version}/update/{platform}/en-US/firefox-{lastversion}-{version}.partial.mar'.format(version=version, platform=platform, lastversion=lastbuildid)


def get_nightly_sizes(branch, p, d, lastbuildid):
    taskId = get_taskId(branch, p, d)
    if not taskId:
        print 'Couldn\'t find task for', branch, p, d
        return
    artifacts = get_artifacts(taskId)
    installer = get_installer_artifact(artifacts)
    if not installer:
        print 'Couldn\'t find installer for', branch, p, taskId, d
        return
    installer_size = get_size(get_artifact_url(taskId, installer))

    complete = get_complete_artifact(artifacts)
    if complete:
        complete_size = get_size(get_artifact_url(taskId, complete))
    else:
        print 'Couldn\'t find complete for', branch, p, taskId, d
        complete_size = None

    buildprops = get_buildprops(taskId)
    targetjson = get_targetjson(taskId)
    if buildprops:
        buildid = buildprops.get('properties', {}).get('buildid')
        version = buildprops.get('properties', {}).get('appVersion')
    elif targetjson:
        buildid = targetjson.get('buildid')
        version = targetjson.get('moz_app_version')
    else:
        print 'Couldn\'t find buildprops for', taskId
        buildid = None
        version = None

    if lastbuildid and buildid and version:
        partial_url = get_partial_url(branch, p, version, lastbuildid, buildid)
        try:
            partial_size = get_size(partial_url)
        except IOError:
            print 'Couldn\'t get partial size for', branch, p, d, partial_url
            partial_size = None
    else:
        partial_size = None


    return dict(
        installer_size=installer_size,
        complete_size=complete_size,
        partial_size=partial_size,
        buildid=buildid)


def main():
    #import reportor.graphite
    #graphite = reportor.graphite.graphite_from_config()

    platforms = ['linux64', 'win32', 'win64', 'macosx64', 'android-api-15', 'android-x86']
    branches = ['mozilla-central', 'mozilla-beta', 'mozilla-release']
    branches = ['mozilla-beta', 'mozilla-release']

    oneday = timedelta(days=1)
    today = date.today()

    days = 5

    for branch in branches:
        if branch == 'mozilla-central':
            for p in platforms:
                d = today
                taskId = None
                lastbuildid = None
                for i in range(days):
                    d = today - i * oneday
                    sizes = get_nightly_sizes(branch, p, d, lastbuildid)
                    if not sizes:
                        continue
                    #graphite.submit('release_sizes.{}.{}.installer'.format(branch, p), sizes['installer_size'], date2ts(d))
                    #if sizes['complete_size']:
                        #graphite.submit('release_sizes.{}.{}.complete'.format(branch, p), sizes['complete_size'], date2ts(d))
                    #if sizes['partial_size']:
                        #graphite.submit('release_sizes.{}.{}.partial'.format(branch, p), sizes['partial_size'], date2ts(d))

                    lastbuildid = sizes['buildid']
                    print d, branch, p, sizes
        else:
            # Figure out something for releases...
            # Partials can be found at e.g. 
            # https://tools.taskcluster.net/index/artifacts/#releases.v1.mozilla-beta.1b954c82dd04faf1926804d89c0d130dc6b9ab93.firefox.51_0b4.build1.partials.51_0b3.win64/releases.v1.mozilla-beta.1b954c82dd04faf1926804d89c0d130dc6b9ab93.firefox.51_0b4.build1.partials.51_0b3.win64.en-US
            # How to figure out the previous versions?

if __name__ == '__main__':
    main()
