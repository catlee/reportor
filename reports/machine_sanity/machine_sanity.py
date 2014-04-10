#!/usr/bin/env python2

from collections import defaultdict
import logging
from os import devnull, path
import shutil
from subprocess import check_call
import sys
from tempfile import mkdtemp

from boto.ec2 import connect_to_region
from furl import furl
import requests

# These are imports from https://hg.mozilla.org/build/cloud-tools.
# If the full couldtools library didn't depend on so many things we don't need
# we'd just install it instead.
DEFAULT_REGIONS = ['us-east-1', 'us-west-2']

def get_aws_connection(region):
    """Connect to an EC2 region. Caches connection objects"""
    return connect_to_region(region)

def is_beanstalk_instance(i):
    return i.tags.get("elasticbeanstalk:environment-name") is not None


def get_all_instances(conn):
    res = conn.get_all_instances()
    instances = []
    if res:
        instances = reduce(lambda a, b: a + b, [r.instances for r in res])
    # Skip instances managed by Elastic Beanstalk
    return [i for i in instances if not is_beanstalk_instance(i)]

# End of cloudtools imports

INSTANCE_TYPES_WE_CARE_ABOUT = ('bld-linux64', 'try-linux64', 'tst-linux32',
                                'tst-linux64', 'tst-win64', 'servo-linux64',
                                'buildbot-master')


log = logging.getLogger(__name__)

INHOUSE_NETWORKS = [
    "build.scl1.mozilla.com",
    "winbuild.scl1.mozilla.com",
    "build.releng.scl3.mozilla.com",
    "mobile.releng.scl3.mozilla.com",
    "srv.releng.scl3.mozilla.com",
    "tegra.releng.scl3.mozilla.com",
    "test.releng.scl3.mozilla.com",
    "try.releng.scl3.mozilla.com",
    "winbuild.releng.scl3.mozilla.com",
    "wintest.releng.scl3.mozilla.com",
    "wintry.releng.scl3.mozilla.com",
    "p1.releng.scl1.mozilla.com",
    "p2.releng.scl1.mozilla.com",
    "p3.releng.scl1.mozilla.com",
    "p4.releng.scl1.mozilla.com",
    "p5.releng.scl1.mozilla.com",
    "p6.releng.scl1.mozilla.com",
    "p7.releng.scl1.mozilla.com",
    "p8.releng.scl1.mozilla.com",
    "p9.releng.scl1.mozilla.com",
    "p10.releng.scl1.mozilla.com",
    "p127.releng.scl1.mozilla.com",
]

SKIP_PATTERNS = ["puppet", "pdu", "install", "ref", "admin", "casper",
                 "partner", "foopy", "slaveapi", "webhost", "buildlb",
                 "packager", "jenkins", "nagios", "relay", "imaging",
                 "dev", "signing", "cruncher", "bm-remote", "pxe",
                 "-mini-", "seamicro-test", "aws-manager",
                 # buildbot-master81 is special because it's a scheduler
                 # master only, and thus not in slavealloc
                 "buildbot-master81"]

REQUIRED_DNS_RECORDS = ["A", "PTR", "CNAME"]

# Magic number in inventory's system_status field that means "decommissioned"
DECOMM_STATUS = 6

def matches_skip_pattern(name):
    for pat in SKIP_PATTERNS:
        if pat in name:
            return True
    return False

def should_skip(name, i):
    if matches_skip_pattern(name):
        log.info("Skipping %s because it matches a skip pattern", name)
        return True
    if "FQDN" not in i.tags:
        log.info("Skipping %s because it has no FQDN", name)
        return True
    if "moz-loaned-to" in i.tags:
        log.info("Skipping %s because it is a loaner", name)
        return True
    if "moz-type" not in i.tags:
        log.info("Skipping %s because it has no moz-type tag", name)
        return True
    if hasattr(i, "state") and i.state == "pending":
        log.info("Skipping %s because state is pending", name)
        return True
    if i.tags["moz-type"] not in INSTANCE_TYPES_WE_CARE_ABOUT:
        log.info("Skipping %s because its moz-type (%s) is not one that we care about", name, i.tags["moz-type"])
        return True

    return False


def ptr2ip(ptr):
    ip = ptr.strip(".in-addr.arpa.").split(".")
    ip.reverse()
    return ".".join(ip)

def get_slavealloc_machines(slavealloc):
    log.info("Getting slavealloc machines")
    machines = {}
    url = furl(slavealloc)
    url.path.add("slaves")
    for slave in requests.get(str(url)).json():
        machines[slave["name"]] = slave

    url = furl(slavealloc)
    url.path.add("masters")
    for master in requests.get(str(url)).json():
        machines[master["fqdn"].split(".")[0]] = master

    log.info("Done getting slavealloc machines")
    return machines

def get_buildbot_machines(buildbot_configs):
    log.info("Getting buildbot machines")
    machines = set()
    workdir = mkdtemp()
    bbdir = path.join(workdir, "buildbot-configs")
    try:
        with open(devnull, 'w') as null:
            check_call(["hg", "clone", buildbot_configs, bbdir], stdout=null)

        for dir_ in ("mozilla", "mozilla-tests"):
            # We're looking for all slaves, and staging configs have both
            # the production and staging slaves defined, so sourcing from
            # those is best.
            workdir = path.join(bbdir, dir_)
            shutil.copyfile(path.join(workdir, "staging_config.py"), path.join(workdir, "localconfig.py"))
            sys.path.append(workdir)
            # Because we're re-using the same namespace each time through this
            # loop, we need to reload some modules. Even though we're only
            # directly accessing "config", it depends on localconfig (which is staging_config),
            # which depends on production_config. If we don't reload each of
            # these, they'll end up being the same as the previous iteration
            # of the loop.
            import production_config
            reload(production_config)
            import localconfig
            reload(localconfig)
            import config
            reload(config)
            for _, slaves in config.SLAVES.iteritems():
                # Slave lists can be lists, or dicts. We need to handle both.
                if hasattr(slaves, "keys"):
                    machines.update(slaves.keys())
                else:
                    machines.update(slaves)
            sys.path.remove(workdir)
        return machines
    finally:
        shutil.rmtree(workdir)

def get_all_aws_instances(regions=DEFAULT_REGIONS):
    for region in regions:
        conn = get_aws_connection(region)
        log.info("Getting AWS instances")
        for i in get_all_instances(conn):
            # get_all_instances only gives instances that exist right now.
            # Spot instances won't exist until they're needed, so we can't
            # look for them this way. They're handled in the subsequent loop.
            if i.spot_instance_request_id:
                continue

            if "Name" in i.tags:
                name = i.tags["Name"]
            else:
                name = i.id

            if not should_skip(name, i):
                yield name, i
        log.info("Done getting AWS instances")

        log.info("Getting AWS Spot instances")
        for n in conn.get_all_network_interfaces():
            # Non-spot network interfaces will be returned as well, we only
            # want to look at spot though.
            name = n.tags.get("Name")
            if not name or "spot" not in name:
                continue

            if not should_skip(name, n):
                yield name, n

        log.info("Done getting AWS Spot instances")


def get_all_inhouse_machines(inventory):
    log.info("Getting inhouse machines")
    url = furl(inventory)
    url.path.add("bulk_action/export")
    for n in INHOUSE_NETWORKS:
        url.args["q"] = "/\\.%s" % n
        resp = requests.get(str(url))
        for name,info in resp.json()["systems"].iteritems():
            name = name.split(".")[0]
            if info["system_status"] == DECOMM_STATUS:
                log.info("Skipping %s because it has been decommissioned", name)
                continue
            if matches_skip_pattern(name):
                log.info("Skipping %s because it matches a skip pattern", name)
                continue
            yield name,info
    log.info("Done getting inhouse machines")


def get_inventory_dns(machine, inventory):
    url = furl(inventory)
    url.path.add("core/search/search_dns_text")
    url.args.add("search", "/^%s\\." % machine)
    resp = requests.get(str(url))
    results = defaultdict(list)
    for line in resp.json()["text_response"].splitlines():
        if "SREG" in line:
            if "in-addr" in line:
                type_ = "PTR"
            else:
                type_ = "A"
            _, src, _, _, _, target = line.split()
        else:
            for t in REQUIRED_DNS_RECORDS:
                if " %s " % t in line:
                    break
            else:
                continue
            _, src, _, _, type_, target = line.split()
        results[type_].append((src, target))
    return results

def verify_machine(name, machine_fqdn, machine_ip, inventory):
    log.info("Verifying %s", name)
    # * look in slavealloc
    # ** should we check for enabled vs. disabled?
    # * search for inventory, complain if len(results) != 1
    # * look at dns

    # If a machine is a master or slave, we need should check to make sure
    # it's listed in slavealloc.
    if name not in slavealloc_machines:
        log.debug("%s: missing from slavealloc", name)
        missing_from_slavealloc.add(name)

    if "master" not in name and name not in buildbot_machines:
        log.debug("%s: missing from buildbot", name)
        missing_from_buildbot.add(name)

    records = get_inventory_dns(name, inventory)
    for type_ in REQUIRED_DNS_RECORDS:
        if len(records[type_]) != 1:
            msg = "%s: wrong number of %s records (found %d, expected 1)" % (name, type_, len(records[type_]))
            log.debug(msg)
            incorrect_dns[type_].append(msg)
            if len(records[type_]) == 0:
                continue
        record = records[type_][0]

        if type_ == "A":
            dns_fqdn = record[0].rstrip(".")
            dns_ip = record[1]
            if dns_fqdn != machine_fqdn:
                msg = "%s: mismatched FQDN on A record (%s vs. %s)" % (name, dns_fqdn, machine_fqdn)
                log.debug(msg)
                incorrect_dns["A"].append(msg)
            if dns_ip != machine_ip:
                msg = "%s: mismatched IP on A record (%s vs. %s)" % (name, dns_ip, machine_ip)
                log.debug(msg)
                incorrect_dns["A"].append(msg)
        elif type_ == "CNAME":
            cname = "%s.build.mozilla.org." % name
            dns_fqdn = record[1].rstrip(".")
            if record[0] != cname:
                msg = "%s: mismatched CNAME record (%s vs. %s)" % (name, record[0], cname)
                log.debug(msg)
                incorrect_dns["CNAME"].append(msg)
            if dns_fqdn != machine_fqdn:
                msg = "%s: mismatched FQDN on CNAME record (%s vs. %s)" % (name, dns_fqdn, machine_fqdn)
                log.debug(msg)
                incorrect_dns["CNAME"].append(msg)
        elif type_ == "PTR":
            dns_ip = ptr2ip(record[0])
            dns_fqdn = record[1].rstrip(".")
            if dns_ip != machine_ip:
                msg = "%s: mismatched IP on PTR record (%s vs. %s)" % (name, dns_ip, machine_ip)
                log.debug(msg)
                incorrect_dns["PTR"].append(msg)
            if dns_fqdn != machine_fqdn:
                msg = "%s: mismatched FQDN on PTR record (%s vs. %s)" % (name, dns_fqdn, machine_fqdn)
                log.debug(msg)
                incorrect_dns["PTR"].append(msg)
    log.info("Done verifying %s", name)


if __name__ == "__main__":
    from reportor.config import load_config

    config = load_config()

    logging.basicConfig(format="%(message)s", level=logging.ERROR)

    slavealloc = config.get("slavealloc", "api")
    buildbot_configs = config.get("repos", "buildbot_configs")
    inventory = config.get("inventory", "api")

    slavealloc_machines = get_slavealloc_machines(slavealloc)
    buildbot_machines = get_buildbot_machines(buildbot_configs)

    missing_from_slavealloc = set()
    missing_from_buildbot = set()
    incorrect_dns = defaultdict(list)
    cant_verify = list()
    aws_machines = set()
    inhouse_machines = set()

    for name, details in get_all_aws_instances():
        try:
            aws_machines.add(name)
            verify_machine(name, details.tags["FQDN"], details.private_ip_address, inventory)
        except:
            log.error("Error verifying %s", name, exc_info=True)

    for name, details in get_all_inhouse_machines(inventory):
        try:
            inhouse_machines.add(name)
            if not details.get("staticreg_set", {}).get("nic0", {}).get("ip_str"):
                cant_verify.append("%s is missing IP information" % name)
                continue
            verify_machine(name, details["hostname"], details["staticreg_set"]["nic0"]["ip_str"], inventory)
        except:
            log.error("Error verifying %s", name, exc_info=True)


    # Write the report!
    print "Machines in AWS/Inventory but not in Slavealloc:"
    print "************************************************"
    for err in sorted(missing_from_slavealloc):
        print "%s" % err
    print "\n"

    print "Machines in AWS/Inventory but not in Buildbot configs:"
    print "******************************************************"
    for err in sorted(missing_from_buildbot):
        print "%s" % err
    print "\n"

    print "Machines in Slavealloc but not in AWS or inventory:"
    print "***************************************************"
    for m in sorted(slavealloc_machines):
        if m not in aws_machines and m not in inhouse_machines:
            print "%s" % m
    print "\n"

    print "Machines in Buildbot configs but not in AWS or inventory:"
    print "*********************************************************"
    for m in sorted(buildbot_machines):
        if m not in aws_machines and m not in inhouse_machines:
            print "%s" % m
    print "\n"

    for type_ in incorrect_dns:
        print "Machines with errors in their %s DNS records:" % type_
        print "***********************************************"
        for err in incorrect_dns[type_]:
            print "%s" % err
        print "\n"
