"""
Handle reportor manifests

e.g.

---
report1:
    when: hourly
    command: [python,report1.py]
    maxtime: 10

report2:
    when: hourly
    command: [python,report2.py]
    requires: [report1]
    locks: [statusdb]
    cwd: report2

report3:
    when: hourly
    command: python report2.py
    requires: [report1]
    locks: [statusdb]
    cwd: report2
    copy_files:
        - flot/
"""
import os
import time
import logging
import subprocess
import glob
import shutil
import calendar
from datetime import datetime

import yaml

log = logging.getLogger(__name__)


def copyfile(src, dst):
    """
    Copies src to dst if src is newer than dst
    """
    if not os.path.exists(dst) or os.path.getmtime(src) > os.path.getmtime(dst):
        log.info("Copying %s to %s", src, dst)
        shutil.copyfile(src, dst)
        shutil.copymode(src, dst)
        shutil.copystat(src, dst)


class ReportRun:
    stdout = None
    stderr = None
    proc = None
    start_time = None
    end_time = None

    def __init__(self, name, config, basedir, now):
        self.name = name
        self.config = config
        self.cwd = config.get('cwd', name)
        self.basedir = basedir
        self.now = now
        env = os.environ.copy()
        self.output_dir = os.path.join(basedir, name)
        env.update({
            'OUTPUT_DIR': self.output_dir,
            'REPORTOR_NOW': str(calendar.timegm(now.utctimetuple())),
        })
        self.env = env
        self.stdout_path = os.path.join(self.output_dir, config.get('stdout', 'output.txt'))
        self.stderr_path = os.path.join(self.output_dir, config.get('stderr', 'logs/output.log'))

    def copy_files(self):
        # Copy files specified
        for pat in self.config.get('copy_files', []):
            if not isinstance(pat, basestring) and len(pat) == 2:
                src, dst_path = pat
            else:
                src, dst_path = pat, self.output_dir

            # Expand globs; files are relative to cwd
            for f in glob.glob(os.path.join(self.cwd, src)):
                if os.path.isdir(dst_path):
                    dst = os.path.join(dst_path, os.path.basename(f))
                    copyfile(f, dst)
                else:
                    print "uh oh"

    def start(self):
        self.start_time = time.time()

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        if 'command' not in self.config:
            self.end_time = time.time()
            return

        stdout_dir = os.path.dirname(self.stdout_path)
        stderr_dir = os.path.dirname(self.stderr_path)
        if not os.path.exists(stdout_dir):
            os.makedirs(stdout_dir)
        if not os.path.exists(stderr_dir):
            os.makedirs(stderr_dir)
        self.stdout = open(self.stdout_path, 'wb')
        self.stderr = open(self.stderr_path, 'wb')
        devnull = open(os.devnull, 'rw')
        try:
            log.debug("%s: command: %s", self.name, self.config['command'])
            log.debug("%s: cwd: %s", self.name, self.cwd)
            self.proc = subprocess.Popen(self.config['command'], shell=True, cwd=self.cwd,
                                         env=self.env, stdout=self.stdout,
                                         stderr=self.stderr, stdin=devnull)
        except OSError, e:
            log.debug("%s: failed to start", self.name, exc_info=True)
            self.stdout.close()
            self.stderr.write(str(e))
            self.stderr.close()
            self.end_time = time.time()

    @property
    def alive(self):
        if self.proc is None or self.proc.poll() is not None:
            return False
        else:
            return True

    def wait(self):
        if self.proc:
            self.proc.wait()
            self.end_time = time.time()
        self.copy_files()

    def kill(self):
        if self.proc:
            self.proc.kill()


def parse_manifest(m, whens):
    """
    Returns a parsed version of the manfiest
    """
    # TODO: some validation
    m = yaml.safe_load(m)
    for name, config in m.items():
        if config['when'] not in whens:
            del m[name]
    return m


def run_manifest(m, basedir, now):
    to_run = m.items()
    running = []
    finished = set()
    held_locks = set()
    while to_run or running:
        for name, config in to_run[:]:
            # Check to see if our upstreams are done
            if 'requires' in config:
                waiting = set(config['requires']) - finished
                if waiting:
                    log.debug("%s is still waiting for report %s", name, waiting)
                    continue
            # Check to see if locks are being held
            if 'locks' in config:
                waiting = set(config['locks']) & held_locks
                if waiting:
                    log.debug("%s is still waiting for lock %s", name, waiting)
                    continue

            # All set!
            # Let's run it!
            log.info("%s: starting", name)
            proc = ReportRun(name, config, basedir, now)
            proc.start()
            held_locks |= set(config.get('locks', []))
            running.append((name, config, proc))
            to_run.remove((name, config))

        # Wait for stuff to happen
        time.sleep(0.1)

        # Check our running stuff
        for name, config, proc in running[:]:
            if not proc.alive:
                proc.wait()
                log.info("%s finished (%is elapsed)", name, proc.end_time - proc.start_time)
                finished.add(name)
                held_locks -= set(config.get('locks', []))
                running.remove((name, config, proc))
            else:
                if time.time() - proc.start_time > config.get('maxtime', 3600):
                    log.info("killing %s; it's taking too long", name)
                    proc.kill()


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.set_defaults(
        log_level=logging.INFO,
    )
    parser.add_argument("-v", "--verbose", action="store_const", const=logging.DEBUG, dest="log_level")
    parser.add_argument("-q", "--quiet", action="store_const", const=logging.WARN, dest="log_level")
    parser.add_argument("-o", "--output-dir", dest="output_dir", required=True)
    parser.add_argument("-m", "--manifest", dest="manifest", required=True)
    parser.add_argument("-d", "--date", dest="date", type=int, help="date to use, in epoch time")
    parser.add_argument("-l", "--logfile", dest="logfile")
    parser.add_argument("-s", "--symlink", dest="symlink")
    parser.add_argument(dest='when', nargs='+')

    options = parser.parse_args()

    # Set umask so our files are readable by everyone
    os.umask(0o022)

    # TODO: add global locking to prevent running on top of ourselves?
    # TODO: create index for all reports run in output_dir?
    if options.date:
        now = datetime.utcfromtimestamp(options.date)
    else:
        now = datetime.utcnow()
    output_dir = now.strftime(options.output_dir)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if not options.logfile:
        logging.basicConfig(level=options.log_level, format="%(asctime)s - %(message)s")
    else:
        logfile = os.path.join(output_dir, options.logfile)
        logging.basicConfig(level=options.log_level, format="%(asctime)s - %(message)s", filename=logfile)

    m = parse_manifest(open(options.manifest), options.when)
    log.debug("manifest: %s", m)
    run_manifest(m, output_dir, now)

    if options.symlink:
        # Add a symlink from 
        log.debug("%s -> %s", output_dir, options.symlink)
        try:
            if os.path.exists(options.symlink):
                os.unlink(options.symlink)
            os.symlink(os.path.abspath(output_dir), options.symlink)
        except OSError:
            log.error("Couldn't update symlink %s", options.symlink, exc_info=True)


if __name__ == '__main__':
    main()
