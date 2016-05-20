#!/usr/bin/env python

import re
import psutil
import yaml
import logging
import smtplib
import platform

from email.mime.text import MIMEText
from multiprocessing.pool import ThreadPool
from subprocess import Popen, PIPE, CalledProcessError
from apscheduler.scheduler import Scheduler


pool = None
COUNT = 4
WAIT = 5

ping_count_re = re.compile('(\d+) received')


log = logging.getLogger()
logging.basicConfig()


def bytes2human(n):
    # http://code.activestate.com/recipes/578019
    # >>> bytes2human(10000)
    # '9.8K'
    # >>> bytes2human(100001221)
    # '95.4M'
    symbols = ('K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
    prefix = {}
    for i, s in enumerate(symbols):
        prefix[s] = 1 << (i + 1) * 10
    for s in reversed(symbols):
        if n >= prefix[s]:
            value = float(n) / prefix[s]
            return '%.1f%s' % (value, s)
    return "%sB" % n


def ping(hosts):
    global pool
    try:
        return fping(hosts)
    except OSError:
        failed_list = []
        if pool is None:
            pool = ThreadPool(20)
        futures = []
        for host in hosts:
            if host:
                futures.append(pool.apply_async(_ping, (host,)))
        for f in futures:
            host, up = f.get()
            if not up:
                failed_list.append(host)
        return failed_list


def _ping(host):
    try:
        with open('/dev/null', 'w') as devnull:
            out = Popen(['ping', '-c', str(COUNT), '-w', str(WAIT), host], stdout=PIPE, stderr=devnull)
            out, err = out.communicate()
            match = ping_count_re.search(out)
            if match:
                count = match.group(1)
                if count != '0':
                    return host, True
            return host, False
    except CalledProcessError:
        return host, False


def fping(hosts):
    process = Popen(['fpingX', '-u'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = process.communicate('\n'.join(hosts))
    return [h for h in out.split('\n') if h]


def get_running_agents():
    desigs = []
    for p in psutil.process_iter():
        cmdline = p.cmdline()
        if len(cmdline) > 1:
            if 'port_agent' in cmdline[1]:
                desigs.append(cmdline[-1])
    return desigs


def missing_agents(expected_desigs):
    found_desigs = get_running_agents()
    return set(expected_desigs) - set(found_desigs)


def check_disk(directory, minfree=1e10, maxpercent=90):
    usage = psutil.disk_usage(directory)
    if usage.free < minfree or usage.percent > maxpercent:
        out = [
            '',
            'Disk threshold exceeded (%s):' % directory,
            '  Free: %s' % bytes2human(usage.free),
            '  PercentAvail: %s' % usage.percent,
            ''
        ]
        return '\n'.join(out)


def generate_fail_message(header, fail_dict):
    out = ['', header]
    for refdes in sorted(fail_dict):
        ip = fail_dict[refdes]
        out.append('  %-30s: %s' % (refdes, ip))
    out.append('')
    return '\n'.join(out)


def generate_ping_fail_message(ip_list, agent_dict):
    header = 'Unable to ping the following instruments:'
    return generate_fail_message(header, {k: v for k, v in agent_dict.items() if v in ip_list})


def generate_not_running_message(agent_list, agent_dict):
    header = 'The following expected port agents not running:'
    return generate_fail_message(header, {k: v for k, v in agent_dict.items() if k in agent_list})


def check(port_agents, directory, addresses):
    ping_fail = ping(port_agents.values())
    run_fail = missing_agents(port_agents.keys())
    disk_fail = check_disk(directory)
    log.info('ping results: %r', ping_fail)
    log.info('pacheck results: %r', run_fail)
    log.info('disk_check results: %r', disk_fail)

    email_contents = []

    if ping_fail:
        email_contents.append(generate_ping_fail_message(ping_fail, port_agents))

    if run_fail:
        email_contents.append(generate_not_running_message(run_fail, port_agents))

    if disk_fail:
        email_contents.append(disk_fail)

    if email_contents:
        email('\n'.join(email_contents), addresses)


def email(contents, addresses):
    noreply = 'noreply@ooi.rutgers.edu'  # config
    notify_list = addresses
    notify_subject = 'OOI STATUS CHANGE NOTIFICATION: %s' % platform.node()
    notifier = EmailNotifier('localhost')
    notifier.send_status(noreply, notify_list, notify_subject, contents)


class EmailNotifier(object):
    def __init__(self, server):
        self.conn = smtplib.SMTP(server)

    def send_status(self, sender, receivers, subject, contents):

        for receiver in receivers:
            message = MIMEText(contents)
            message['Subject'] = subject
            message['From'] = sender
            message['To'] = receiver

            self.conn.sendmail(sender, [receiver], message.as_string())


if __name__ == '__main__':
    import sys
    import time

    config = yaml.load(open(sys.argv[1]))
    agents = config.get('port_agents')
    directory = config.get('directory', '.')
    addresses = config.get('addresses', [])

    sched = Scheduler()
    sched.start()
    sched.add_cron_job(check, minute='*/10', args=(agents, directory, addresses))

    while True:
        time.sleep(1)
