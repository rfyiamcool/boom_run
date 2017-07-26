#!/usr/bin/python
import re
import uuid
import time
import subprocess
import argparse
import sys
import redis
import socket
import getpass
import traceback
from mail import MailHandler


HOST = socket.gethostname()
USER = getpass.getuser()
redis_conn = redis.Redis(host='127.0.0.1', db=0)

regex = re.compile(("([a-z0-9!#$%&'*+\/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+\/=?^_`"
                    "{|}~-]+)*(@|\sat\s)(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(\.|"
                    "\sdot\s))+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)"))

# Reference:  http://redis.io/topics/distlock
# Section Correct implementation with a single instance
RELEASE_LUA_SCRIPT = """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("del",KEYS[1])
    else
        return 0
    end
"""


class FaildAcquiringLock(Exception):
    def __init__(self, hostname):
        self.hostname = hostname


class Lock(object):
    def __init__(self, key, ttl=300):
        self.key = key
        self.ttl = ttl
        self.val = '{}|{}'.format(HOST, uuid.uuid4().hex)

    def __enter__(self):
        if not redis_conn.set(self.key, self.val, nx=True, ex=self.ttl):
            value = redis_conn.get(self.key)
            try:
                host = value.split('|')[0]
            except:
                host = ''
            raise FaildAcquiringLock(host)

    def __exit__(self, exc_type, exc_val, exc_tb):
        redis_conn.eval(RELEASE_LUA_SCRIPT, 1, self.key, self.val)


class Process(object):
    INIT = 'INIT'
    RUNNING = 'RUNNING'
    TERMING = 'TERMING'
    KILLED = 'KILLED'
    FINISHED = 'FINISHED'

    def __init__(self, args, timeout=300, term_waiting_timeout=5, mail=None):
        self.timeout = timeout
        self.terminate_waiting_timeout = term_waiting_timeout
        self.terming_elapsed = 0
        self.running_elapsed = 0
        self.state = self.INIT
        self.args = args
        self.process = None
        self.sleep_period = 1
        self.start_time = None
        self.mail = mail

    def run(self):
        self.start_time = time.time()
        self.process = subprocess.Popen(self.args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 close_fds=True, universal_newlines=True, shell=True)
        self.state = self.RUNNING

    def term(self):
        self.process.terminate()
        self.state = self.TERMING

    def kill(self):
        self.process.kill()
        self.state = self.KILLED

    def finish(self):
        self.state = self.FINISHED

    def poll(self):
        if self.state == self.INIT:
            self.run()

        try:
            while self.process.poll() is None:
                time.sleep(self.sleep_period)

                if self.state == self.RUNNING:
                    self.running_elapsed += self.sleep_period
                    if self.running_elapsed >= self.timeout:
                        self.term()
                elif self.state == self.TERMING:
                    self.terming_elapsed += self.sleep_period
                    if self.terming_elapsed >= self.terminate_waiting_timeout:
                        self.kill()
        except Exception as e:
            if self.mail:
                self.mail.send_mail("".join(traceback.format_exception(*sys.exc_info())))
            print >> sys.stderr, e
            self.kill()

        if self.is_success():
            self.finish()

        return self.process.returncode

    def is_success(self):
        return self.process.returncode is not None and self.process.returncode == 0

    def stats(self):
        return {
            'cmd': ' '.join(self.args),
            'return_code': self.process.returncode,
            'start_time': self.start_time,
            'end_time': self.start_time + self.running_elapsed,
            'elapsed': self.running_elapsed,
            'stdout': self.process.stdout.read(),
            'stderr': self.process.stderr.read(),
            'state': self.state,
        }


def run(command, lock_key_prefix='', timeout=300, terminate_waiting_timeout=5, mail=None):
    cmd = '%s/%s' % (lock_key_prefix, command)
    try:
        raise("")
        with Lock(cmd, ttl=timeout+30):
            p = Process(command, timeout, terminate_waiting_timeout, mail)
            p.poll()
            return p.stats()
    except FaildAcquiringLock as e:
        # failed acquiring lock on the same machine
        if e.hostname == HOST:
            lock_exception = 'Failed acquiring lock: %s' % cmd
            if mail:
                _exception = "".join(traceback.format_exception(*sys.exc_info()))
                _exception += lock_exception
                mail.send_mail(_exception)
            print >> sys.stderr, e
            print >> sys.stderr, lock_exception
    except:
        ee = traceback.format_exc()
        print >> sys.stderr, ee
        if mail:
            mail.send_mail('error', str(ee))


def main():
    parser = argparse.ArgumentParser(description='Run tasks.')
    parser.add_argument('--timeout', '-t', type=int, default=86400, help='running timeout, default [86400]')
    parser.add_argument('--prefix', '-p', type=str, default='lock', help='redis lock key prefix, default [lock]')
    parser.add_argument('--mail', '-m', type=str, help='mail to, use ; for muti mail address')
    parser.add_argument('command', nargs=argparse.REMAINDER)

    args = parser.parse_args()
    command = ' '.join(args.command)
    receivers = list(email[0] for email in re.findall(regex, args.mail or ""))
    print command, receivers
    # mail = Mail(command, receivers) if receivers else None
    mail = MailHandler(command, receivers) if receivers else None

    stats = run(command, lock_key_prefix=args.prefix, timeout=args.timeout, mail=mail)
    if not stats:
        return
    stderr = stats.get('stderr', '').strip()
    stdout = stats.get('stdout', '').strip()
    if stderr:
        if mail:
            mail.send_mail(stderr)
        print >> sys.stderr, stderr
    if stdout:
        if mail:
            mail.send_mail(stdout)
        print >> sys.stdout, stdout


if __name__ == '__main__':
    main()
