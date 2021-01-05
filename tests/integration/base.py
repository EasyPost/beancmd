import io
import tempfile
import shutil
import socket
import os
import os.path
import subprocess
import time
import yaml

import mock
from unittest import TestCase

from beancmd import util

import pystalk


class BeanstalkProcessError(Exception):
    def __init__(self, status):
        self.status = status

    def __repr__(self):
        return '{0}({1!r})'.format(self.__class__.__name__, self.status)


class TestingBeanStalk(object):
    MAX_STARTUP_TIME = 1.0

    def __init__(self, wal_directory):
        self.wal_directory = wal_directory

        for i in range(3):
            if not os.path.exists(wal_directory):
                os.makedirs(wal_directory)
            self.p = None

            try:
                self.try_startup()
                return
            except BeanstalkProcessError:
                if self.p is not None:
                    self.p.kill()
                    shutil.rmtree(wal_directory)

    def try_startup(self):
        # get a port to bind to
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('localhost', 0))
        host, port = s.getsockname()
        s.close()
        self.host = host
        self.port = port
        program = os.path.expanduser(os.environ.get('BEANSTALKD_PATH', 'beanstalkd'))
        cmdline = [program, '-b', self.wal_directory, '-l', self.host, '-p', str(self.port)]
        # give beanstalkd enough time to exit if the port is already in use
        time.sleep(0.02)
        self.p = subprocess.Popen(cmdline)
        start_time = time.time()
        self.client = pystalk.BeanstalkClient(self.host, self.port)
        while (time.time() - start_time) < self.MAX_STARTUP_TIME:
            if self.p.poll() is not None:
                exit_status = self.p.returncode
                raise BeanstalkProcessError(exit_status)
            try:
                self.status()
                self.client.watch('default')
                return
            except (socket.error, OSError):
                pass
            time.sleep(self.MAX_STARTUP_TIME / 20.0)
        raise ValueError('beanstalk never started!')

    def stop(self):
        self.p.terminate()
        self.p.wait()

    def status(self):
        s = socket.create_connection((self.host, self.port), timeout=0.25)
        s.sendall(b'stats\r\n')
        top, rest = s.recv(1024).split(b'\r\n', 1)
        status, count = top.split(b' ')
        count = int(count) + 2
        bio = io.BytesIO()
        bytes_read = len(rest)
        bio.write(rest)
        while bytes_read < count:
            message = s.recv(count - bytes_read)
            if not message:
                break
            bio.write(message)
        bio.seek(0, 0)
        return yaml.safe_load(bio)


class IntegrationBaseTestCase(TestCase):
    def setUp(self):
        self.wd = tempfile.mkdtemp()
        self.bs1 = TestingBeanStalk(os.path.join(self.wd, 'bs1'))
        self.bs2 = TestingBeanStalk(os.path.join(self.wd, 'bs2'))
        self.tqdm_patcher = mock.patch.object(util, 'tqdm', None)
        self.tqdm_patcher.start()

    def tearDown(self):
        shutil.rmtree(self.wd)
        self.bs1.stop()
        self.bs2.stop()
        self.tqdm_patcher.stop()

    def generate_jobs(self, client, tube, ready=0, delayed=0, buried=0, data=b'data', pri=65535, ttr=120):
        client.watch('unused-default-tube')
        client.use(tube)
        client.watch(tube)
        for _ in range(ready):
            client.put_job(data, pri=pri, ttr=ttr, delay=0)
        for _ in range(delayed):
            client.put_job(data, pri=pri, ttr=ttr, delay=1000)
        for _ in range(buried):
            client.put_job(data, pri=pri, ttr=ttr, delay=0)
            job_info = client.reserve_job()
            client.bury_job(job_info.job_id)
        client.ignore(tube)
