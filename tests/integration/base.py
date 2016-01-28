import io
import tempfile
import shutil
import socket
import os
import os.path
import subprocess
import time
import yaml

from unittest import TestCase

import simple_beanstalk


class TestingBeanStalk(object):
    MAX_STARTUP_TIME = 0.5

    def __init__(self, wal_directory):
        if not os.path.exists(wal_directory):
            os.makedirs(wal_directory)
        # get a port to bind to
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('localhost', 0))
        host, port = s.getsockname()
        s.close()
        self.host = host
        self.port = port
        cmdline = ['beanstalkd', '-b', wal_directory, '-l', self.host, '-p', str(self.port)]
        self.p = subprocess.Popen(cmdline)
        start_time = time.time()
        self.client = simple_beanstalk.BeanstalkClient(self.host, self.port)
        while (time.time() - start_time) < self.MAX_STARTUP_TIME:
            try:
                self.status()
                return
            except (socket.error, OSError):
                pass
            time.sleep(self.MAX_STARTUP_TIME / 20.0)
        raise ValueError('beanstalk never started!')

    def stop(self):
        self.p.terminate()
        self.p.wait()

    def status(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
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

    def tearDown(self):
        shutil.rmtree(self.wd)
        self.bs1.stop()
        self.bs2.stop()
