import base64
import os.path
import json

import mock
import pytest

from .base import IntegrationBaseTestCase

import simple_beanstalk

from beancmd import migrate


class MigrateTestCase(IntegrationBaseTestCase):
    def test_migrate_runnable(self):
        self.bs1.client.use('some-tube')
        for i in range(100):
            self.bs1.client.put_job(str(i))

        assert self.bs1.status()['current-jobs-ready'] == 100
        assert self.bs2.status()['current-jobs-ready'] == 0

        parser = migrate.setup_parser()
        args = parser.parse_args([
            '-sh', self.bs1.host, '-sp', str(self.bs1.port),
            '-dh', self.bs2.host, '-dp', str(self.bs2.port),
            '-q',
            'some-tube'
        ])
        migrate.run(args)
        assert self.bs1.status()['current-jobs-ready'] == 0
        assert self.bs2.status()['current-jobs-ready'] == 100

    def test_migrate_with_limit(self):
        self.bs1.client.use('some-tube')
        for i in range(100):
            self.bs1.client.put_job(str(i))
        assert self.bs1.status()['current-jobs-ready'] == 100
        assert self.bs2.status()['current-jobs-ready'] == 0

        parser = migrate.setup_parser()
        args = parser.parse_args([
            '-sh', self.bs1.host, '-sp', str(self.bs1.port),
            '-dh', self.bs2.host, '-dp', str(self.bs2.port),
            '-q', '-n', '50',
            'some-tube'
        ])
        migrate.run(args)
        assert self.bs1.status()['current-jobs-ready'] == 50
        assert self.bs2.status()['current-jobs-ready'] == 50

    def test_migrate_with_delayed_jobs(self):
        self.bs1.client.use('some-tube')
        for i in range(100):
            self.bs1.client.put_job(str(i))
        for i in range(100):
            self.bs1.client.put_job(str(i), delay=1000)

        assert self.bs1.status()['current-jobs-ready'] == 100
        assert self.bs1.status()['current-jobs-delayed'] == 100
        assert self.bs2.status()['current-jobs-ready'] == 0
        assert self.bs2.status()['current-jobs-delayed'] == 0

        parser = migrate.setup_parser()
        args = parser.parse_args([
            '-sh', self.bs1.host, '-sp', str(self.bs1.port),
            '-dh', self.bs2.host, '-dp', str(self.bs2.port),
            '-q',
            'some-tube'
        ])
        migrate.run(args)

        assert self.bs1.status()['current-jobs-ready'] == 0
        assert self.bs1.status()['current-jobs-delayed'] == 0
        assert self.bs2.status()['current-jobs-ready'] == 100
        assert self.bs2.status()['current-jobs-delayed'] == 100

    def test_migrate_does_not_break_already_reserved_jobs(self):
        self.bs1.client.use('some-tube')
        for i in range(100):
            self.bs1.client.put_job(str(i), delay=0)
        self.bs1.client.watch('some-tube')
        job = self.bs1.client.reserve_job()

        assert self.bs1.status()['current-jobs-ready'] == 99
        assert self.bs1.status()['current-jobs-reserved'] == 1
        assert self.bs2.status()['current-jobs-ready'] == 0

        parser = migrate.setup_parser()
        args = parser.parse_args([
            '-sh', self.bs1.host, '-sp', str(self.bs1.port),
            '-dh', self.bs2.host, '-dp', str(self.bs2.port),
            '-q',
            'some-tube'
        ])
        migrate.run(args)

        assert self.bs1.status()['current-jobs-ready'] == 0
        assert self.bs1.status()['current-jobs-reserved'] == 1
        assert self.bs2.status()['current-jobs-ready'] == 99

        self.bs1.client.release_job(job.job_id)
        assert self.bs1.status()['current-jobs-ready'] == 1
        assert self.bs2.status()['current-jobs-ready'] == 99

    def test_migrates_buried_jobs_back_to_runnable(self):
        self.bs1.client.use('some-tube')
        self.bs1.client.put_job('1', delay=0)
        self.bs1.client.put_job('2', delay=0)
        self.bs1.client.watch('some-tube')
        job = self.bs1.client.reserve_job()
        self.bs1.client.bury_job(job.job_id)

        assert self.bs1.status()['current-jobs-ready'] == 1
        assert self.bs1.status()['current-jobs-buried'] == 1
        assert self.bs1.status()['current-jobs-reserved'] == 0

        parser = migrate.setup_parser()
        args = parser.parse_args([
            '-sh', self.bs1.host, '-sp', str(self.bs1.port),
            '-dh', self.bs2.host, '-dp', str(self.bs2.port),
            '-q',
            'some-tube'
        ])
        migrate.run(args)

        assert self.bs1.status()['current-jobs-ready'] == 0
        assert self.bs1.status()['current-jobs-buried'] == 0
        assert self.bs1.status()['current-jobs-reserved'] == 0

        assert self.bs2.status()['current-jobs-ready'] == 2
        assert self.bs2.status()['current-jobs-buried'] == 0
        assert self.bs2.status()['current-jobs-reserved'] == 0

    def test_ignores_buried_jobs_with_B(self):
        self.bs1.client.put_job('1', delay=0)
        self.bs1.client.put_job('2', delay=0)
        job = self.bs1.client.reserve_job()
        self.bs1.client.bury_job(job.job_id)

        assert self.bs1.status()['current-jobs-ready'] == 1
        assert self.bs1.status()['current-jobs-buried'] == 1
        assert self.bs1.status()['current-jobs-reserved'] == 0

        parser = migrate.setup_parser()
        args = parser.parse_args([
            '-sh', self.bs1.host, '-sp', str(self.bs1.port),
            '-dh', self.bs2.host, '-dp', str(self.bs2.port),
            '-q', '-B',
            'default'
        ])
        migrate.run(args)

        assert self.bs1.status()['current-jobs-ready'] == 0
        assert self.bs1.status()['current-jobs-buried'] == 1
        assert self.bs1.status()['current-jobs-reserved'] == 0

        assert self.bs2.status()['current-jobs-ready'] == 1
        assert self.bs2.status()['current-jobs-buried'] == 0
        assert self.bs2.status()['current-jobs-reserved'] == 0

    def test_migrate_multiple_tubes(self):
        self.generate_jobs(self.bs1.client, 'some-tube', 2)
        self.generate_jobs(self.bs1.client, 'some-other-tube', 2)
        self.generate_jobs(self.bs1.client, 'ignored-tube', 1)

        assert self.bs1.status()['current-jobs-ready'] == 5

        parser = migrate.setup_parser()
        args = parser.parse_args([
            '-sh', self.bs1.host, '-sp', str(self.bs1.port),
            '-dh', self.bs2.host, '-dp', str(self.bs2.port),
            '-q',
            'some-tube', 'some-other-tube'
        ])
        migrate.run(args)

        assert self.bs1.status()['current-jobs-ready'] == 1
        assert self.bs2.status()['current-jobs-ready'] == 4

        assert self.bs1.client.stats_tube('ignored-tube')['current-jobs-ready'] == 1

    def test_migrate_with_log(self):
        job_data = b'lots of data might contain invalid unicode, like \xad'
        b64_data = base64.b64encode(job_data).decode('ascii')
        self.generate_jobs(self.bs1.client, 'some-tube', ready=2, data=job_data)

        assert self.bs1.status()['current-jobs-ready'] == 2

        log_file = os.path.join(self.wd, 'migration_log')
        parser = migrate.setup_parser()
        args = parser.parse_args([
            '-sh', self.bs1.host, '-sp', str(self.bs1.port),
            '-dh', self.bs2.host, '-dp', str(self.bs2.port),
            '-q', '-l', log_file,
            'some-tube',
        ])
        migrate.run(args)

        assert self.bs1.status()['current-jobs-ready'] == 0

        with open(log_file, 'r') as log_f:
            lines = [json.loads(f.strip()) for f in log_f.readlines()]
            assert lines == [
                {'tube': 'some-tube', 'delay': 0, 'ttr': 120, 'job_data': b64_data, 'pri': 65535},
                {'tube': 'some-tube', 'delay': 0, 'ttr': 120, 'job_data': b64_data, 'pri': 65535},
            ]

    def test_migrate_rename_tube(self):
        self.generate_jobs(self.bs1.client, 'some-tube', ready=1, delayed=1)

        assert self.bs1.client.stats_tube('some-tube')['current-jobs-ready'] == 1
        assert self.bs1.client.stats_tube('some-tube')['current-jobs-delayed'] == 1

        parser = migrate.setup_parser()
        args = parser.parse_args([
            '-sh', self.bs1.host, '-sp', str(self.bs1.port),
            '-dh', self.bs2.host, '-dp', str(self.bs2.port),
            '-T', 'some-other-tube', '-q',
            'some-tube',
        ])
        migrate.run(args)

        assert self.bs1.client.stats_tube('some-tube')['current-jobs-ready'] == 0
        assert self.bs1.client.stats_tube('some-tube')['current-jobs-delayed'] == 0
        with pytest.raises(simple_beanstalk.BeanstalkError):
            assert self.bs2.client.stats_tube('some-tube')['current-jobs-ready'] == 0
        with pytest.raises(simple_beanstalk.BeanstalkError):
            assert self.bs2.client.stats_tube('some-tube')['current-jobs-delayed'] == 0
        assert self.bs2.client.stats_tube('some-other-tube')['current-jobs-ready'] == 1
        assert self.bs2.client.stats_tube('some-other-tube')['current-jobs-delayed'] == 1

    def test_verbose_output(self):
        self.generate_jobs(self.bs1.client, 'some-tube', ready=1, delayed=1, buried=1)

        assert self.bs1.client.stats_tube('some-tube')['current-jobs-ready'] == 1
        assert self.bs1.client.stats_tube('some-tube')['current-jobs-delayed'] == 1
        assert self.bs1.client.stats_tube('some-tube')['current-jobs-buried'] == 1

        parser = migrate.setup_parser()
        args = parser.parse_args([
            '-sh', self.bs1.host, '-sp', str(self.bs1.port),
            '-dh', self.bs2.host, '-dp', str(self.bs2.port),
            'some-tube',
        ])

        with mock.patch('sys.stderr.write') as mock_stderr:
            with mock.patch('sys.stdout.write') as mock_stdout:
                migrate.run(args)
        # stdout should never be written to
        assert mock_stdout.mock_calls == []
        mock_stderr.assert_has_calls([
            mock.call('Migrating ~1 READY jobs on tube some-tube'),
            mock.call('Migrating ~1 DELAYED jobs on tube some-tube'),
            mock.call('Migrating ~1 BURIED jobs on tube some-tube'),
        ], any_order=True)
