from .base import IntegrationBaseTestCase

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

    def test_migrate_multiple_tubes(self):
        self.bs1.client.use('some-tube')
        self.bs1.client.put_job('1', delay=0)
        self.bs1.client.put_job('2', delay=0)
        self.bs1.client.use('some-other-tube')
        self.bs1.client.put_job('3', delay=0)
        self.bs1.client.put_job('4', delay=0)
        self.bs1.client.use('ignored-tube')
        self.bs1.client.put_job('5', delay=0)

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
