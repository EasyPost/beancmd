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
