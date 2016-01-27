from .base import IntegrationBaseTestCase

from beancmd import bury


class BuryTestCase(IntegrationBaseTestCase):
    def test_bury_all_runnable(self):
        self.bs1.client.use('some-tube')
        for i in range(100):
            self.bs1.client.put_job(str(i))
        assert self.bs1.status()['current-jobs-ready'] == 100
        parser = bury.setup_parser()
        args = parser.parse_args([
            '-H', self.bs1.host, '-p', str(self.bs1.port),
            'some-tube'
        ])
        bury.run(args)
        assert self.bs1.status()['current-jobs-ready'] == 0
        assert self.bs1.status()['current-jobs-buried'] == 100
