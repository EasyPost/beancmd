from .base import IntegrationBaseTestCase

import itertools

from beancmd import purge_buried


class PurgeBuriedTestCase(IntegrationBaseTestCase):
    def test_purge_buried(self):
        self.bs1.client.use('some-tube')
        self.bs1.client.watch('some-tube')
        for i in range(100):
            self.bs1.client.put_job(str(i))
        for j in itertools.islice(self.bs1.client.reserve_iter(), 50):
            self.bs1.client.bury_job(j.job_id)
        assert self.bs1.status()['current-jobs-ready'] == 50
        assert self.bs1.status()['current-jobs-buried'] == 50
        parser = purge_buried.setup_parser()
        args = parser.parse_args([
            '-H', self.bs1.host, '-p', str(self.bs1.port),
            '-y',
            'some-tube'
        ])
        purge_buried.run(args)
        assert self.bs1.status()['current-jobs-ready'] == 50
        assert self.bs1.status()['current-jobs-buried'] == 0
