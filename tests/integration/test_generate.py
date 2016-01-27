from .base import IntegrationBaseTestCase

from beancmd import generate


class GenerateTestCase(IntegrationBaseTestCase):
    SEED = 0

    def test_generate_basic(self):
        parser = generate.setup_parser()
        args = parser.parse_args([
            '-H', self.bs1.host, '-p', str(self.bs1.port), '-S', str(self.SEED),
            str(100), 'some-tube'
        ])
        assert self.bs1.status()['total-jobs'] == 0
        generate.run(args)
        assert self.bs1.status()['total-jobs'] == 100
        assert self.bs1.status()['current-jobs-ready'] == 100

    def test_generate_delay(self):
        parser = generate.setup_parser()
        args = parser.parse_args([
            '-H', self.bs1.host, '-p', str(self.bs1.port), '-S', str(self.SEED),
            '-d', '60',
            str(100), 'some-tube'
        ])
        assert self.bs1.status()['current-jobs-delayed'] == 0
        generate.run(args)
        assert self.bs1.status()['current-jobs-delayed'] == 100
        self.bs1.client.use('some-tube')
        job = self.bs1.client.peek_delayed()
        job_status = self.bs1.client.stats_job(job.job_id)
        assert job_status['delay'] == 60
        assert job_status['time-left'] < 60

    def test_generate_binary(self):
        parser = generate.setup_parser()
        args = parser.parse_args([
            '-H', self.bs1.host, '-p', str(self.bs1.port), '-S', str(self.SEED),
            '-B',
            str(1), 'some-tube'
        ])
        generate.run(args)
        assert self.bs1.status()['current-jobs-ready'] == 1
