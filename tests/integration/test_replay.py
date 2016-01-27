import os.path
import base64
import json

from .base import IntegrationBaseTestCase

from beancmd import replay
from beancmd import migrate


class ReplayTestCase(IntegrationBaseTestCase):
    def test_basic_replay(self):
        log_file_path = os.path.join(self.wd, 'log')

        with open(log_file_path, 'w') as f:
            f.write(json.dumps({
                'tube': 'tube1',
                'job_data': base64.b64encode(b'1').decode('ascii'),
                'pri': 1,
                'delay': 0,
                'ttr': 120
            }) + '\n')

        parser = replay.setup_parser()
        args = parser.parse_args([
            '-H', self.bs1.host, '-p', str(self.bs1.port),
            log_file_path,
        ])
        replay.run(args)

        assert self.bs1.status()['current-jobs-ready'] == 1
        assert self.bs1.client.list_tubes() == ['default', 'tube1']

    def test_migrate_and_replay(self):
        # create a bunch of jobs
        self.bs1.client.use('tube1')
        for i in range(200):
            self.bs1.client.put_job(str(i))

        log_file_path = os.path.join(self.wd, 'log')

        # migrate them into bs2, keeping a log
        assert self.bs1.client.stats()['current-jobs-ready'] == 200
        parser = migrate.setup_parser()
        args = parser.parse_args([
            '-sh', self.bs1.host, '-sp', str(self.bs1.port),
            '-dh', self.bs2.host, '-dp', str(self.bs2.port),
            '-q', '-l', log_file_path,
            'tube1'
        ])
        migrate.run(args)
        assert self.bs1.client.stats()['current-jobs-ready'] == 0
        assert self.bs2.client.stats()['current-jobs-ready'] == 200

        # replay the log back into bs1
        parser = replay.setup_parser()
        args = parser.parse_args([
            '-H', self.bs1.host, '-p', str(self.bs1.port),
            log_file_path,
        ])
        replay.run(args)
        assert self.bs1.client.stats()['current-jobs-ready'] == 200
