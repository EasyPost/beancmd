from .base import IntegrationBaseTestCase

import mock
from mock import call

from beancmd import list_tubes


class ListTubesTestCase(IntegrationBaseTestCase):
    def test_list_tubes(self):
        for tube in ('foo', 'bar', 'baz'):
            self.bs1.client.use(tube)
            self.bs1.client.put_job('job_data')
        parser = list_tubes.setup_parser()
        args = parser.parse_args([
            '-H', self.bs1.host, '-p', str(self.bs1.port),
        ])
        with mock.patch('sys.stdout.write') as mock_sys_stdout_write:
            list_tubes.run(args)

            mock_sys_stdout_write.assert_has_calls([
                call('bar\nbaz\ndefault\nfoo'),
                call('\n')
            ])
