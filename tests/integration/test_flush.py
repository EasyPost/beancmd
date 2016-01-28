from .base import IntegrationBaseTestCase

import mock

from beancmd import flush
from beancmd import util


class FlushTestCase(IntegrationBaseTestCase):
    def test_flush_runnable(self):
        self.bs1.client.use('some-tube')
        for i in range(100):
            self.bs1.client.put_job(str(i))
        assert self.bs1.status()['current-jobs-ready'] == 100
        parser = flush.setup_parser()
        args = parser.parse_args([
            '-H', self.bs1.host, '-p', str(self.bs1.port),
            '-y',
            'some-tube'
        ])
        flush.run(args)
        assert self.bs1.status()['current-jobs-ready'] == 0
        assert self.bs1.status()['current-jobs-delayed'] == 0
        assert self.bs1.status()['current-jobs-buried'] == 0

    @mock.patch.object(util, 'prompt_yesno', return_value='y')
    def test_prompting(self, mock_prompt):
        self.bs1.client.use('some-tube')
        for i in range(100):
            self.bs1.client.put_job(str(i))
        parser = flush.setup_parser()
        args = parser.parse_args([
            '-H', self.bs1.host, '-p', str(self.bs1.port),
        ])
        flush.run(args)
        mock_prompt.assert_called_once_with('Are you sure you want to flush tubes default, some-tube (y/N)? ')
