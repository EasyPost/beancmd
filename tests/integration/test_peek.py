from .base import IntegrationBaseTestCase

import mock
from six.moves import StringIO
import json

from beancmd import peek


class PeekTestCase(IntegrationBaseTestCase):
    def setUp(self):
        super(PeekTestCase, self).setUp()
        self.bs1.client.use('some-tube')
        for i in range(10):
            self.bs1.client.put_job('some-tube {0}'.format(i))
        for i in range(10):
            self.bs1.client.put_job('some-tube d{0}'.format(i), delay=100)
        self.bs1.client.use('other-tube')
        for i in range(10):
            self.bs1.client.put_job('other-tube {0}'.format(i))
        assert self.bs1.status()['current-jobs-ready'] == 20
        assert self.bs1.status()['current-jobs-delayed'] == 10

    def test_peek_ready(self):
        parser = peek.setup_parser()
        args = parser.parse_args([
            '-H', self.bs1.host, '-p', str(self.bs1.port),
            '-m', 'ready', 'some-tube',
        ])
        with mock.patch('sys.stdout', StringIO()) as mock_stdout:
            peek.run(args)

        mock_stdout.seek(0, 0)
        written = json.loads(mock_stdout.getvalue())
        assert written == {'job_id': mock.ANY, 'data': 'some-tube 0', 'stats': mock.ANY}

    def test_peek_delayed(self):
        parser = peek.setup_parser()
        args = parser.parse_args([
            '-H', self.bs1.host, '-p', str(self.bs1.port),
            '-m', 'delayed', 'some-tube',
        ])
        with mock.patch('sys.stdout', StringIO()) as mock_stdout:
            peek.run(args)

        mock_stdout.seek(0, 0)
        written = json.loads(mock_stdout.getvalue())
        assert written == {'job_id': mock.ANY, 'data': 'some-tube d0', 'stats': mock.ANY}
