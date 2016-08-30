from .base import IntegrationBaseTestCase

import mock
from six.moves import StringIO

from beancmd import stats_tubes


class StatsTubesTestCase(IntegrationBaseTestCase):
    def test_stats_smoke(self):
        self.bs1.client.use('some-tube')
        for i in range(100):
            self.bs1.client.put_job(str(i))
        assert self.bs1.status()['current-jobs-ready'] == 100
        parser = stats_tubes.setup_parser()
        args = parser.parse_args([
            '-H', self.bs1.host, '-p', str(self.bs1.port),
        ])
        with mock.patch('sys.stdout', StringIO()) as mock_stdout:
            stats_tubes.run(args)

        mock_stdout.seek(0, 0)
        written = mock_stdout.getvalue()
        expected = '''-------------------------------------------------------------
|           | reserved  |   ready   |  delayed  |  buried   |
-------------------------------------------------------------
| default   |         0 |         0 |         0 |         0 |
| some-tube |         0 |       100 |         0 |         0 |
-------------------------------------------------------------
'''
        assert written == expected
