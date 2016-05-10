import mock

from .base import IntegrationBaseTestCase

from beancmd import pause


class PauseTestCase(IntegrationBaseTestCase):
    def test_pause_runnable(self):
        self.bs1.client.use('some-tube')
        self.bs1.client.put_job('foo')
        parser = pause.setup_parser()
        args = parser.parse_args([
            '-H', self.bs1.host, '-p', str(self.bs1.port),
            '-d', '7200',
            '-P',
            'some-tube'
        ])
        with mock.patch('sys.stderr.write') as mock_stderr:
            with mock.patch('sys.stdout.write') as mock_stdout:
                pause.run(args)
        assert mock_stderr.mock_calls == []
        assert mock_stdout.mock_calls == [
            mock.call('Pausing some-tube for 7200s'),
            mock.call('\n')
        ]
        assert self.bs1.client.stats_tube('some-tube')['pause-time-left'] > 7198

    def test_pause_unpause(self):
        self.bs1.client.use('some-tube')
        self.bs1.client.put_job('foo')
        parser = pause.setup_parser()
        args = parser.parse_args([
            '-H', self.bs1.host, '-p', str(self.bs1.port),
            '-d', '7200',
            '-P',
            'some-tube'
        ])
        with mock.patch('sys.stderr.write') as mock_stderr:
            with mock.patch('sys.stdout.write') as mock_stdout:
                pause.run(args)
        assert mock_stderr.mock_calls == []
        assert mock_stdout.mock_calls == [
            mock.call('Pausing some-tube for 7200s'),
            mock.call('\n')
        ]
        assert self.bs1.client.stats_tube('some-tube')['pause-time-left'] > 7198
        parser = pause.setup_parser()
        args = parser.parse_args([
            '-H', self.bs1.host, '-p', str(self.bs1.port),
            '-U',
            'some-tube'
        ])
        with mock.patch('sys.stderr.write') as mock_stderr:
            with mock.patch('sys.stdout.write') as mock_stdout:
                pause.run(args)
        assert mock_stderr.mock_calls == []
        assert mock_stdout.mock_calls == [
            mock.call('Unpausing some-tube'),
            mock.call('\n')
        ]
