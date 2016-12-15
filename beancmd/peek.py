import argparse
import json

import pystalk

MODES = ('ready', 'delayed', 'buried')


def setup_parser(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', default='localhost', help='Host of beanstalk server (default %(default)s)')
    parser.add_argument('-p', '--port', default=11300, type=int, help='Port of beanstalk server (default %(default)s)')
    parser.add_argument('-m', '--mode', choices=MODES, required=True, help='What to peek (choices {0})'.format(MODES))
    parser.add_argument('tube', type=str, help='Tube to peek at')
    return parser


def run(args):
    client = pystalk.BeanstalkClient(args.host, args.port)
    client.use(args.tube)
    fn = getattr(client, 'peek_{0}'.format(args.mode))
    job = fn()
    print(json.dumps({
        'job_id': job.job_id,
        'stats': client.stats_job(job.job_id),
        'data': job.job_data.decode('utf-8')
    }))
    return 0
