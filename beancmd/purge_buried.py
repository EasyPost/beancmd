from __future__ import print_function
import argparse

from . import util

import pystalk


def setup_parser(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', default='localhost', help='Host of beanstalk server (default %(default)s)')
    parser.add_argument('-p', '--port', default=11300, type=int, help='Port of beanstalk server (default %(default)s)')
    parser.add_argument('-y', '--yes', action='store_true', help='Do not prompt when deleting things')
    parser.add_argument('tubes', nargs='*', help='Tubes to drop all buried items from (if not passed, does them all!)')
    return parser


def run(args):
    client = pystalk.BeanstalkClient(args.host, args.port)

    tubes = util.get_tubes(client, args.tubes)

    if not args.yes:
        util.prompt_yesno('Are you sure you want to purge all buried jobs from tubes {0} (y/N)? '.format(
            ', '.join(sorted(tubes))
        ))

    client.watch('unused-fake-tube')
    for tube in tubes:
        print('Purging {0}'.format(tube))
        client.watch(tube)
        client.use(tube)
        stats = client.stats_tube(tube)
        buried_jobs = stats['current-jobs-buried']
        for job in util.progress(client.peek_buried_iter(), total=buried_jobs, unit='jobs'):
            client.delete_job(job.job_id)
        client.ignore(tube)
    return 0
