from __future__ import print_function
import argparse

from . import util

import simple_beanstalk


def setup_parser(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', default='localhost', help='Host of beanstalk server (default %(default)s)')
    parser.add_argument('-p', '--port', default=11300, type=int, help='Port of beanstalk server (default %(default)s)')
    parser.add_argument('-y', '--yes', action='store_true', help='Do not prompt when deleting things')
    parser.add_argument('tubes', nargs='*', help='Tubes to flush (if not passed, does them all!)')
    return parser


def run(args):
    client = simple_beanstalk.BeanstalkClient(args.host, args.port)

    tubes = util.get_tubes(client, args.tubes)

    if not args.yes:
        util.prompt_yesno('Are you sure you want to flush tubes {0} (y/N)? '.format(', '.join(sorted(tubes))))

    ready_jobs = 0
    delayed_jobs = 0
    buried_jobs = 0

    for tube in tubes:
        stats = client.stats_tube(tube)
        ready_jobs += stats['current-jobs-ready']
        delayed_jobs += stats['current-jobs-delayed']
        buried_jobs += stats['current-jobs-buried']

    client.watch('unused-fake-tube')
    for tube in tubes:
        client.watch(tube)
        client.use(tube)
        print('Flushing {0} READY jobs'.format(ready_jobs))
        for job in util.progress(client.reserve_iter(), total=ready_jobs, unit='jobs'):
            client.delete_job(job.job_id)
        print('Flushing {0} DELAYED jobs'.format(delayed_jobs))
        for job in util.progress(client.peek_delayed_iter(), total=delayed_jobs, unit='jobs'):
            client.delete_job(job.job_id)
        print('Flushing {0} BURIED jobs'.format(buried_jobs))
        for job in util.progress(client.peek_buried_iter(), total=buried_jobs, unit='jobs'):
            client.delete_job(job.job_id)
        client.ignore(tube)
    return 0
