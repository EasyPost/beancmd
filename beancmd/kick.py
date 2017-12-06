from __future__ import print_function

import argparse
import itertools
import sys

from . import util

import pystalk


def parse_source(s):
    sources = set(o.strip().lower() for o in s.split(','))
    if sources - set(['buried', 'delayed']):
        raise ValueError('Invalid sources: {:?}', ','.join(sorted(sources - set(['buried', 'delayed']))))
    return sources


def setup_parser(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', default='localhost', help='Host of beanstalk server (default %(default)s)')
    parser.add_argument('-p', '--port', default=11300, type=int, help='Port of beanstalk server (default %(default)s)')
    parser.add_argument('-n', '--num-jobs', default=None, type=int, help='How many jobs to kick (default all in tube)')
    parser.add_argument('-s', '--source', type=parse_source, default='buried,delayed', help='Which queue to kick from (default %(default)s)')
    parser.add_argument('-q', '--quiet', action='store_true')
    parser.add_argument('tubes', nargs='*', help='Tubes to kick from (if not passed, defaults to all)')
    return parser


def run(args):
    client = pystalk.BeanstalkClient(args.host, args.port)

    tubes = util.get_tubes(client, args.tubes)

    for tube in tubes:
        # KICK uses USE instead of WATCH
        client.use(tube)
        stats = client.stats_tube(tube)
        iterators = []
        total = 0
        if 'buried' in args.source:
            iterators.append(client.peek_buried_iter())
            total += stats['current-jobs-buried']
        if 'delayed' in args.source:
            iterators.append(client.peek_delayed_iter())
            total += stats['current-jobs-delayed']
        iterator = itertools.chain(*iterators)
        if args.num_jobs is not None:
            iterator = itertools.islice(iterator, args.num_jobs)
            total = min(args.num_jobs, total)
        if not args.quiet:
            print('Kicking ~{0} {2} jobs on tube {1}'.format(total, tube, '+'.join(args.source)), file=sys.stderr)
        for job in util.progress(iterator, total=total, unit='jobs'):
            client.kick_job(job.job_id)
    return 0
