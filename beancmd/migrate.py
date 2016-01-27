from __future__ import print_function

import argparse
import sys
import yaml

from . import util

import simple_beanstalk


def chunk(iterable, chunk_size):
    """Break an iterable into chunks of no more than chunk_size"""
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def print_stats(client, fo):
    stats = dict((k, v) for (k, v) in client.stats().items() if k.startswith('current'))
    yaml.dump(stats, fo, canonical=False, default_flow_style=False)
    print('', file=fo)


def setup_parser(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser()
    parser.add_argument('-sh', '--source-host', required=True,
                        help='Host of beanstalk server')
    parser.add_argument('-sp', '--source-port', default=11300, type=int,
                        help='Port of beanstalk server (default %(default)s)')
    parser.add_argument('-dh', '--dest-host', required=True,
                        help='Host of beanstalk server')
    parser.add_argument('-dp', '--dest-port', default=11300, type=int,
                        help='Port of beanstalk server (default %(default)s)')
    parser.add_argument('-B', '--skip-buried', action='store_true', help='Do not migrate any buried jobs')
    parser.add_argument('-q', '--quiet', action='store_true', help='Be quieter')
    parser.add_argument('tubes', type=str, nargs='*', help='Tubes to migrate (if not passed, migrates all)')
    return parser


def run(args):
    source_client = simple_beanstalk.BeanstalkClient(args.source_host, args.source_port)
    dest_client = simple_beanstalk.BeanstalkClient(args.dest_host, args.dest_port)

    tubes = util.get_tubes(source_client, args.tubes)

    def migrate_job(tube, job, use_on_dest=False):
        """migrate a single job from source_client to dest_client"""
        if use_on_dest:
            dest_client.use(tube)
        job_stats = source_client.stats_job(job.job_id)
        if job_stats['state'] == 'delayed':
            delay = job_stats['time-left']
        else:
            delay = 0
        dest_client.put_job(job.job_data, pri=job_stats['pri'], ttr=job_stats['ttr'], delay=delay)
        source_client.delete_job(job.job_id)

    if not args.quiet:
        print('Beginning migration; source status:', file=sys.stderr)
        print_stats(source_client, sys.stderr)

    # beanstalk doesn't let you have 0 tubes watched, and will force "default" into your tube list if
    # you would ever go to 0 tubes. So we just watch a fake tube. Meh.
    source_client.watch('unused-fake-tube')

    # do one pass, one tube at a time, to move over ready jobs. batch these up and coalesce the USE
    # statements so that these go quickly
    for tube in tubes:
        source_client.watch(tube)
        dest_client.use(tube)
        for job_chunk in chunk(source_client.reserve_iter(), 10):
            for job in job_chunk:
                migrate_job(tube, job)
        source_client.ignore(tube)

    # clean up the buried/delayed jobs. Don't bother doing these one tube at a time
    for tube in tubes:
        # peek commands only work on the USEd tube, not the WATCHd tubes
        source_client.use(tube)

        # XXX: this is racy. The job could pop out of the delayed iter and be picked up by a
        # consumer while we're migrating it.
        for job in source_client.peek_delayed_iter():
            migrate_job(tube, job, True)

        if not args.skip_buried:
            for job in source_client.peek_buried_iter():
                # XXX: there seems to be no way to re-bury this job on the other end (you can only bury
                # a job which you have reserved) :-(
                migrate_job(tube, job, True)

    if not args.quiet:
        print('Migration complete; source status:', file=sys.stderr)
        print_stats(source_client, sys.stderr)
        print('destination status:', file=sys.stderr)
        print_stats(dest_client, sys.stderr)

    return 0
