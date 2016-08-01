from __future__ import print_function

import argparse
import base64
import json
import sys
import yaml

from . import util

import pystalk


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
    parser.add_argument('-D', '--skip-delayed', action='store_true', help='Do not migrate any delayed jobs')
    parser.add_argument('-B', '--skip-buried', action='store_true', help='Do not migrate any buried jobs')
    parser.add_argument('-q', '--quiet', action='store_true', help='Be quieter')
    parser.add_argument('-l', '--log', type=argparse.FileType('a'), default=None,
                        help='Log a copy of all migrated jobs to a file as newline-delimited JSON')
    parser.add_argument('-n', '--num-jobs', type=int, default=None,
                        help='If passed, migrate at most N jobs')
    parser.add_argument('-T', '--destination-tube', default=None,
                        help='Tube into which to insert jobs. Only valid if migrating a single tube')
    parser.add_argument('tubes', type=str, nargs='*', help='Tubes to migrate (if not passed, migrates all)')
    return parser


def migrate_jobs(args, tubes, source_client, dest_client):
    migrated_jobs = 0

    def migrate_job(tube, job, use_on_dest=False):
        """migrate a single job from source_client to dest_client"""
        if use_on_dest:
            if args.destination_tube:
                dest_client.use(args.destination_tube)
            else:
                dest_client.use(tube)
        job_stats = source_client.stats_job(job.job_id)
        if job_stats['state'] == 'delayed':
            delay = job_stats['time-left']
        else:
            delay = 0
        if args.log is not None:
            args.log.write(json.dumps({
                'tube': tube,
                'job_data': base64.b64encode(job.job_data).decode('ascii'),
                'pri': job_stats['pri'],
                'ttr': job_stats['ttr'],
                'delay': delay
            }) + '\n')
        dest_client.put_job(job.job_data, pri=job_stats['pri'], ttr=job_stats['ttr'], delay=delay)
        source_client.delete_job(job.job_id)

    # beanstalk doesn't let you have 0 tubes watched, and will force "default" into your tube list if
    # you would ever go to 0 tubes. So we just watch a fake tube. Meh.
    source_client.watch('unused-fake-tube')

    # do one pass, one tube at a time, to move over ready jobs. batch these up and coalesce the USE
    # statements so that these go quickly
    for tube in tubes:
        source_client.watch(tube)
        if args.destination_tube:
            dest_client.use(args.destination_tube)
        else:
            dest_client.use(tube)
        tube_stats = source_client.stats_tube(tube)
        num_ready_jobs = tube_stats['current-jobs-ready']

        if not args.quiet:
            print('Migrating ~{0} READY jobs on tube {1}'.format(num_ready_jobs, tube), file=sys.stderr)
        for job in util.progress(source_client.reserve_iter(), total=num_ready_jobs, unit='jobs'):
            migrate_job(tube, job)
            migrated_jobs += 1
            if args.num_jobs and migrated_jobs >= args.num_jobs:
                return migrated_jobs
        source_client.ignore(tube)

    # clean up the buried/delayed jobs. Don't bother doing these one tube at a time
    for tube in tubes:
        # peek commands only work on the USEd tube, not the WATCHd tubes
        source_client.use(tube)

        if not args.skip_delayed:
            tube_stats = source_client.stats_tube(tube)
            num_delayed_jobs = tube_stats['current-jobs-delayed']

            if not args.quiet:
                print('Migrating ~{0} DELAYED jobs on tube {1}'.format(num_delayed_jobs, tube), file=sys.stderr)

            # XXX: this is racy. The job could pop out of the delayed iter and be picked up by a
            # consumer while we're migrating it.
            for job in util.progress(source_client.peek_delayed_iter(), total=num_delayed_jobs, unit='jobs'):
                migrate_job(tube, job, True)
                migrated_jobs += 1
                if args.num_jobs and migrated_jobs >= args.num_jobs:
                    return migrated_jobs

        if not args.skip_buried:

            tube_stats = source_client.stats_tube(tube)
            num_buried_jobs = tube_stats['current-jobs-buried']

            if not args.quiet:
                print('Migrating ~{0} BURIED jobs on tube {1}'.format(num_buried_jobs, tube), file=sys.stderr)

            for job in util.progress(source_client.peek_buried_iter(), total=num_buried_jobs, unit='jobs'):
                # XXX: there seems to be no way to re-bury this job on the other end (you can only bury
                # a job which you have reserved) :-(
                migrate_job(tube, job, True)
                migrated_jobs += 1
                if args.num_jobs and migrated_jobs >= args.num_jobs:
                    return migrated_jobs

    return migrated_jobs


def run(args):
    source_client = pystalk.BeanstalkClient(args.source_host, args.source_port)
    dest_client = pystalk.BeanstalkClient(args.dest_host, args.dest_port)

    tubes = util.get_tubes(source_client, args.tubes)

    if args.destination_tube and len(tubes) > 1:
        raise ValueError(
            'Cannot specify destination tube when migrating more than one tube (saw {0})'.format(
                ', '.join(sorted(tubes))
            )
        )

    if not args.quiet:
        print('Beginning migration; source status:', file=sys.stderr)
        print_stats(source_client, sys.stderr)

    migrated_jobs = migrate_jobs(args, tubes, source_client, dest_client)

    if not args.quiet:
        print(file=sys.stderr)
        print('Migration complete; migrated {0} jobs; source status:'.format(migrated_jobs), file=sys.stderr)
        print_stats(source_client, sys.stderr)
        print('destination status:', file=sys.stderr)
        print_stats(dest_client, sys.stderr)

    if args.log is not None:
        args.log.flush()

    return 0
