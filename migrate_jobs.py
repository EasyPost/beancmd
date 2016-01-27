from __future__ import print_function

import argparse
import io
import random
import struct
import sys
import yaml

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


def non_cryptographically_random_bytes(length):
    bio = io.BytesIO()
    for _ in range(length):
        bio.write(struct.pack('B', random.randint(0, 255)))
    return bio.getvalue()


def print_stats(client, fo):
    stats = dict((k, v) for (k, v) in client.stats().items() if k.startswith('current'))
    yaml.dump(stats, fo, canonical=False, default_flow_style=False)
    print('', file=fo)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-sh', '--source-host', required=True,
                        help='Host of beanstalk server')
    parser.add_argument('-sp', '--source-port', default=11300, type=int,
                        help='Port of beanstalk server (default %(default)s)')
    parser.add_argument('-dh', '--dest-host', required=True,
                        help='Host of beanstalk server')
    parser.add_argument('-dp', '--dest-port', default=11300, type=int,
                        help='Port of beanstalk server (default %(default)s)')
    parser.add_argument('tubes', type=str, nargs='*', help='Tubes to migrate (if not passed, migrates all)')
    args = parser.parse_args()

    source_client = simple_beanstalk.BeanstalkClient(args.source_host, args.source_port)
    dest_client = simple_beanstalk.BeanstalkClient(args.dest_host, args.dest_port)

    if args.tubes:
        tubes = args.tubes
    else:
        tubes = source_client.list_tubes()

    def migrate_job(tube, job, use_on_dest=False):
        if use_on_dest:
            dest_client.use(tube)
        job_stats = source_client.stats_job(job.job_id)
        if job_stats['state'] == 'delayed':
            delay = job_stats['time-left']
        else:
            delay = 0
        dest_client.put_job(job.job_data, pri=job_stats['pri'], ttr=job_stats['ttr'], delay=delay)
        source_client.delete_job(job.job_id)

    print('Beginning migration; source status:', file=sys.stderr)
    print_stats(source_client, sys.stderr)

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

    # watch all the tubes
    for tube in tubes:
        source_client.watch(tube)

    # clean up the buried/delayed jobs. Don't bother doing these one tube at a time
    for tube in tubes:
        source_client.use(tube)

        for job in source_client.peek_delayed_iter():
            migrate_job(tube, job, True)

        for job in source_client.peek_buried_iter():
            # XXX: there seems to be no way to re-bury this job on the other end
            # (you can only bury a job which you have reserved) :-(
            migrate_job(tube, job, True)

    print('Migration complete; source status:', file=sys.stderr)
    print_stats(source_client, sys.stderr)
    print('destination status:', file=sys.stderr)
    print_stats(dest_client, sys.stderr)

    return 0


if __name__ == '__main__':
    sys.exit(main())
