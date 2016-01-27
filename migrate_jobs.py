import argparse
import io
import random
import struct
import sys
import time

import simple_beanstalk


# how often to check for delayed jobs
DELAY_LOOP_TIME = 30


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
        dest_client.put_job(job.job_data, pri=job_stats['pri'], ttr=job_stats['ttr'])
        source_client.delete_job(job.job_id)

    source_client.watch('unused-fake-tube')

    # do one pass, one tube at a time, to move over ready jobs
    for tube in tubes:
        source_client.watch(tube)
        dest_client.use(tube)
        for job_chunk in chunk(source_client.reserve_iter_nb(), 10):
            for job in job_chunk:
                migrate_job(tube, job)
        source_client.ignore(tube)

    # watch all the tubes
    for tube in tubes:
        source_client.watch(tube)

    # If there are buried jobs, kick them
    buried_jobs = source_client.stats()['current-jobs-buried']

    if buried_jobs:
        for tube in tubes:
            source_client.use(tube)
            # XXX this is not transactionally safe if someone else runs KICK in parallel
            source_client.kick_jobs(buried_jobs)

    # there might still be reserved jobs. If so, let's
    while True:
        start = time.time()
        stats = source_client.stats()
        if stats['current-jobs-ready'] + stats['current-jobs-delayed'] == 0:
            break

        for job in source_client.reserve_iter_nb():
            migrate_job(tube, job, True)
        end = time.time()

        duration = end - start

        if duration < DELAY_LOOP_TIME:
            time.sleep(DELAY_LOOP_TIME - duration)

    return 0


if __name__ == '__main__':
    sys.exit(main())
