from __future__ import print_function

import argparse
import dbm
import base64
import json

import simple_beanstalk

from . import util


def setup_parser(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', default='localhost', help='Host of beanstalk server (default %(default)s)')
    parser.add_argument('-p', '--port', default=11300, type=int, help='Port of beanstalk server (default %(default)s)')
    parser.add_argument('-st', '--source-tube', required=True, help='Tube to pull jobs from')
    parser.add_argument('-dt', '--dest-tube', required=True, help='Tube to insert jobs into')
    parser.add_argument('-l', '--log', type=argparse.FileType('a'), default=None,
                        help='Log a copy of all jobs to a file as newline-delimited JSON')
    return parser


def filter_fn(job, db_conn):
    # for now, just check that the first arg is in the given dbm file
    # TODO: make this not suck. :-)
    job_json = json.loads(job.job_data.decode('utf-8'))
    if str(job_json['args'][0]) in db_conn:
        return False
    else:
        return True


def run(args):
    source_client = simple_beanstalk.BeanstalkClient(args.host, args.port)
    dest_client = simple_beanstalk.BeanstalkClient(args.host, args.port)

    source_client.watch(args.source_tube)
    dest_client.use(args.dest_tube)

    tube_stats = source_client.stats_tube(args.source_tube)
    num_ready_jobs = tube_stats['current-jobs-ready']

    db_conn = dbm.open('event_ids.db', 'r')

    def migrate_or_delete(job):
        job_stats = source_client.stats_job(job.job_id)
        if job_stats['state'] == 'delayed':
            delay = job_stats['time-left']
        else:
            delay = 0
        args.log.write(json.dumps({
            'tube': args.source_tube,
            'job_data': base64.b64encode(job.job_data).decode('ascii'),
            'pri': job_stats['pri'],
            'ttr': job_stats['ttr'],
            'delay': delay
        }) + '\n')
        if filter_fn(job, db_conn):
            dest_client.put_job(job.job_data, pri=job_stats['pri'], ttr=job_stats['ttr'], delay=delay)
            source_client.delete_job(job.job_id)
        else:
            source_client.delete_job(job.job_id)

    for job in util.progress(source_client.reserve_iter(), total=num_ready_jobs, unit='jobs'):
        try:
            migrate_or_delete(job)
        except Exception:
            source_client.release_job(job.job_id)
