import argparse
import itertools

from . import util

import simple_beanstalk


def setup_parser(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', default='localhost', help='Host of beanstalk server (default %(default)s)')
    parser.add_argument('-p', '--port', default=11300, type=int, help='Port of beanstalk server (default %(default)s)')
    parser.add_argument('-n', '--num-jobs', default=10, type=int, help='How many jobs to bury')
    parser.add_argument('tubes', nargs='*', help='Tubes to bury from (if not passed, defaults to all)')
    return parser


def run(args):
    client = simple_beanstalk.BeanstalkClient(args.host, args.port)

    tubes = util.get_tubes(client, args.tubes)

    client.watch('unused-fake-tube')
    for tube in tubes:
        client.watch(tube)
        for job in itertools.islice(client.reserve_iter(), args.num_jobs):
            client.bury_job(job.job_id)
        client.ignore(tube)
    return 0
