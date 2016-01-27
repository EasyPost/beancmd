import argparse
import itertools
import sys

import simple_beanstalk


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', default='localhost', help='Host of beanstalk server (default %(default)s)')
    parser.add_argument('-p', '--port', default=11300, type=int, help='Port of beanstalk server (default %(default)s)')
    parser.add_argument('-n', '--num-jobs', default=10, type=int, help='How many jobs to bury')
    parser.add_argument('tubes', nargs='*', help='Tubes to bury from (default %(default)s)')
    args = parser.parse_args()

    client = simple_beanstalk.BeanstalkClient(args.host, args.port)

    tubes = args.tubes
    if not tubes:
        tubes = client.list_tubes()

    client.watch('unused-fake-tube')
    for tube in tubes:
        client.watch(tube)
        for job in itertools.islice(client.reserve_iter(), args.num_jobs):
            client.bury_job(job.job_id)
        client.ignore(tube)
    return 0


if __name__ == '__main__':
    sys.exit(main())
