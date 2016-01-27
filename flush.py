import argparse
import sys

import simple_beanstalk


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', default='localhost', help='Host of beanstalk server (default %(default)s)')
    parser.add_argument('-p', '--port', default=11300, type=int, help='Port of beanstalk server (default %(default)s)')
    parser.add_argument('tubes', nargs='*', help='Tubes to flush (if not passed, does them all!)')
    args = parser.parse_args()

    client = simple_beanstalk.BeanstalkClient(args.host, args.port)
    if args.tubes:
        tubes = args.tubes
    else:
        tubes = client.list_tubes()

    client.watch('unused-fake-tube')
    for tube in tubes:
        client.watch(tube)
        client.use(tube)
        for job in client.reserve_iter():
            client.delete_job(job.job_id)
        for job in client.peek_delayed_iter():
            client.delete_job(job.job_id)
        for job in client.peek_buried_iter():
            client.delete_job(job.job_id)
        client.ignore(tube)
    return 0


if __name__ == '__main__':
    sys.exit(main())
