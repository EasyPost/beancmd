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

    for tube in tubes:
        client.watch(tube)
        while True:
            try:
                job = client.reserve_job(timeout=0)
            except simple_beanstalk.BeanstalkError as e:
                if e.message != 'TIMED_OUT':
                    raise
                else:
                    break
            client.delete_job(job.job_id)
    return 0


if __name__ == '__main__':
    sys.exit(main())
