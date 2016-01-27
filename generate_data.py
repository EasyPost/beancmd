import argparse
import base64
import json
import io
import random
import struct
import sys

import simple_beanstalk


def parse_range(string):
    if '-' not in string:
        return int(string), int(string)
    min_b, max_b = string.split('-')
    return int(min_b), int(max_b)


def non_cryptographically_random_bytes(length):
    bio = io.BytesIO()
    for _ in range(length):
        bio.write(struct.pack('B', random.randint(0, 255)))
    return bio.getvalue()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', default='localhost', help='Host of beanstalk server (default %(default)s)')
    parser.add_argument('-p', '--port', default=11300, type=int, help='Port of beanstalk server (default %(default)s)')
    parser.add_argument('-s', '--task-size', default='10-1000', type=parse_range,
                        help='Size range of tasks to generate, in bytes (default %(default)s)')
    parser.add_argument('-d', '--delay-range', default='0', type=parse_range,
                        help='Size range of delays to generate, in seconds (default %(default)s)')
    parser.add_argument('number_of_tasks_per_tube', type=int, help='Number of tasks to generate per tube')
    parser.add_argument('tubes', type=str, nargs='+', help='Tubes into which tasks should be inserted')
    args = parser.parse_args()

    client = simple_beanstalk.BeanstalkClient(args.host, args.port)
    for tube in args.tubes:
        client.use(tube)
        for i in range(args.number_of_tasks_per_tube):
            data_size = random.randint(*args.task_size)
            data = non_cryptographically_random_bytes(data_size)
            data = base64.b64encode(data).decode('ascii')
            job = json.dumps({'sequence': i, 'data': data})
            priority = random.randrange(0, 1 << 31)
            delay = random.randint(*args.delay_range)
            state, job_id = client.put_job(job, pri=priority, delay=delay)
    return 0


if __name__ == '__main__':
    sys.exit(main())
