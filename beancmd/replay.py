import argparse
import base64
import json

import pystalk


def setup_parser(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', default='localhost', help='Host of beanstalk server (default %(default)s)')
    parser.add_argument('-p', '--port', default=11300, type=int, help='Port of beanstalk server (default %(default)s)')
    parser.add_argument('log_file', type=argparse.FileType('r'), help='Path to log file')
    return parser


def run(args):
    client = pystalk.BeanstalkClient(args.host, args.port)

    for line in args.log_file:
        job = json.loads(line.rstrip())
        job_data = base64.b64decode(job['job_data'])
        client.use(job['tube'])
        client.put_job(job_data, pri=job['pri'], delay=job['delay'], ttr=job['ttr'])
