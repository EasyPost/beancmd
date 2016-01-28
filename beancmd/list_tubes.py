from __future__ import print_function

import argparse

import simple_beanstalk


def setup_parser(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', default='localhost', help='Host of beanstalk server (default %(default)s)')
    parser.add_argument('-p', '--port', default=11300, type=int, help='Port of beanstalk server (default %(default)s)')
    return parser


def run(args):
    client = simple_beanstalk.BeanstalkClient(args.host, args.port)

    tubes = client.list_tubes()

    print('\n'.join(sorted(tubes)))
