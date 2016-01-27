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

    stats = client.stats()

    max_key_width = max(len(k) for k in stats)
    fmt_string = '{{key: <{0}}} | {{value}}'.format(max_key_width)

    for k, v in sorted(stats.items()):
        print(fmt_string.format(key=k, value=v))
