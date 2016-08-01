from __future__ import print_function

import argparse

import pystalk

from . import util


def setup_parser(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', default='localhost', help='Host of beanstalk server (default %(default)s)')
    parser.add_argument('-p', '--port', default=11300, type=int, help='Port of beanstalk server (default %(default)s)')
    parser.add_argument('tubes', nargs='*', help='Tubes to bury from (if not passed, defaults to all)')
    return parser


def run(args):
    client = pystalk.BeanstalkClient(args.host, args.port)

    tubes = util.get_tubes(client, args.tubes)

    if tubes:
        tube_column_length = max(len(l) for l in tubes)
    else:
        tube_column_length = 1

    fmt_string = '| {{tube: <{0}}} | {{ready:>9}} | {{delayed:>9}} | {{buried:>9}} |'.format(tube_column_length)

    header_line = fmt_string.replace('>', '^').format(tube='', ready='ready', delayed='delayed', buried='buried')
    print('-' * len(header_line))
    print(header_line)
    print('-' * len(header_line))
    for tube in sorted(tubes):
        stats = client.stats_tube(tube)
        print(fmt_string.format(
            tube=tube,
            ready=stats['current-jobs-ready'],
            delayed=stats['current-jobs-delayed'],
            buried=stats['current-jobs-buried'],
        ))
    print('-' * len(header_line))
