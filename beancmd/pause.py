from __future__ import print_function
import argparse

from . import util

import pystalk


def setup_parser(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', default='localhost', help='Host of beanstalk server (default %(default)s)')
    parser.add_argument('-p', '--port', default=11300, type=int, help='Port of beanstalk server (default %(default)s)')
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('-P', '--pause', action='store_true', help='Pause tubes')
    action.add_argument('-U', '--unpause', action='store_true', help='Unpause tubes')
    parser.add_argument('-d', '--delay', type=int, default=7200, help='Time to pause tube for')
    parser.add_argument('tubes', nargs='*', help='Tubes to act on (if not passed, does them all; allows globs)')
    return parser


def run(args):
    client = pystalk.BeanstalkClient(args.host, args.port)

    tubes = util.get_tubes(client, args.tubes)

    for tube in tubes:
        stats = client.stats_tube(tube)
        left = stats.get('pause-time-left', 0)
        if args.pause:
            if left:
                print('Pausing {0} for {1}s (was previously paused for {2}s)'.format(tube, args.delay, left))
            else:
                print('Pausing {0} for {1}s'.format(tube, args.delay))
            client.pause_tube(tube, args.delay)
        elif args.unpause:
            if left:
                print('Unpausing {0}'.format(tube))
                client.unpause_tube(tube)
            else:
                print('Not unpausing {0} -- was not paused'.format(tube))
    return 0
