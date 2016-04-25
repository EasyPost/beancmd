import argparse
import sys

from . import __version__

from . import bury
from . import filter_jobs
from . import flush
from . import generate
from . import list_tubes
from . import migrate
from . import peek_top
from . import replay
from . import stats
from . import stats_tubes


COMMANDS = (
    ('bury', 'Bury the jobs on top of a tube', bury),
    ('flush', 'Delete all jobs from a set of tubes', flush),
    ('generate', 'Generate random data for testing', generate),
    ('migrate', 'Migrate jobs between beanstalkd instances', migrate),
    ('stats', 'Print out the stats of a beanstalk instance', stats),
    ('replay', 'Replay a log generated by `beancmd migrate -l`', replay),
    ('list', 'List all tubes on a server', list_tubes),
    ('stats_tubes', 'Print out detailed information about a set of tubes', stats_tubes),
    ('peek_top', 'Print out the top job on a tube', peek_top),
    ('filter_jobs', 'Filter jobs out of a tube, deleting those that do not match the filter', filter_jobs),
)


def main():
    parser = argparse.ArgumentParser(prog='beancmd')
    parser.add_argument('-V', '--version', version='%(prog)s {0}'.format(__version__), action='version')
    subparsers = parser.add_subparsers(title='commands', metavar='COMMAND')

    for (command, help_string, module) in COMMANDS:
        this_parser = subparsers.add_parser(command, help=help_string)
        module.setup_parser(this_parser)
        this_parser.set_defaults(func=getattr(module, 'run'))

    args = parser.parse_args()

    if not getattr(args, 'func', None):
        parser.print_help()
        return 2

    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
