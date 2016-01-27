import argparse
import sys

from . import bury
from . import flush
from . import generate
from . import migrate
from . import stats


COMMANDS = (
    ('bury', 'Bury the jobs on top of a tube', bury),
    ('flush', 'Delete all jobs from a set of tubes', flush),
    ('generate', 'Generate random data for testing', generate),
    ('migrate', 'Migrate jobs between beanstalkd instances', migrate),
    ('stats', 'Print out the stats of a beanstalk instance', stats)
)


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='commands')

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
