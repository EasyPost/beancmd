*beancmd* is a simple ops-focused command-line for [beanstalkd](http://kr.github.io/beanstalkd/) written in Python.

[![Circle CI](https://circleci.com/gh/EasyPost/beancmd.svg?style=svg&circle-token=2f6ae769a7e6d9c16a6724ff29abb4488421feec)](https://circleci.com/gh/EasyPost/beancmd)

## Dependencies

 - Python 2.7+ or 3.5+
 - [`PyYAML`](http://pyyaml.org/) 3.x
 - [`pystalk`](https://github.com/easypost/pystalk)

If [tqdm](https://github.com/tqdm/tqdm) is available, we will use it to display progress bars.

It's **highly** recommended to make sure that PyYAML builds its C extension; all migrations require lots and lots of YAML loading, and we see a 4x-6x performance improvement when using the C loader instead of the pure-Python loader.

## Usage

If you install with `python setup.py install` or through pip, this should drop a program named `beancmd` into your PYTHONHOME. Otherwise, you can use `PYTHONPATH=. python -m beancmd.beancmd` to get the same effect.

The original purpose of this package was to migrate tasks between beanstalk instances, so it's worth discussing a little bit about how that works.

 - Migration is a two-pass system: first we migrate out all READY jobs, which can safely be done while other workers are running. Then, unless `-D` and `-B` are passed, we migrate out DELAYED and BURIED jobs. Since this isn't a transactional process, it should not be run while any other workers are connected. It's also possible (in fact, likely) that jobs will become READY while you're migrating DELAYED jobs, so you'll probably have to run migration more than once to get everything
 - It's currently impossible for us to preserve the buried status of jobs, so when they're migrated, they'll become un-buried. You can skip migrating buried jobs with the `-B` flag.
 - Tubes can be specified using `fnmatch` glob characters (e.g., `beancmd migrate -sh foo -dh bar service*` to migrate all tubes beginning with the string "service"). We automatically apply globbing on any tube name which contains a `*` or `?` character.

### Functions

 * `beancmd bury`: Bury jobs at the top of a tube or set of tubes
 * `beancmd flush`: Delete all jobs in a tube or set of tubes
 * `beancmd generate`: Generate synthetic data for testing
 * `beancmd migrate`: Migrate jobs between beanstalk instances, attempting to preserve as much metadata as possible. 
 * `beancmd replay`: The `migrate` command can output a log of the jobs it migrates; this will replay that log in case something goes wrong.
 * `beancmd stats`: Pretty-print statistics about the beanstalkd instance
 * `beancmd stats_tubes`: Print a table of per-tube statistics about a given beanstalkd instance (similar in appearance to [beanstalk-console](https://github.com/ptrofimov/beanstalk_console))


## License
This tool is licensed under the ISC License, the text of which is available at [LICENSE.txt](LICENSE.txt).
