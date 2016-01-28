*beancmd* is a simple, self-contained command-line for [beanstalkd](http://kr.github.io/beanstalkd/). It contains its own, minimal, client (`simple_beanstalk`) and should function on Python 2.6 or above.

[![Circle CI](https://circleci.com/gh/EasyPost/beancmd.svg?style=svg&circle-token=2f6ae769a7e6d9c16a6724ff29abb4488421feec)](https://circleci.com/gh/EasyPost/beancmd)

## Dependencies

 - Python 2.6+ or 3.3+
 - `python-argparse` if using Python 2.6 (not required on any newer version)
 - [`PyYAML`](http://pyyaml.org/) 3.x

If [tqdm](https://github.com/tqdm/tqdm) is available, we will use it to display progress bars.

It's **highly** recommended to make sure that PyYAML builds its C extension; all migrations require lots and lots of YAML loading, and we see a 4x-6x performance improvement when using the C loader instead of the pure-Python loader.

## Usage

If you install with `python setup.py install` or through pip, this should drop a program named `beancmd` into your PYTHONHOME. Otherwise, you can use `PYTHONPATH=. python -m beancmd.beancmd` to get the same effect.

### Functions

 * `beancmd migrate`: Migrate jobs between beanstalk instances, attempting to preserve as much metadata as possible. Note that it's currently impossible for us to preserve the buried status of jobs, so when they're migrated, they'll become un-buried. You can skip migrating buried jobs with the `-B` flag
 * `beancmd flush`: Delete all jobs in a tube or set of tubes
 * `beancmd generate`: Generate synthetic data for testing
 * `beancmd bury`: Bury jobs at the top of a tube or set of tubes
 * `beancmd stats`: Pretty-print statistics about the beanstalkd instance
 * `beancmd replay`: The `migrate` command can output a log of the jobs it migrates; this will replay that log in case something goes wrong.


## License
This tool is licensed under the ISC License, the text of which is available at [LICENSE.txt](LICENSE.txt).
