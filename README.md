This repo contains some tools for manipulating Beanstalk queues. They're kind of lame, but have a few advantages over existing tools (e.g., [beanstalk-tools](https://github.com/dustin/beanstalk-tools)):

 - No dependencies except Python 2.7+ and PyYAML
 - Not implemented in PHP
 - No magic

Tools provided:

 - *`migrate_jobs.py`*: Migrate jobs between two beanstalk instances, attempting to preserve as much metadata as possible
 - *`generate_data.py`*: Generate synthetic Beanstalk data
 - *`flush.py`*: Purge a tube
 - *`bury_some_jobs.py`*: Mark the top N jobs in each tube as buried
