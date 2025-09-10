0.4.3
_____
- The `bury`, `flush`, `kick`, `pause`, and `purge_buried` commands now verify that,
if given a specific set of tube names, those tubes exist on the given server before
proceeding. This is in order to reduce confusion for sensitive or destructive 
commands.

0.4.2
-----
- Do not include tests in distributed package even when not processing `MANIFEST.in`

0.4.1
-----
- Do not include tests in distributed package

0.4.0
-----
- Add `kick` subcommand
- Drop Python 2.6, 3.3, and 3.4 from support list

0.3.2
-----
- Add reserved jobs to the `stats_tubes` subcommand
- Add `-m` argument to the `flush` subcommand

0.3.1
----
- Switch to `pystalk` for beanstalk connectivity

0.3
---
- `pause` command for pausing and unpausing tubes
- `purge_buried` which deletes all buried jobs on a tube
- `stats_tubes` command which prints out detailed stats about a group of tubes
