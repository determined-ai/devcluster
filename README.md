# devcluster.py

A developer tool for running the Determined cluster.  Definitely still a WIP.

## Configuring

Check out the example.yaml config file, which should work on tip-of-master.  It
will not quite work out-of-the box; you'll have to replace `MY_IP_ADDR` with
your computer's external ip address.

Note that relative paths in the config file should be relative to the path in
the `DET_PROJ` environment variable (see `Running`, below).

## Running

You will need to set the `DET_PROJ` environment variable.  `devcluster.py` will
`cd` into that directory after reading the `--config` option on the command
line but before doing anything else.

You can either specify a config file via the `--config` or `-c` option, or
`devcluster.py` will try to read the default config file at
`~/.devcluster.yaml`.

## Keybindings

Keybindings are shown in the status bar, but in general and `1`-`4` on the
keyboard set target states for the cluster, and `!`-`$` toggle log streams for
each different stage.

`q` or `ctrl`+`c` once to quit, or twice to force-quit (which may leave
dangling docker containers laying around).

## Philosophy

`devcluster.py` tries as much as possible to be a standalone tool for running
arbitrary versions of the determined cluster.  Currently it is only tested as
far back as v0.12.3.

Some defaults are kept up-to-date with tip-of-master to simplify the config
file for the most-common use case, but in general preference was given to being
configurable and explicit.

## TODO

- make log view scrollable
- support docker volumes
