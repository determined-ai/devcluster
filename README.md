# `devcluster`

A developer tool for running the Determined cluster.

## Configuring and Running

You can run the code from the git root by running `python -m devcluster`, or
install it into your python environment with `pip install -e .` and then just
call `devcluster` by itself.

The first time you run it, it will give you the option to automatically install
the default configuration file (see `devcluster/example.yaml`).

By default, devcluster needs to run from the root of a `determined` source
tree.  You can do this in one of a few ways:
- always run `devcluster` from the root of your `determined` source tree
- set `cwd: /path/to/determined` to run `devcluster` from anywhere
- run `devcluster` with the `-C /path/to/determined` option

You can either specify a config file via the `--config` or `-c` option, or
`devcluster` will try to read the default config file at
`~/.devcluster.yaml`.

## How `devcluster` Works

devcluster runs a cluster with a linear dependency graph of stages.  With the
default config (`devcluster/example.yaml`), those stages are:

- `DEAD`
- `DB`
- `MASTER`
- `AGENT`

Because of the linear dependency graph, the `DB` stage can run by itself, the
`MASTER` stage is only started after `DB` is already up, and the `AGENT` is only
started after the `MASTER` is up.  The `DEAD` stage indicates that nothing
is running.  The non-`DEAD` stages correspond to the `stages` setting in your
config file.

The status bar has two rows.  The first row is the state row.  It shows all of
the stages in your dependency graph.  All active stages are highlighted orange.
There is also a 'target state', which is indicated by a less-than character
(`<`).  You set the target state by pressing the key corresponding to a
particular stage.

When the target state is modified, devcluster will walk up or down the stage
list, starting or killing processes as needed to reach the target stage.  In
the default configuration, the MASTER and AGENT stages are rebuilt each time
they are started.

A common pattern during development might be to:
- make a change to the master code
- press `1` to set the target state to `DB` (killing the `MASTER` and the
  `AGENT`)
- press `3` to set the target state back to `AGENT` (rebuilding/restarting the
  `MASTER` and `AGENT` on the way).

The second row in the status bar is the logging row.  Every stage collects a
separate stream of logs, and each stream can be toggled by pressing the
corresponding keys.

## Keybindings

- `1`-`3` set target states for the cluster (as shown in the status bar)
- `!`-`#` toggle logs for the corresponding stage (as shown in the status bar)
- `q` or `ctrl`+`c` once to quit, or twice to force-quit (which may leave
  dangling processes or docker containers laying around)
- `u`/`d` will scroll up/down, and `U`/`D` will scroll up/down faster
  Scrolling support is only partially line-aware; it will scroll by
  literal log chunks, which may each be more or less than a line
- `x` will reset scrolling to continue following logs
- Spacebar will emit a visual separator with a timestamp to the console stream.
- With the default configuration, the keys `p`, `w`, and `c` will trigger
  building the python harness, webui, and docs, respectively.

## Server/Client Mode

You can run `devcluster` as a headless server with various configurations for
listening for clients:

    # listen on 0.0.0.0:1234
    devcluster --quiet --listen 1234

    # listen on localhost:1234
    devcluster -q -l 127.0.0.1:1234

    # listen on a unix socket
    devcluster -q -l /path/to/socket

    # listen on the default unix socket (/tmp/devcluster/sock)
    devcluster -q

    # configure multiple listeners
    devcluster -q -l 1234 /path/to/socket

Sending a `SIGINT` (via `ctrl`+`c`) or a `SIGTERM` (via `kill`) to the headless
server will close it.

You can then connect the UI to a headless server:

    devcluster 1234
    devcluster host:1234
    devcluster /path/to/socket

In client mode, pressing `q` or `ctrl`+`c` will only close the UI; it will not
affect the server.

## Oneshot Mode

You can run `devcluster` in a special oneshot mode (`-1`/`--oneshot`), suitable
for automated usage.  In automated mode, the `stdout` from `devcluster` will be
suitable for redirecting to a file, and the `stderr` will emit the line:

    devcluster is up

to indicate when the cluster is ready for use.

## Philosophy

`devcluster` tries as much as possible to be a standalone tool for running
arbitrary versions of the Determined cluster.  It is tested as far back as
v0.12.3.

Some defaults are kept up-to-date with tip-of-master to simplify the config
file for the most-common use case, but in general preference was given to being
configurable and explicit.

## TODO

- config file reloading
