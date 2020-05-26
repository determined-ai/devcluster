#!/usr/bin/env python3

import argparse
import contextlib
import fcntl
import os
import signal
import sys
import yaml

import devcluster as dc


def standalone_main(config):
    with dc.terminal_config():
        poll = dc.Poll()

        stage_names = [stage_config.name for stage_config in config.stages]

        logger = dc.Logger(stage_names, config.temp_dir)

        state_machine = dc.StateMachine(logger, poll)

        console = dc.Console(
            logger, poll, stage_names, state_machine.set_target, state_machine.quit
        )
        state_machine.add_callback(console.state_cb)

        def _sigwinch_handler(signum, frame):
            """Enqueue a call to _sigwinch_callback() via poll()."""
            os.write(state_machine.get_report_fd(), b"W")

        def _sigwinch_callback():
            """Handle the SIGWINCH when it is safe"""
            console.handle_window_change()

        state_machine.add_report_callback("W", _sigwinch_callback)
        signal.signal(signal.SIGWINCH, _sigwinch_handler)

        for stage_config in config.stages:
            state_machine.add_stage(
                stage_config.build_stage(poll, logger, state_machine)
            )

        # Draw the screen ASAP
        console.start()

        state_machine.set_target(len(stage_names))

        for c in config.startup_input:
            console.handle_key(c)

        while state_machine.should_run():
            poll.poll()


class Oneshot:
    """
    Watch the state to print a message when the final state is up and to quit if anything fails.
    """

    def __init__(self, quit_cb):
        self.first_target = None
        self.up = False
        self.failing = False
        self.quit_cb = quit_cb

    def state_cb(self, state, substate, target):
        if self.first_target is None:
            self.first_target = target

        # Did we reach the target stat?
        if state == self.first_target and substate == "" and not self.up:
            self.up = True
            print("cluster is up")

        # Is the cluster failing?
        if target != self.first_target and not self.failing:
            self.failing = True
            print("cluster is failing")
            self.quit_cb()


def server_main(config, host, port, oneshot_mode):
    server = dc.Server(config, host, port)

    if oneshot_mode:

        # In case of a signal state_machine.quit() may have been called, so we check try to detect
        # if we need to call quit or not from the Oneshot.
        def quit_cb():
            if not server.state_machine.quitting:
                server.state_machine.quit()

        oneshot = Oneshot(quit_cb)
        server.state_machine.add_callback(oneshot.state_cb)

    server.run()


def client_main(host, port):
    client = dc.Client(host, port)
    with dc.terminal_config():
        client.run()


@contextlib.contextmanager
def lockfile(filename):
    with open(filename, "w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            print(
                f"{filename} is already locked, is another devcluster running?",
                file=sys.stderr,
            )
            sys.exit(1)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", dest="config", action="store")
    parser.add_argument("-1", "--oneshot", dest="oneshot", action="store_true")
    parser.add_argument(
        "-p", "--port", dest="port", action="store", type=int, default=None
    )
    parser.add_argument(
        "mode",
        nargs="?",
        default="standalone",
        choices=["standalone", "server", "client"],
    )
    args = parser.parse_args()

    # Validate mode
    if args.mode == "standalone" and args.port is not None:
        print("--port is not allowed with standalone mode", file=sys.stderr)
        sys.exit(1)
    if args.mode != "standalone" and args.port is None:
        print(f"--port is required with {args.mode} mode", file=sys.stderr)
        sys.exit(1)

    # Validate oneshot.
    if args.oneshot and args.mode != "server":
        print("--oneshot is only supported in server mode", file=sys.stderr)
        sys.exit(1)

    # Read config before the chdir()
    if args.config is not None:
        with open(args.config) as f:
            config = dc.Config(yaml.safe_load(f.read()))
    else:
        check_paths = []
        if "HOME" in os.environ:
            check_paths.append(os.path.join(os.environ["HOME"], ".devcluster.yaml"))
        if "XDG_CONFIG_HOME" in os.environ:
            check_paths.append(
                os.path.join(os.environ["XDG_CONFIG_HOME"], "devcluster.yaml")
            )
        for path in check_paths:
            if os.path.exists(path):
                with open(path) as f:
                    config = dc.Config(yaml.safe_load(f.read()))
                    break
        else:
            print(
                "you must either specify --config or use the file",
                " or ".join(check_paths),
                file=sys.stderr,
            )
            sys.exit(1)

    if "DET_PROJ" not in os.environ:
        print("you must specify the DET_PROJ environment variable", file=sys.stderr)
        sys.exit(1)
    os.chdir(os.environ["DET_PROJ"])

    if args.mode == "standalone":
        os.makedirs(config.temp_dir, exist_ok=True)
        with lockfile(os.path.join(config.temp_dir, "lock")):
            standalone_main(config)
    elif args.mode == "server":
        os.makedirs(config.temp_dir, exist_ok=True)
        with lockfile(os.path.join(config.temp_dir, "lock")):
            server_main(config, "localhost", args.port, args.oneshot)
    elif args.mode == "client":
        client_main("localhost", args.port)


if __name__ == "__main__":
    main()
