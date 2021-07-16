#!/usr/bin/env python3

import argparse
import contextlib
import fcntl
import os
import traceback
import signal
import subprocess
import re
import sys
import yaml
from typing import Optional, Sequence

import devcluster as dc


def standalone_main(config):
    # Write a traceback to stdout on SIGUSR1 (10)
    def _traceback_signal(signum, frame):
        with open(os.path.join(config.temp_dir, "traceback"), "a") as f:
            f.write("------\n")
            traceback.print_stack(frame, file=f)

    signal.signal(signal.SIGUSR1, _traceback_signal)

    with dc.terminal_config():
        poll = dc.Poll()

        stage_names = [stage_config.name for stage_config in config.stages]

        logger = dc.Logger(stage_names, config.temp_dir)

        state_machine = dc.StateMachine(logger, poll, config.commands)

        console = dc.Console(
            logger,
            poll,
            stage_names,
            state_machine.set_target,
            config.commands,
            state_machine.run_command,
            state_machine.quit,
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


def get_host_addr_for_docker() -> Optional[str]:
    if "darwin" in sys.platform:
        # On macOS, docker runs in a VM and host.docker.internal points to the IP
        # address of this VM.
        return "host.docker.internal"

    # On non-macOS, host.docker.internal does not exist. Instead, grab the source IP
    # address we would use if we had to talk to the internet. The sed command
    # searches the first line of its input for "src" and prints the first field
    # after that.
    proxy_addr_args = ["ip", "route", "get", "8.8.8.8"]
    pattern = r"s|.* src +(\S+).*|\1|"
    s = subprocess.check_output(proxy_addr_args, encoding="utf-8")
    matches = re.match(pattern, s)
    if matches is not None:
        groups: Sequence[str] = matches.groups()
        if len(groups) != 0:
            return groups[0]

    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", dest="config", action="store")
    parser.add_argument("-1", "--oneshot", dest="oneshot", action="store_true")
    parser.add_argument("-C", "--cwd", dest="cwd", action="store")
    parser.add_argument("--no-guess-host", dest="no_guess_host", action="store_true")
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

    # Read config before the cwd.
    if args.config is not None:
        config_path = args.config
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
                config_path = path
                break
        else:
            print(
                "you must either specify --config or use the file",
                " or ".join(check_paths),
                file=sys.stderr,
            )
            sys.exit(1)

    env = dict(os.environ)

    if "DOCKER_LOCALHOST" in env or not args.no_guess_host:
        docker_localhost = get_host_addr_for_docker()
        if docker_localhost is None:
            print(
                "Unable to guess the value for $DOCKER_LOCALHOST.\n"
                "If you do not need $DOCKER_LOCALHOST in your devcluster config\n"
                "you can disable the check by passing --no-guess-host or by\n"
                "setting the DOCKER_LOCALHOST environment variable yourself.",
                file=sys.stderr,
            )
            sys.exit(1)
        env["DOCKER_LOCALHOST"] = get_host_addr_for_docker()

    with open(config_path) as f:
        config = dc.Config(dc.expand_env(yaml.safe_load(f.read()), env))

    # Process cwd.
    cwd_path = None
    if args.cwd is not None:
        cwd_source = "--cwd"
        cwd_path = args.cwd
    elif config.cwd is not None:
        cwd_source = "config.cwd"
        cwd_path = config.cwd
    elif "DET_PROJ" in os.environ:
        # Legacy setup, from when -C/--cwd wasn't an option.
        cwd_source = "$DET_PROJ"
        cwd_path = os.environ["DET_PROJ"]

    if cwd_path is not None:
        if not os.path.exists(cwd_path):
            print(f"{cwd_source}: {cwd_path} does not exist!", file=sys.stderr)
            sys.exit(1)
        if not os.path.isdir(cwd_path):
            print(f"{cwd_source}: {cwd_path} is not a directory!", file=sys.stderr)
            sys.exit(1)
        os.chdir(cwd_path)

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
