#!/usr/bin/env python3

import argparse
import os
import signal
import sys
import yaml

import devcluster as dc


def standalone_main(config):
    with dc.terminal_config():
        poll = dc.Poll()

        stage_names = [stage_config.name for stage_config in config.stages]

        logger = dc.Logger(stage_names, config.log_dir)

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


def server_main(config, host, port):
    dc.Server(config, host, port).run()


def client_main(host, port):
    client = dc.Client(host, port)
    with dc.terminal_config():
        client.run()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", dest="config", action="store")
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
        standalone_main(config)
    elif args.mode == "server":
        server_main(config, "localhost", args.port)
    elif args.mode == "client":
        client_main("localhost", args.port)


if __name__ == "__main__":
    main()
