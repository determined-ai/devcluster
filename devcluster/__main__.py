#!/usr/bin/env python3

import argparse
import contextlib
import fcntl
import os
import subprocess
import re
import sys
import yaml
import typing

import appdirs

import devcluster as dc


# prefer stdlib importlib.resources over pkg_resources, when available
try:
    import importlib.resources

    resources = importlib.resources  # type: typing.Any

    def _get_example_yaml() -> bytes:
        ref = resources.files("devcluster").joinpath("example.yaml")
        with ref.open("rb") as f:
            return f.read()  # type: ignore

except ImportError:
    import pkg_resources

    def _get_example_yaml() -> bytes:
        return pkg_resources.resource_string("devcluster", "example.yaml")


@contextlib.contextmanager
def lockfile(filename: str) -> typing.Iterator[None]:
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


def get_host_addr_for_docker() -> typing.Optional[str]:
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
        groups: typing.Sequence[str] = matches.groups()
        if len(groups) != 0:
            return groups[0]

    return None


def maybe_install_default_config() -> typing.Optional[str]:
    # Don't bother asking the user if the user isn't able to tell us.
    if not sys.stdin.isatty():
        return None

    # Suggest ~/.devcluster.yaml over appdirs.user_config_dir() in order to nudge dev environments
    # towards uniformity.
    if "HOME" not in os.environ:
        return None
    path = os.path.join(os.environ["HOME"], ".devcluster.yaml")

    prompt = f"config not found, install a default config at {path}? [y]/n: "
    resp = input(prompt)
    while resp not in ("y", "n", ""):
        print("invalid input; type 'y' or 'n'", file=sys.stderr)
        resp = input(prompt)

    if resp == "n":
        # user declined
        return None

    with open(path, "wb") as f:
        f.write(_get_example_yaml())
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", dest="config", action="store")
    parser.add_argument("-1", "--oneshot", dest="oneshot", action="store_true")
    parser.add_argument("-q", "--quiet", dest="quiet", action="store_true")
    parser.add_argument("-C", "--cwd", dest="cwd", action="store")
    parser.add_argument("--no-guess-host", dest="no_guess_host", action="store_true")
    parser.add_argument("-l", "--listen", dest="listen", action="store_true")
    parser.add_argument("--target-stage", type=str)
    parser.add_argument("addr", nargs="*")
    args = parser.parse_args()

    if args.listen or len(args.addr) == 0:
        # --listen was set explicitly or no addresses were set for the client
        mode = "server"
    else:
        mode = "client"

    # Validate args
    ok = True
    if mode == "client":
        if len(args.addr) != 1:
            print(
                "in client mode, devcluster requires exactly one positional address argument",
                file=sys.stderr,
            )
            ok = False
        if args.oneshot:
            print("--oneshot is not supported in client mode", file=sys.stderr)
            ok = False
        if args.quiet:
            print("--quiet is not supported in client mode", file=sys.stderr)
            ok = False
        if args.no_guess_host:
            print("--no-guess-host is not supported in client mode", file=sys.stderr)
            ok = False
        if args.config:
            print("--config is not supported in client mode", file=sys.stderr)
            ok = False
    if mode == "server":
        if args.oneshot and args.quiet:
            print("--oneshot and --quiet don't make sense together", file=sys.stderr)
            ok = False
        if args.oneshot and args.addr:
            print("--oneshot requires that no addresses are provided", file=sys.stderr)

    if not ok:
        sys.exit(1)

    # Read config before the cwd.
    if args.config is not None:
        config_path = args.config
    else:
        check_paths = []
        # Always support ~/.devcluster.yaml
        if "HOME" in os.environ:
            check_paths.append(os.path.join(os.environ["HOME"], ".devcluster.yaml"))
        # Also support the OS's standard config path.
        check_paths.append(os.path.join(appdirs.user_config_dir(), "devcluster.yaml"))
        for path in check_paths:
            if os.path.exists(path):
                config_path = path
                break
        else:
            config_path = maybe_install_default_config()
            if config_path is None:
                print(
                    "you must either specify --config or put a config file at",
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
                "If you do not need $DOCKER_LOCALHOST in your devcluster config.\n"
                "You can disable the check by passing --no-guess-host or by\n"
                "setting the DOCKER_LOCALHOST environment variable yourself.",
                file=sys.stderr,
            )
            sys.exit(1)
        env["DOCKER_LOCALHOST"] = docker_localhost

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

    if mode == "server":
        os.makedirs(config.temp_dir, exist_ok=True)
        with lockfile(os.path.join(config.temp_dir, "lock")):
            if not args.oneshot:
                args.addr = [os.path.join(config.temp_dir, "sock")] + args.addr
            server = dc.Server(
                config,
                args.addr,
                quiet=args.quiet,
                oneshot=args.oneshot,
                initial_target_stage=args.target_stage,
            )
            server.run()
    elif mode == "client":
        client = dc.ConsoleClient(args.addr[0])
        client.run()


if __name__ == "__main__":
    main()
