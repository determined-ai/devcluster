import json
import os
import subprocess
import typing

import devcluster as dc


class ProcessTracker:
    """
    ProcessTracker simply logs to a file which processes are running (both docker and native
    processes) and offers the ability to search through the log from an old devcluster invocation
    and kill any dangling processes that can still be found.

    The global filelock that devcluster uses protects users from strange errors if they try to
    call devcluster while another devcluster process is still alive.
    """

    def __init__(self, temp_dir: str) -> None:
        self.temp_dir = temp_dir
        # Process list is ordered so that when we recover a crashed devcluster,
        # we can clean up the processes in reverse order of when they started.
        self.running = []  # type: typing.Any

    def _update_file(self) -> None:
        path = os.path.join(self.temp_dir, "running.json")
        tmp_path = path + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(self.running, f)
        os.rename(tmp_path, path)

    def report_container_started(self, container_id: str) -> None:
        self.running.append({"container_id": container_id})
        self._update_file()

    def report_container_killed(self, container_id: str) -> None:
        self.running = list(
            filter(lambda p: p.get("container_id") != container_id, self.running)
        )
        self._update_file()

    def report_pid_started(self, pid: int, match_args: str) -> None:
        self.running.append({"pid": pid, "match_args": match_args})
        self._update_file()

    def report_pid_killed(self, pid: int) -> None:
        self.running = list(filter(lambda p: p.get("pid") != pid, self.running))
        self._update_file()

    def recover(self, logger: "dc.Logger") -> None:
        path = os.path.join(self.temp_dir, "running.json")
        if not os.path.exists(path):
            return
        with open(path) as f:
            old_processes = json.load(f)

        for proc in reversed(old_processes):
            if "pid" in proc:
                msg = recover_process(proc["pid"], proc["match_args"])
                if msg:
                    logger.log(msg)
            if "container_id" in proc:
                msg = recover_container(proc["container_id"])
                if msg:
                    logger.log(msg)


def recover_process(pid: int, match_args: str) -> typing.Optional[str]:
    """Detect/kill a dangling process from an earlier devluster."""
    # ps args, formatting, and exit code all tested on mac and linux
    cmd = ["ps", "-p", str(pid), "-o", "command"]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    assert p.stdout is not None
    if p.returncode != 0:
        # No such pid found
        return None

    # Ignore the header; there's no cross-platform way to not print it.
    command_found = p.stdout.splitlines()[1].strip().decode("utf8")
    if not command_found.startswith(match_args):
        # The pid matches but the args do not.  Better not to kill it.
        return f"chose not to kill pid {pid} whose args don't match '{match_args}'\n"

    # Kill the process.
    os.kill(pid, 9)
    return f"killed old pid {pid} running '{match_args}'\n"


def recover_container(container_id: str) -> typing.Optional[str]:
    """Detect/kill a dangling container from an earlier devcluster."""
    cmd = [
        "docker",
        "ps",
        "-a",
        "--filter",
        f"id={container_id}",
        "--format",
        "{{.State}},{{.Names}}",
    ]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    assert p.stdout is not None
    out = p.stdout.strip().decode("utf8")

    if not out:
        # Container not found.
        return None

    state, name = out.split(",", 1)

    if state not in ("created", "exited"):
        # Kill container.
        cmd = ["docker", "kill", container_id]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Wait for it to exit.
        cmd = ["docker", "wait", container_id]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Remove the container.
    cmd = ["docker", "rm", container_id]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return f"killed old docker container {name}\n"
