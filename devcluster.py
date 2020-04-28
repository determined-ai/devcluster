import abc
import enum
import time
import sys
from typing import List
import select
import textwrap
import os
import termios
import contextlib
import threading
import socket
import yaml

import subprocess
import fcntl

import argparse


@contextlib.contextmanager
def terminal_config():
    fd = sys.stdin.fileno()
    # old and new are of the form [iflag, oflag, cflag, lflag, ispeed, ospeed, cc]
    old = termios.tcgetattr(fd)
    new = termios.tcgetattr(fd)

    # raw terminal settings from `man 3 termios`
    new[0] = new[0] & ~(termios.IGNBRK | termios.BRKINT | termios.PARMRK
                      | termios.ISTRIP | termios.INLCR | termios.IGNCR
                      | termios.ICRNL | termios.IXON);
    # new[1] = new[1] & ~termios.OPOST;
    new[2] = new[2] & ~(termios.CSIZE | termios.PARENB);
    new[2] = new[2] | termios.CS8;
    new[3] = new[3] & ~(termios.ECHO | termios.ECHONL | termios.ICANON
                      | termios.ISIG | termios.IEXTEN);

    try:
        # enable alternate screen buffer
        os.write(sys.stdout.fileno(), b'\x1b[?1049h')
        # make the terminal raw
        termios.tcsetattr(fd, termios.TCSADRAIN, new)
        yield
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
        # disable alternate screen buffer
        os.write(sys.stdout.fileno(), b'\x1b[?1049l')


def invert_colors(msg):
    return b'\x1b[7m' + msg + b'\x1b[0m'


_gopath = None
def get_gopath():
    global _gopath
    if _gopath is None:
        p = subprocess.Popen(["go", "env", "GOPATH"], stdout=subprocess.PIPE)
        _gopath = p.stdout.read().strip().decode("utf8")
        assert p.wait() == 0
    return _gopath



def asbytes(msg):
    if isinstance(msg, bytes):
        return msg
    return msg.encode('utf8')


def nonblock(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


class Poll:
    IN_FLAGS = select.POLLIN | select.POLLPRI
    ERR_FLAGS = select.POLLERR | select.POLLHUP | select.POLLNVAL

    def __init__(self):
        self.handlers = {}
        self.fds = {}
        self._poll = select.poll()

    def register(self, fd, flags, handler):
        self._poll.register(fd, flags)
        self.handlers[fd] = handler
        self.fds[handler] = fd

    def unregister(self, handler):
        fd = self.fds[handler]
        self._poll.unregister(fd)
        del self.fds[handler]
        del self.handlers[fd]

    def poll(self, *args, **kwargs):
        ready = self._poll.poll()
        for fd, ev in ready:
            handler = self.handlers[fd]
            handler(ev)


class AtomicOperation:
    """
    Only have one atomic operation in flight at a time.  You must wait for it to finish but you may
    request it ends early if you know you will ignore its output.

    An example would be a connector which is trying to connect to the master binary, except if the
    master binary has already exited, we will want to exit the connector.
    """

    @abc.abstractmethod
    def __str__(self):
        """Return a one-word summary of what the operation is"""
        pass

    @abc.abstractmethod
    def cancel(self):
        pass

    @abc.abstractmethod
    def join(self):
        pass


class ConnCheck(threading.Thread):
    """ConnCheck is an AtomicOperation."""

    def __init__(self, host, port, report_fd):
        self.host = host
        self.port = port
        self.report_fd = report_fd
        self.quit = False

        super().__init__()

        # AtomicOperations should not need a start() call.
        self.start()

    def __str__(self):
        return "connecting"

    def run(self):
        success = False
        try:
            # 30 seconds to succeed
            deadline = time.time() + 30
            while time.time() < deadline:
                if self.quit:
                    break
                s = socket.socket()
                try:
                    # try every 20ms
                    waittime = time.time() + 0.02
                    s.settimeout(0.02)
                    s.connect((self.host, self.port))
                except (socket.timeout, ConnectionError) as _:
                    now = time.time()
                    if now < waittime:
                        time.sleep(waittime - now)
                    continue
                s.close()
                success = True
                break
        finally:
            os.write(self.report_fd, b'success' if success else b'FAIL')

    def cancel(self):
        self.quit = True


class AtomicSubprocess(AtomicOperation):
    def __init__(self, poll, log, report_fd, cmd):
        self.poll = poll
        self.log = log
        self.report_fd = report_fd

        self.start_time = time.time()

        self.dying = False
        self.proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.out = self.proc.stdout.fileno()
        self.err = self.proc.stderr.fileno()

        nonblock(self.out)
        nonblock(self.err)

        self.poll.register(self.out, Poll.IN_FLAGS, self._handle_out)
        self.poll.register(self.err, Poll.IN_FLAGS, self._handle_err)

    def __str__(self):
        return "building"

    def _maybe_wait(self):
        """Only respond after both stdout and stderr have closed."""
        if self.out is None and self.err is None:
            ret = self.proc.wait()
            self.proc = None
            success = False

            if self.dying:
                self.log(" ----- {self} canceled -----\n")
            elif ret != 0:
                self.log(f" ----- {self} exited with {ret} -----\n")
            else:
                build_time = time.time() - self.start_time
                self.log(" ----- {self} complete! (%.2fs) -----\n"%(build_time))
                success = True

            os.write(self.report_fd, b'SUCCESS' if success else b'FAIL')

    def _handle_out(self, ev):
        if ev & Poll.IN_FLAGS:
            self.log(os.read(self.out, 4096))
        if ev & Poll.ERR_FLAGS:
            self.poll.unregister(self._handle_out)
            os.close(self.out)
            self.out = None
            self._maybe_wait()

    def _handle_err(self, ev):
        if ev & Poll.IN_FLAGS:
            self.log(os.read(self.err, 4096))
        if ev & Poll.ERR_FLAGS:
            self.poll.unregister(self._handle_err)
            os.close(self.err)
            self.err = None
            self._maybe_wait()

    def cancel(self):
        self.dying = True
        self.proc.kill()

    def join(self):
        pass


class Stage:
    @abc.abstractmethod
    def get_precommand(self):
        pass

    @abc.abstractmethod
    def run_command(self):
        pass

    @abc.abstractmethod
    def running(self):
        pass

    @abc.abstractmethod
    def kill(self):
        pass

    @abc.abstractmethod
    def get_precommand(self):
        """Return the next AtomicOperation or None, at which point it is safe to run_command)."""
        pass

    @abc.abstractmethod
    def get_postcommand(self):
        """Return the next AtomicOperation or None, at which point the command is up."""
        pass

    @abc.abstractmethod
    def log_name(self):
        """return the name of this stage, it must be unique"""
        pass


class DeadStage(Stage):
    """A noop stage for the base state of the state machine"""

    def __init__(self, state_machine):
        self.state_machine = state_machine
        self._running = False

    def get_precommand(self):
        return None

    def run_command(self):
        self._running = True

    def running(self):
        return self._running

    def kill(self):
        self._running = False
        self.state_machine.next_thing()

    def get_precommand(self):
        pass

    def get_postcommand(self):
        pass

    def log_name(self):
        return "dead"


class Process(Stage):
    """
    A long-running process may have precommands to run first and postcommands before it is ready.
    """
    def __init__(self, config, poll, logger, state_machine):
        self.proc = None
        self.out = None
        self.err = None
        self.dying = False

        self.config = config
        self.poll = poll
        self.log = logger.log
        self.state_machine = state_machine

        self._reset()

    def _maybe_wait(self):
        """wait() on proc if both stdout and stderr are empty."""
        if not self.dying:
            self.log(f"{self.log_name()} closing unexpectedly!\n")
            # TODO: don't always go to dead state
            self.state_machine.set_target(0)

        if self.out is None and self.err is None:
            ret = self.proc.wait()
            self.log(f"{self.log_name()} exited with {ret}\n")
            self.log(f" ----- {self.log_name()} exited with {ret} -----\n", self.log_name())
            self.proc = None
            self._reset()
            self.state_machine.next_thing()

    def _handle_out(self, ev):
        if ev & Poll.IN_FLAGS:
            self.log(os.read(self.out, 4096), self.log_name())
        if ev & Poll.ERR_FLAGS:
            self.poll.unregister(self._handle_out)
            os.close(self.out)
            self.out = None
            self._maybe_wait()

    def _handle_err(self, ev):
        if ev & Poll.IN_FLAGS:
            self.log(os.read(self.err, 4096), self.log_name())
        if ev & Poll.ERR_FLAGS:
            self.poll.unregister(self._handle_err)
            os.close(self.err)
            self.err = None
            self._maybe_wait()

    def get_precommand(self):
        if self.precmds_run < len(self.config.pre):
            precmd_config = self.config.pre[self.precmds_run]
            self.precmds_run += 1
            return precmd_config.build_atomic(
                self.poll, self.log, self.state_machine.get_report_fd()
            )
        return None

    def get_postcommand(self):
        if self.postcmds_run < len(self.config.post):
            postcmd_config = self.config.post[self.postcmds_run]
            self.postcmds_run += 1
            return postcmd_config.build_atomic(
                self.poll, self.log, self.state_machine.get_report_fd()
            )
        return None

    def run_command(self):
        self.dying = False
        self.proc = subprocess.Popen(
            self.config.cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.out = self.proc.stdout.fileno()
        self.err = self.proc.stderr.fileno()

        nonblock(self.out)
        nonblock(self.err)

        self.poll.register(self.out, Poll.IN_FLAGS, self._handle_out)
        self.poll.register(self.err, Poll.IN_FLAGS, self._handle_err)

    def running(self):
        return self.proc is not None

    def kill(self):
        if len(self.config.kill) == 0:
            # kill via signal
            self.dying = True
            self.proc.kill()
        else:
            # kill via command (mainly for docker containers)
            self.dying = True
            p = subprocess.Popen(
                self.config.kill,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            err = p.stderr.read()
            ret = p.wait()
            if ret != 0:
                kill_str = " ".join(self.config.kill)
                self.log(asbytes(f"\"{kill_str}\" says:\n") + asbytes(err))

    def log_name(self):
        return self.config.name

    def _reset(self):
        self.precmds_run = 0
        self.postcmds_run = 0


class Logger:
    def __init__(self, streams):
        self.streams = {stream: b"" for stream in streams}
        self.streams["console"] = b""
        self.callbacks = []

    def log(self, msg, stream="console"):
        """Append to a log stream."""

        msg = asbytes(msg)
        self.streams[stream] += msg

        for cb in self.callbacks:
            cb(msg, stream)

    def add_callback(self, cb):
        self.callbacks.append(cb)


class StateMachine:
    def __init__(self, logger, poll):
        self.logger = logger

        self.quitting = False

        # atomic_op is intermediate steps like calling `make` or connecting to a server.
        # We only support having one run at a time (since they're atomic...)
        self.atomic_op = None

        # the pipe is used by the atomic_op to pass messages to the poll loop
        self.pipe_rd, self.pipe_wr = os.pipe2(os.O_CLOEXEC | os.O_NONBLOCK)
        poll.register(self.pipe_rd, Poll.IN_FLAGS, self.handle_pipe)

        self.stages = [DeadStage(self)]
        self.state = 0
        self.target = 0

        self.old_status = (self.state, self.atomic_op, self.target)

        self.callbacks = []

    def get_report_fd(self):
        return self.pipe_wr

    def add_stage(self, stage):
        self.stages.append(stage)

    def add_callback(self, cb):
        self.callbacks.append(cb)

    def advance_stage(self):
        """
        Either:
          - start the next precommand,
          - start the real command (and maybe a postcommand), or
          - start the next postcommand
        """

        # nothing to do in the dead state
        if self.state < 0:
            return

        process = self.stages[self.state]
        # Is there another precommand?
        atomic_op = process.get_precommand()
        if atomic_op is not None:
            self.atomic_op = atomic_op
            return

        if not process.running():
            process.run_command()

        atomic_op = process.get_postcommand()
        if atomic_op is not None:
            self.atomic_op = atomic_op
            return

    def next_thing(self):
        """
        Should be called either when:
          - a new transition is set
          - an atomic operation completes
          - a long-running process is closed
        """

        # Possibly further some atomic operations in the current state.
        if self.state == self.target:
            self.advance_stage()

        # Advance state?
        elif self.state < self.target:
            # Wait for the atomic operation to finish.
            if self.atomic_op is not None:
                pass
            else:
                self.advance_stage()
                if self.atomic_op is None:
                    self.transition(self.state + 1)

        # Regress state.
        elif self.state > self.target:
            # Cancel any atomic operations first.
            if self.atomic_op is not None:
                self.atomic_op.cancel()
            else:
                if self.stages[self.state].running():
                    self.stages[self.state].kill()
                else:
                    self.transition(self.state - 1)

        # Notify changes of state.
        new_status = (self.state, self.atomic_op, self.target)
        if self.old_status != new_status:
            state_str = self.stages[self.state].log_name().upper()
            atomic_str = str(self.atomic_op) if self.atomic_op else ""
            target_str = self.stages[self.target].log_name().upper()
            for cb in self.callbacks:
                cb(state_str, atomic_str, target_str)

            self.old_status = new_status

    def set_target(self, target):
        """For when you choose a new target state."""
        if target == self.target:
            return

        self.target = target

        self.next_thing()

    def quit(self):
        # Raise an error on the second try.
        if self.quitting:
            raise ValueError("quitting forcibly")
        # Exit gracefully on the first try.
        self.logger.log("quitting...\n")
        self.quitting = True
        self.set_target(0)

    def transition(self, new_state):
        """For when you arrive at a new state."""
        self.state = new_state

        self.next_thing()

    def handle_pipe(self, ev):
        self.atomic_op.join()
        self.atomic_op = None

        if ev & Poll.IN_FLAGS:
            msg = os.read(self.pipe_rd, 4096).decode("utf8")
            if msg == "FAIL":
                # set the target state to be one less than wherever-we-are
                self.target = min(self.state - 1, self.target)

            self.next_thing()

        if ev & Poll.ERR_FLAGS:
            # Just die.
            raise ValueError("pipe failed!")

    def should_run(self):
        return not (self.quitting and self.state == 0)


class Console:
    def __init__(self, logger, poll, state_machine):
        self.logger = logger
        self.logger.add_callback(self.log_cb)

        self.poll = poll
        self.poll.register(sys.stdin.fileno(), Poll.IN_FLAGS, self.handle_stdin)

        self.state_machine = state_machine
        state_machine.add_callback(self.state_cb)

        self.state_msg = b"uninitialized"

        self.active_stream = ""

        # set_active_stream will trigger the print_bar() operation
        self.set_active_stream("master")

    def set_active_stream(self, new_stream):
        if new_stream == self.active_stream:
            return

        self.erase_screen()
        self.place_cursor(2, 1)
        # dump logs
        os.write(sys.stdout.fileno(), self.logger.streams[new_stream][-16*1024:])
        self.print_bar()

        self.active_stream = new_stream

    def log_cb(self, msg, stream):
        if self.active_stream == stream:
            os.write(sys.stdout.fileno(), msg)
            self.print_bar()

    def state_cb(self, state, substate, target):
        state_msg = state + (f"({substate})" if substate else "")
        msg = f"state:{state_msg} target:{target} stream:{self.active_stream}".encode("utf8")
        self.state_msg = msg
        self.print_bar()

    def handle_stdin(self, ev):
        c = sys.stdin.read(1)
        if c == '\x03':
            self.state_machine.quit()
        elif c == "q":
            self.state_machine.quit()
        elif c == "d":
            self.set_active_stream("db")
        elif c == "h":
            self.set_active_stream("hasura")
        elif c == "m":
            self.set_active_stream("master")
        elif c == "a":
            self.set_active_stream("agent")
        elif c == "c":
            self.set_active_stream("console")
        elif c == "0":
            self.state_machine.set_target(0)
            self.logger.log("set target dead!\n")
        elif c == "1":
            self.state_machine.set_target(1)
            self.logger.log("set target db!\n")
        elif c == "2":
            self.state_machine.set_target(2)
            self.logger.log("set target hasura!\n")
        elif c == "3":
            self.state_machine.set_target(3)
            self.logger.log("set target master!\n")
        elif c == "4":
            self.state_machine.set_target(4)
            self.logger.log("set target agent!\n")

    def get_cursor_pos(self):
        os.write(1, b'\x1b[6n')
        buf = b''
        while True:
            buf += os.read(sys.stdin.fileno(), 1)
            if b'R' in buf:
                break
        esc = buf.index(b'\x1b')
        semi = buf.index(b';')
        R = buf.index(b'R')
        return buf[:esc], int(buf[esc+2:semi]), int(buf[semi+1:R])

    def place_cursor(self, row, col):
        os.write(sys.stdout.fileno(), b'\x1b[%d;%dH'%(row, col))

    def erase_line(self):
        os.write(sys.stdout.fileno(), b'\x1b[2K')

    def erase_screen(self):
        os.write(sys.stdout.fileno(), b'\x1b[2J')

    def print_bar(self):
        _, row, col = self.get_cursor_pos()
        self.place_cursor(1, 1)
        self.erase_line()
        os.write(sys.stdout.fileno(), invert_colors(self.state_msg))
        self.place_cursor(row, col)


def check_keys(allowed, required, config, name):
    extra = set(config.keys()).difference(allowed)
    assert len(extra) == 0, f"invalid keys for {name}: {extra}"
    missing = required.difference(set(config.keys()))
    assert len(missing) == 0, f"missing required keys for {name}: {missing}"


def check_list_of_strings(l, msg):
    assert isinstance(l, list), msg
    for s in l:
        assert isinstance(s, str), msg


def check_list_of_dicts(l, msg):
    assert isinstance(l, list), msg
    for s in l:
        assert isinstance(s, dict), msg


class StageConfig:
    @staticmethod
    def read(config):
        allowed = {"db", "master", "agent", "custom"}
        required = set()

        assert isinstance(config, dict), "StageConfig must be a dictionary with a single key"
        assert len(config), "StageConfig must be a dictionary with a single key"
        typ, val = next(iter(config.items()))
        assert typ in allowed, f"{typ} is not one of {allowed}"

        if typ == "custom":
            return CustomConfig(val)
        elif typ == "db":
            return DBConfig(val)
        elif typ == "master":
            return MasterConfig(val)
        elif typ == "agent":
            return AgentConfig(val)

    @abc.abstractmethod
    def build_stage():
        pass


class AtomicConfig:
    @staticmethod
    def read(config):
        allowed = {"custom", "conncheck"}
        required = set()

        assert isinstance(config, dict), "AtomicConfig must be a dictionary with a single key"
        assert len(config), "AtomicConfig must be a dictionary with a single key"
        typ, val = next(iter(config.items()))
        assert typ in allowed, f"{typ} is not one of {allowed}"

        if typ == "custom":
            return CustomAtomicConfig(val)
        elif typ == "conncheck":
            return ConnCheckConfig(val)

    @abc.abstractmethod
    def build_atomic(self, poll, log, report_fd):
        pass


class DBConfig(StageConfig):
    """DBConfig is a canned stage that runs the database in docker"""

    def __init__(self, config):
        allowed = {"port", "password", "db_name", "data_dir", "container_name"}
        required = set()
        check_keys(allowed, required, config, type(self).__name__)

        self.port = int(config.get("port", 5432))
        self.password = str(config.get("password", "postgres"))
        self.db_name = str(config.get("db_name", "determined"))
        self.container_name = str(config.get("container_name", "determined_db"))
        self.data_dir = config.get("data_dir")
        self.name = "db"

    def build_stage(self, poll, logger, state_machine):
        cmd = [
            "docker",
            "run",
            "--rm",
        ]

        if self.data_dir:
            cmd += ["-v", f"{self.data_dir}:/var/lib/postgresql/data"]

        cmd += [
            "-e",
            f"POSTGRES_DB={self.db_name}",
            "-e",
            f"POSTGRES_PASSWORD={self.password}",
            "-p",
            f"{self.port}:5432",
            "--name",
            self.container_name,
            "postgres:10.7",
            "-N",
            "10000",
        ]

        custom_config = CustomConfig({
            "cmd": cmd,
            "name": "db",
            "post": [{"conncheck": {"port": self.port}}],
            "kill": ["docker", "kill", self.container_name],
        })

        return Process(custom_config, poll, logger, state_machine)


class MasterConfig(StageConfig):
    def __init__(self, config):
        self.config = config
        self.name = "master"

    def build_stage(self, poll, logger, state_machine):
        config_path = "/tmp/devcluster-master.conf"
        with open(config_path, "w") as f:
            f.write(yaml.dump(self.config))

        cmd = [
            os.path.join(get_gopath(), "bin", "determined-master"),
            "--config-file",
            config_path,
            "--root",
            "build/share/determined/master",
        ]

        custom_config = CustomConfig({
            "cmd": cmd,
            "name": "master",
            "pre": [{"custom": ["make", "-C", "master", "build-files", "install-native"]}],
            # TODO: don't hardcode 8080
            "post": [{"conncheck": {"port": 8080}}],
        })

        return Process(custom_config, poll, logger, state_machine)


class AgentConfig(StageConfig):
    def __init__(self, config):
        self.config = config or {"master_host": "localhost", "master_port": 8080}
        self.name = "agent"

    def build_stage(self, poll, logger, state_machine):
        config_path = "/tmp/devcluster-agent.conf"
        with open(config_path, "w") as f:
            f.write(yaml.dump(self.config))

        cmd = [
            os.path.join(get_gopath(), "bin", "determined-agent"),
            "run",
            "--config-file",
            config_path,
        ]

        custom_config = CustomConfig({
            "cmd": cmd,
            "name": "agent",
            "pre": [{"custom": ["make", "-C", "agent", "install-native"]}],
        })

        return Process(custom_config, poll, logger, state_machine)


class ConnCheckConfig:
    def __init__(self, config):
        allowed = {"host", "port"}
        required = {"port"}
        check_keys(allowed, required, config, type(self).__name__)

        self.host = config.get("host", "localhost")
        self.port = config["port"]

    def build_atomic(self, poll, log, report_fd):
        return ConnCheck(self.host, self.port, report_fd)


class CustomAtomicConfig:
    def __init__(self, config):
        check_list_of_strings(config, "AtomicConfig.custom must be a list of strings")
        self.cmd = config

    def build_atomic(self, poll, log, report_fd):
        return AtomicSubprocess(poll, log, report_fd, self.cmd)


class CustomConfig(StageConfig):
    def __init__(self, config):
        allowed = {"cmd", "name", "pre", "post", "kill"}
        required = {"cmd", "name"}

        check_keys(allowed, required, config, type(self).__name__)

        self.cmd = config["cmd"]
        check_list_of_strings(self.cmd, "CustomConfig.cmd must be a list of strings")

        self.name = config["name"]
        assert isinstance(self.name, str), "CustomConfig.name msut be a string"

        self.kill = config.get("kill", [])
        check_list_of_strings(self.kill, "CustomConfig.kill must be a list of strings")

        check_list_of_dicts(config.get("pre", []), "CustomConfig.pre must be a list of dicts")
        self.pre = [AtomicConfig.read(pre) for pre in config.get("pre", [])]

        check_list_of_dicts(config.get("post", []), "CustomConfig.post must be a list of dicts")
        self.post = [AtomicConfig.read(post) for post in config.get("post", [])]

    def build_stage(self, poll, logger, state_machine):
        return Process(self, poll, logger, state_machine)


class Config:
    def __init__(self, config):
        allowed = {"stages"}
        required = {"stages"}
        check_keys(allowed, required, config, type(self).__name__)

        check_list_of_dicts(config["stages"], "stages must be a list of dicts")
        self.stages = [StageConfig.read(stage) for stage in config["stages"]]


def main(config):

    with terminal_config():
        poll = Poll()

        stage_names = [stage_config.name for stage_config in config.stages]

        logger = Logger(stage_names)

        state_machine = StateMachine(logger, poll)

        console = Console(logger, poll, state_machine)

        for stage_config in config.stages:
            state_machine.add_stage(stage_config.build_stage(poll, logger, state_machine))

        state_machine.set_target(len(stage_names))

        while state_machine.should_run():
            poll.poll()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', dest='config', action='store')
    args = parser.parse_args()

    # Read config before the chdir()
    if args.config is not None:
        with open(args.config) as f:
            config = Config(yaml.safe_load(f.read()))
    else:
        default_path = os.path.expanduser("~/.devcluster.yaml")
        if not os.path.exists(default_path):
            print("you must either specify --config or have a {default_path}", file=sys.stderr)
        with open(default_path) as f:
            config = Config(yaml.safe_load(f.read()))

    get_gopath()

    if "DET_ROOT" not in os.environ:
        print("you must specify the DET_ROOT environment variable", file=sys.stderr)
        sys.exit(1)
    os.chdir(os.environ["DET_ROOT"])

    main(config)
