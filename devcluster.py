#!/usr/bin/env python3

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
import re
import signal

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


_gopath = None

def get_gopath():
    global _gopath
    if _gopath is None:
        p = subprocess.Popen(["go", "env", "GOPATH"], stdout=subprocess.PIPE)
        _gopath = p.stdout.read().strip().decode("utf8")
        assert p.wait() == 0
    return _gopath


_save_cursor = None

def save_cursor():
    global _save_cursor
    if _save_cursor is None:
        p = subprocess.Popen(["tput", "sc"], stdout=subprocess.PIPE)
        _save_cursor = p.stdout.read().strip()
        assert p.wait() == 0
    return _save_cursor


_restore_cursor = None

def restore_cursor():
    global _restore_cursor
    if _restore_cursor is None:
        p = subprocess.Popen(["tput", "rc"], stdout=subprocess.PIPE)
        _restore_cursor = p.stdout.read().strip()
        assert p.wait() == 0
    return _restore_cursor


_cols = None

def get_cols(recheck=False):
    global _cols
    if _cols is None or recheck:
        p = subprocess.Popen(["tput", "cols"], stdout=subprocess.PIPE)
        _cols = int(p.stdout.read().strip())
        assert p.wait() == 0
    return _cols


_rows = None

def get_rows(recheck=False):
    global _rows
    if _rows is None or recheck:
        p = subprocess.Popen(["tput", "lines"], stdout=subprocess.PIPE)
        _rows = int(p.stdout.read().strip())
        assert p.wait() == 0
    return _rows


def asbytes(msg):
    if isinstance(msg, bytes):
        return msg
    return msg.encode('utf8')


def nonblock(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK | os.O_CLOEXEC)


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
            # "S"uccess or "F"ail
            os.write(self.report_fd, b'S' if success else b'F')

    def cancel(self):
        self.quit = True


class LogCheck(AtomicOperation):
    """
    Wait for a log stream to print out a phrase before allowing the state to progress.
    """
    def __init__(self, logger, stream, report_fd, regex):
        self.logger = logger
        self.stream = stream
        self.report_fd = report_fd

        self.pattern = re.compile(asbytes(regex))

        self.canceled = False

        self.logger.add_callback(self.log_cb)

    def __str__(self):
        return "checking"

    def cancel(self):
        if not self.canceled:
            self.canceled = True
            os.write(self.report_fd, b'F')
            self.logger.remove_callback(self.log_cb)

    def join(self):
        pass

    def log_cb(self, msg, stream):
        if stream != self.stream:
            return

        if len(self.pattern.findall(msg)) == 0:
            return

        os.write(self.report_fd, b'S')
        self.logger.remove_callback(self.log_cb)


class AtomicSubprocess(AtomicOperation):
    def __init__(self, poll, logger, stream, report_fd, cmd, quiet=False):
        self.poll = poll
        self.logger = logger
        self.stream = stream
        self.report_fd = report_fd

        self.start_time = time.time()

        self.dying = False
        self.proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL if quiet else subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.out = None if quiet else self.proc.stdout.fileno()
        self.err = self.proc.stderr.fileno()

        if not quiet:
            nonblock(self.out)
        nonblock(self.err)

        if not quiet:
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
                self.logger.log(f" ----- {self} canceled -----\n", self.stream)
            elif ret != 0:
                self.logger.log(f" ----- {self} exited with {ret} -----\n", self.stream)
            else:
                build_time = time.time() - self.start_time
                self.logger.log(f" ----- {self} complete! (%.2fs) -----\n"%(build_time), self.stream)
                success = True

            os.write(self.report_fd, b'S' if success else b'F')

    def _handle_out(self, ev):
        if ev & Poll.IN_FLAGS:
            self.logger.log(os.read(self.out, 4096), self.stream)
        if ev & Poll.ERR_FLAGS:
            self.poll.unregister(self._handle_out)
            os.close(self.out)
            self.out = None
            self._maybe_wait()

    def _handle_err(self, ev):
        if ev & Poll.IN_FLAGS:
            self.logger.log(os.read(self.err, 4096), self.stream)
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


class DockerRunAtomic(AtomicSubprocess):
    def __init__(self, *args, **kwargs):
        kwargs["quiet"] = True
        super().__init__(*args, **kwargs)

    def __str__(self):
        return "starting"

    def cancel(self):
        # Don't support canceling at all; it creates a race condition where we don't know when
        # we can docker kill the container.
        pass


class Stage:
    @abc.abstractmethod
    def run_command(self):
        pass

    @abc.abstractmethod
    def running(self):
        pass

    def killable(self):
        """By default, killable() returns running().  It's more complex for docker"""
        return self.running()

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
        self.logger = logger
        self.state_machine = state_machine

        self._reset()

    def wait(self):
        return self.proc.wait()

    def _maybe_wait(self):
        """wait() on proc if both stdout and stderr are empty."""
        if not self.dying:
            self.logger.log(f"{self.log_name()} closing unexpectedly!\n")
            # TODO: don't always go to dead state
            self.state_machine.set_target(0)

        if self.out is None and self.err is None:
            ret = self.wait()
            self.logger.log(f"{self.log_name()} exited with {ret}\n")
            self.logger.log(f" ----- {self.log_name()} exited with {ret} -----\n", self.log_name())
            self.proc = None
            self._reset()
            self.state_machine.next_thing()

    def _handle_out(self, ev):
        if ev & Poll.IN_FLAGS:
            self.logger.log(os.read(self.out, 4096), self.log_name())
        if ev & Poll.ERR_FLAGS:
            self.poll.unregister(self._handle_out)
            os.close(self.out)
            self.out = None
            self._maybe_wait()

    def _handle_err(self, ev):
        if ev & Poll.IN_FLAGS:
            self.logger.log(os.read(self.err, 4096), self.log_name())
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
                self.poll, self.logger, self.log_name(), self.state_machine.get_report_fd()
            )
        return None

    def get_postcommand(self):
        if self.postcmds_run < len(self.config.post):
            postcmd_config = self.config.post[self.postcmds_run]
            self.postcmds_run += 1

            return postcmd_config.build_atomic(
                self.poll, self.logger, self.log_name(), self.state_machine.get_report_fd()
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
        # kill via signal
        self.dying = True
        self.proc.kill()

    def log_name(self):
        return self.config.name

    def _reset(self):
        self.precmds_run = 0
        self.postcmds_run = 0


class DockerProcess(Process):
    """
    A long-running process in docker with special startup and kill semantics.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # docker run --detach has to be an AtomicOperation because it is way too slow and causes
        # the UI to hang.  This has far-reaching implications, since `running()` is not longer
        # based on the main subprocess, and you may then have to `kill()` or `wait()` while the
        # main subprocess hasn't even been launched.
        self.docker_started = False


    def get_precommand(self):
        # Inherit the precmds behavior from Process.
        precmd = super().get_precommand()
        if precmd is not None:
            return precmd

        if self.docker_started == False:
            self.docker_started = True
            # Add in a new atomic for starting the docker container.
            run_args = [
                "docker",
                "container",
                "run",
                "--detach",
                "--name",
                self.config.container_name,
                *self.config.run_args,
            ]
            return DockerRunAtomic(
                self.poll,
                self.logger,
                self.log_name(),
                self.state_machine.get_report_fd(),
                run_args,
            )

        return None

    def killable(self):
        return self.docker_started or self.proc is not None

    def run_command(self):
        self.dying = False
        self.proc = subprocess.Popen(
            ["docker", "container", "logs", "-f", self.config.container_name],
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

    def docker_wait(self):
        # Wait for the container.
        self.docker_started = False

        run_args = ["docker", "wait", self.config.container_name]

        p = subprocess.Popen(
            run_args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        err = p.stderr.read()
        ret = p.wait()
        if ret != 0:
            self.logger.log(
                asbytes(f"`docker wait` for {self.log_name()} failed with {ret} saying\n")
                + asbytes(err)
            )
            self.logger.log(
                f" ----- `docker wait` for {self.log_name()} exited with {ret} -----\n",
                self.log_name()
            )
            # We don't know what to return
            return -1

        docker_exit_val = int(p.stdout.read().strip())

        # Remove the container
        run_args = ["docker", "container", "rm", self.config.container_name]

        p = subprocess.Popen(
            run_args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        err = p.stderr.read()
        ret = p.wait()
        if ret != 0:
            self.logger.log(
                asbytes(f"`docker container rm` for {self.log_name()} failed with {ret} saying:\n")
                + asbytes(err)
            )
            self.logger.log(
                f" ----- `docker container rm` for {self.log_name()} exited with {ret} -----\n",
                self.log_name()
            )

        return docker_exit_val

    def wait(self):
        ret = self.proc.wait()
        if ret != 0:
            self.logger.log(f"`docker container logs` for {self.log_name()} exited with {ret}\n")
            self.logger.log(
                f" ----- `docker container logs` for {self.log_name()} exited with {ret} -----\n",
                self.log_name()
            )

        return self.docker_wait()


    def kill(self):
        self.dying = True
        kill_cmd = ["docker", "kill", self.config.container_name]
        p = subprocess.Popen(
            kill_cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        err = p.stderr.read()
        ret = p.wait()
        if ret != 0:
            kill_str = " ".join(kill_cmd)
            self.logger.log(asbytes(f"\"{kill_str}\" says:\n") + asbytes(err))

        # If we got canceled, immediately wait on the docker container to exit; there won't be any
        # closing file descriptors to alert us that the container has closed.
        # TODO: configure some async thing to wake up the poll loop via the report_fd so this
        # doesn't block.
        if self.proc is None:
            self.docker_wait()
            self._reset()
            self.state_machine.next_thing()


class Logger:
    def __init__(self, streams, log_dir):
        all_streams = ["console"] + streams
        self.streams = {stream: [] for stream in all_streams}
        self.index = {i: stream for i, stream in enumerate(all_streams)}
        self.callbacks = []

        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)

    def log(self, msg, stream="console"):
        """Append to a log stream."""

        now = time.time()
        msg = asbytes(msg)
        self.streams[stream].append((now, msg))

        with open(os.path.join(self.log_dir, stream), "ab") as f:
            f.write(msg)

        for cb in self.callbacks:
            cb(msg, stream)

    def add_callback(self, cb):
        self.callbacks.append(cb)

    def remove_callback(self, cb):
        self.callbacks.remove(cb)


class StateMachine:
    def __init__(self, logger, poll):
        self.logger = logger

        self.quitting = False

        # atomic_op is intermediate steps like calling `make` or connecting to a server.
        # We only support having one run at a time (since they're atomic...)
        self.atomic_op = None

        # the pipe is used by the atomic_op to pass messages to the poll loop
        self.pipe_rd, self.pipe_wr = os.pipe()
        nonblock(self.pipe_rd)
        nonblock(self.pipe_wr)
        poll.register(self.pipe_rd, Poll.IN_FLAGS, self.handle_pipe)

        self.stages = [DeadStage(self)]
        self.state = 0
        self.target = 0

        self.old_status = (self.state, self.atomic_op, self.target)

        self.callbacks = []

        self.report_callbacks = {}

    def get_report_fd(self):
        return self.pipe_wr

    def add_stage(self, stage):
        self.stages.append(stage)

    def add_callback(self, cb):
        self.callbacks.append(cb)

    def add_report_callback(self, report_msg, cb):
        self.report_callbacks[report_msg] = cb

    def advance_stage(self):
        """
        Either:
          - start the next precommand,
          - start the real command (and maybe a postcommand), or
          - start the next postcommand
        """
        # Never do anything if there is an atomic operation to finish.
        if self.atomic_op is not None:
            return

        # nothing to do in the dead state
        if self.state < 0:
            return

        process = self.stages[self.state]
        # Is there another precommand?
        atomic_op = process.get_precommand()
        if atomic_op is not None:
            self.atomic_op = atomic_op
            return

        # Launch the first postcommand immediately before launching the command, in case there
        # is a race condition (such as registering for log callbacks on the stream).
        atomic_op = process.get_postcommand()
        if atomic_op is not None:
            self.atomic_op = atomic_op

        if not process.running():
            process.run_command()

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
            self.advance_stage()
            if self.atomic_op is None:
                self.transition(self.state + 1)

        # Regress state.
        elif self.state > self.target:
            # Cancel any atomic operations first.
            if self.atomic_op is not None:
                self.atomic_op.cancel()
            else:
                if self.stages[self.state].killable():
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
        if ev & Poll.IN_FLAGS:
            msg = os.read(self.pipe_rd, 4096).decode("utf8")
            for c in msg:
                # check if we have a listener for this callback
                cb = self.report_callbacks.get(c)
                if cb is not None:
                    cb()
                    continue

                self.atomic_op.join()
                self.atomic_op = None
                if c == "F":  # Fail
                    # set the target state to be one less than wherever-we-are
                    self.target = min(self.state - 1, self.target)

                self.next_thing()
            return

        if ev & Poll.ERR_FLAGS:
            # Just die.
            raise ValueError("pipe failed!")

    def should_run(self):
        return not (self.quitting and self.state == 0)


def fore_rgb(rgb):
    return b'\x1b[38;2;%d;%d;%dm'%(rgb >> 16, (rgb & 0xff00) >> 8, rgb & 0xff)

def fore_num(num):
    return b'\x1b[38;5;%dm'%(num)

def back_rgb(rgb):
    return b'\x1b[48;2;%d;%d;%dm'%(rgb >> 16, (rgb & 0xff00) >> 8, rgb & 0xff)

def back_num(num):
    return b'\x1b[48;5;%dm'%(num)

res = b'\x1b[m'


class Console:
    def __init__(self, logger, poll, state_machine):
        self.logger = logger
        self.logger.add_callback(self.log_cb)

        self.poll = poll
        self.poll.register(sys.stdin.fileno(), Poll.IN_FLAGS, self.handle_stdin)

        self.state_machine = state_machine
        state_machine.add_callback(self.state_cb)

        self.status = ("state", "substate", "target")

        # default to all streams active
        self.active_streams = set(self.logger.streams)

        self.redraw()

    def redraw(self):
        # assume at least 1 in 2 log packets has a newline (we just need to fill up a screen)
        tail_len = get_cols() * 2

        # build new log output
        new_logs = []
        for stream in self.active_streams:
            new_logs.extend(self.logger.streams[stream][-tail_len:])
        # sort the streams chronologically
        new_logs.sort(key=lambda x: x[0])

        prebar_bytes = self.erase_screen() + self.place_cursor(3, 1)
        prebar_bytes += b"".join(x[1] for x in new_logs)
        self.print_bar(prebar_bytes)

    def handle_window_change(self):
        """This should get called some time after a SIGWINCH."""
        _ = get_cols(True)
        _ = get_rows(True)
        self.redraw()

    def set_stream(self, stream, val):
        if isinstance(stream, int):
            stream = self.logger.index[stream]

        if val is None:
            # toggle when val is None
            val = not stream in self.active_streams

        if val:
            self.active_streams.add(stream)
        else:
            self.active_streams.remove(stream)

        self.redraw()

    def log_cb(self, msg, stream):
        if stream in self.active_streams:
            self.print_bar(msg)

    def state_cb(self, state, substate, target):
        self.status = (state, substate, target)
        self.print_bar()

    def try_set_target(self, idx):
        if idx >= len(self.state_machine.stages):
            return
        self.state_machine.set_target(idx)

    def try_toggle_stream(self, idx):
        if idx not in self.logger.index:
            return
        self.set_stream(idx, None)

    def handle_key(self, key):
        if key == '\x03':
            self.state_machine.quit()
        elif key == "q":
            self.state_machine.quit()

        # 0-9: set target state
        elif key == "0" or key == "`":
            self.try_set_target(0)
        elif key == "1":
            self.try_set_target(1)
        elif key == "2":
            self.try_set_target(2)
        elif key == "3":
            self.try_set_target(3)
        elif key == "4":
            self.try_set_target(4)
        elif key == "5":
            self.try_set_target(5)
        elif key == "6":
            self.try_set_target(6)
        elif key == "7":
            self.try_set_target(7)
        elif key == "8":
            self.try_set_target(8)
        elif key == "9":
            self.try_set_target(9)

        # shift + 0-9: toggle logs
        elif key == ")" or key == "~":
            self.try_toggle_stream(0)
        elif key == "!":
            self.try_toggle_stream(1)
        elif key == "@":
            self.try_toggle_stream(2)
        elif key == "#":
            self.try_toggle_stream(3)
        elif key == "$":
            self.try_toggle_stream(4)
        elif key == "%":
            self.try_toggle_stream(5)
        elif key == "^":
            self.try_toggle_stream(6)
        elif key == "&":
            self.try_toggle_stream(7)
        elif key == "*":
            self.try_toggle_stream(8)
        elif key == "(":
            self.try_toggle_stream(9)

    def handle_stdin(self, ev):
        key = sys.stdin.read(1)
        self.handle_key(key)

    def place_cursor(self, row, col):
        return b'\x1b[%d;%dH'%(row, col)

    def erase_line(self):
        return b'\x1b[2K'

    def erase_screen(self):
        return b'\x1b[2J'

    def print_bar(self, prebar_bytes=b""):
        cols = get_cols()
        state, substate, target = self.status
        state_msg = state + (f"({substate})" if substate else "")

        blue = fore_num(202) + back_num(17)
        orange = fore_num(17) + back_num(202)

        bar1 = b"state: "
        # printable length
        bar1_len = len(bar1)
        for i, stage in enumerate(self.state_machine.stages):
            # Active stage is orange
            if stage.log_name().lower() == state.lower():
                color = orange
            else:
                color = blue

            # Target stage is donoted with <
            if stage.log_name().lower() == target.lower():
                post = b"< "
            else:
                post = b"  "

            # TODO: truncate this properly for narrow consoles
            binding = b"  (`)" if i == 0 else b"  (%d)"%i
            bar1 += binding + color + asbytes(stage.log_name().upper()) + blue + post
            bar1_len += 7 + len(stage.log_name())

        # fill bar
        bar1 += b" " * (cols - bar1_len)

        bar2 = blue + b"logs: "
        bar2_len = 6

        def get_binding(idx):
            return [
                b"(~)",
                b"(!)",
                b"(@)",
                b"(#)",
                b"($)",
                b"(%)",
                b"(^)",
                b"(&)",
                b"(*)",
                b"(()",
                b"   ",
            ][min(idx, 10)]

        def get_log_name(name):
            return "console" if name == "dead" else name

        for i, stage in enumerate(self.state_machine.stages):
            binding = get_binding(i)
            name = get_log_name(stage.log_name())

            if name in self.active_streams:
                color = orange
            else:
                color = blue

            # TODO: truncate this properly for narrow consoles
            bar2 += binding + color + asbytes(name.upper()) + blue + b"    "
            bar2_len += 7 + len(name)

        bar2 += b" " * (cols - bar2_len)

        bar_bytes = b"".join([back_num(17), fore_num(202), bar1, fore_num(7), bar2, res])

        os.write(
            sys.stdout.fileno(),
            b"".join(
                [
                    prebar_bytes,
                    save_cursor(),
                    self.place_cursor(1, 1),
                    self.erase_line(),
                    bar_bytes,
                    restore_cursor(),
                ]
            )
        )


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


def read_path(path):
    """Expand ~'s in a non-None path."""
    if path is None:
        return None
    return os.path.expanduser(path)


class StageConfig:
    @staticmethod
    def read(config):
        allowed = {"db", "master", "agent", "custom", "custom_docker"}
        required = set()

        assert isinstance(config, dict), "StageConfig must be a dictionary with a single key"
        assert len(config), "StageConfig must be a dictionary with a single key"
        typ, val = next(iter(config.items()))
        assert typ in allowed, f"{typ} is not one of {allowed}"

        if typ == "custom":
            return CustomConfig(val)
        if typ == "custom_docker":
            return CustomDockerConfig(val)
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
        allowed = {"custom", "sh", "conncheck", "logcheck"}
        required = set()

        assert isinstance(config, dict), "AtomicConfig must be a dictionary with a single key"
        assert len(config), "AtomicConfig must be a dictionary with a single key"
        typ, val = next(iter(config.items()))
        assert typ in allowed, f"{typ} is not one of {allowed}"

        if typ == "custom":
            return CustomAtomicConfig(val)
        if typ == "sh":
            return ShellAtomicConfig(val)
        elif typ == "conncheck":
            return ConnCheckConfig(val)
        elif typ == "logcheck":
            return LogCheckConfig(val)

    @abc.abstractmethod
    def build_atomic(self, poll, logger, stream, report_fd):
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
        self.data_dir = read_path(config.get("data_dir"))
        self.name = "db"

    def build_stage(self, poll, logger, state_machine):

        if self.data_dir:
            run_args = ["-v", f"{self.data_dir}:/var/lib/postgresql/data"]
        else:
            run_args = []

        run_args += [
            "-p",
            f"{self.port}:5432",
            "-e",
            f"POSTGRES_DB={self.db_name}",
            "-e",
            f"POSTGRES_PASSWORD={self.password}",
            "postgres:10.8",
            "-N",
            "10000",
        ]

        custom_config = CustomDockerConfig({
            "name": "db",
            "container_name": self.container_name,
            "run_args": run_args,
            "post": [{"logcheck": {"regex": "database system is ready to accept connections"}}],
        })

        return DockerProcess(custom_config, poll, logger, state_machine)


class MasterConfig(StageConfig):
    def __init__(self, config):
        allowed = {"pre", "binary", "config_file"}
        required = set()
        check_keys(allowed, required, config, type(self).__name__)

        self.config_file = config.get("config_file", {})

        check_list_of_dicts(config.get("pre", []), "CustomConfig.pre must be a list of dicts")
        self.pre = config.get("pre", [])

        self.binary = read_path(config.get("binary", "master/build/determined-master"))

        self.name = "master"

    def build_stage(self, poll, logger, state_machine):
        config_path = "/tmp/devcluster-master.conf"
        with open(config_path, "w") as f:
            f.write(yaml.dump(self.config_file))

        cmd = [
            self.binary,
            "--config-file",
            config_path,
        ]

        custom_config = CustomConfig({
            "cmd": cmd,
            "name": "master",
            "pre": self.pre,
            # TODO: don't hardcode 8080
            "post": [{"conncheck": {"port": 8080}}],
        })

        return Process(custom_config, poll, logger, state_machine)


class AgentConfig(StageConfig):
    def __init__(self, config):
        allowed = {"pre", "binary", "config_file"}
        required = set()
        check_keys(allowed, required, config, type(self).__name__)

        self.binary = read_path(config.get("binary", "agent/build/determined-agent"))

        self.config_file = config.get("config_file", {})

        check_list_of_dicts(config.get("pre", []), "CustomConfig.pre must be a list of dicts")
        self.pre = config.get("pre", [])

        self.name = "agent"

    def build_stage(self, poll, logger, state_machine):
        config_path = "/tmp/devcluster-agent.conf"
        with open(config_path, "w") as f:
            f.write(yaml.dump(self.config_file))

        cmd = [
            self.binary,
            "run",
            "--config-file",
            config_path,
        ]

        custom_config = CustomConfig({
            "cmd": cmd,
            "name": "agent",
            "pre": self.pre,
        })

        return Process(custom_config, poll, logger, state_machine)


class ConnCheckConfig:
    def __init__(self, config):
        allowed = {"host", "port"}
        required = {"port"}
        check_keys(allowed, required, config, type(self).__name__)

        self.host = config.get("host", "localhost")
        self.port = config["port"]

    def build_atomic(self, poll, logger, stream, report_fd):
        return ConnCheck(self.host, self.port, report_fd)


class LogCheckConfig:
    def __init__(self, config):
        allowed = {"regex", "stream"}
        required = {"regex"}
        check_keys(allowed, required, config, type(self).__name__)

        self.regex = config["regex"]
        self.stream = config.get("stream")

        # confirm that the regex is compilable
        pattern = re.compile(asbytes(self.regex))
        assert pattern.findall(b"asdfasdfdatabase system is ready to accept connections")

    def build_atomic(self, poll, logger, stream, report_fd):
        # Allow the configured stream to overwrite the default stream.
        s = stream if self.stream is None else self.stream
        return LogCheck(logger, s, report_fd, self.regex)


class CustomAtomicConfig:
    def __init__(self, config):
        check_list_of_strings(config, "AtomicConfig.custom must be a list of strings")
        self.cmd = config

    def build_atomic(self, poll, logger, stream, report_fd):
        return AtomicSubprocess(poll, logger, stream, report_fd, self.cmd)

class ShellAtomicConfig:
    def __init__(self, config):
        assert isinstance(config, str), "AtomicConnfig.sh must be a single string"
        self.cmd = ["sh", "-c", config]

    def build_atomic(self, poll, logger, stream, report_fd):
        return AtomicSubprocess(poll, logger, stream, report_fd, self.cmd)


class CustomConfig(StageConfig):
    def __init__(self, config):
        allowed = {"cmd", "name", "pre", "post"}
        required = {"cmd", "name"}

        check_keys(allowed, required, config, type(self).__name__)

        self.cmd = config["cmd"]
        check_list_of_strings(self.cmd, "CustomConfig.cmd must be a list of strings")

        self.name = config["name"]
        assert isinstance(self.name, str), "CustomConfig.name must be a string"

        check_list_of_dicts(config.get("pre", []), "CustomConfig.pre must be a list of dicts")
        self.pre = [AtomicConfig.read(pre) for pre in config.get("pre", [])]

        check_list_of_dicts(config.get("post", []), "CustomConfig.post must be a list of dicts")
        self.post = [AtomicConfig.read(post) for post in config.get("post", [])]

    def build_stage(self, poll, logger, state_machine):
        return Process(self, poll, logger, state_machine)


class CustomDockerConfig(StageConfig):
    def __init__(self, config):
        allowed = {"name", "container_name", "run_args", "pre", "post"}
        required = {"name", "container_name", "run_args"}

        check_keys(allowed, required, config, type(self).__name__)

        self.container_name = config["container_name"]

        self.run_args = config["run_args"]
        check_list_of_strings(
            self.run_args, "CustomConfig.run_args must be a list of strings"
        )

        self.name = config["name"]
        assert isinstance(self.name, str), "CustomConfig.name must be a string"

        check_list_of_dicts(config.get("pre", []), "CustomConfig.pre must be a list of dicts")
        self.pre = [AtomicConfig.read(pre) for pre in config.get("pre", [])]

        check_list_of_dicts(config.get("post", []), "CustomConfig.post must be a list of dicts")
        self.post = [AtomicConfig.read(post) for post in config.get("post", [])]

    def build_stage(self, poll, logger, state_machine):
        return DockerProcess(self, poll, logger, state_machine)


class Config:
    def __init__(self, config):
        allowed = {"stages", "startup_input", "log_dir"}
        required = {"stages"}
        check_keys(allowed, required, config, type(self).__name__)

        check_list_of_dicts(config["stages"], "stages must be a list of dicts")
        self.stages = [StageConfig.read(stage) for stage in config["stages"]]
        self.startup_input = config.get("startup_input", "")
        self.log_dir = config.get("log_dir", "/tmp/devcluster-logs")


def main(config):

    with terminal_config():
        poll = Poll()

        stage_names = [stage_config.name for stage_config in config.stages]

        logger = Logger(stage_names, config.log_dir)

        state_machine = StateMachine(logger, poll)

        console = Console(logger, poll, state_machine)

        def _sigwinch_handler(signum, frame):
            """Enqueue a call to _sigwinch_callback() via poll()."""
            os.write(state_machine.get_report_fd(), b"W")

        def _sigwinch_callback():
            """Handle the SIGWINCH when it is safe"""
            console.handle_window_change()

        state_machine.add_report_callback("W", _sigwinch_callback)
        signal.signal(signal.SIGWINCH, _sigwinch_handler)

        for stage_config in config.stages:
            state_machine.add_stage(stage_config.build_stage(poll, logger, state_machine))

        # Draw the screen ASAP
        console.redraw()

        state_machine.set_target(len(stage_names))

        for c in config.startup_input:
            console.handle_key(c)

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

    if "DET_PROJ" not in os.environ:
        print("you must specify the DET_PROJ environment variable", file=sys.stderr)
        sys.exit(1)
    os.chdir(os.environ["DET_PROJ"])

    main(config)
