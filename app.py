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

import subprocess
import fcntl
import collections

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


# os.write(1, b'\x1b[6n')

def invert_colors(msg):
    return b'\x1b[7m' + msg + b'\x1b[0m'

MASTER_CMD = [
    os.path.join(os.path.expanduser("~/go/bin"), "determined-master"),
    "--log-level", "debug",
    "--db-user", "postgres",
    "--db-port", "5432",
    "--db-name", "pedl",
    "--db-password=jAmGMeVw3ycU2Ft",
    "--hasura-secret=ML7hq3Lyuxv4qUb",
    "--root", os.path.expanduser("~/code/determined/build/share/determined/master"),
]

AGENT_CMD = [
    os.path.join(os.path.expanduser("~/go/bin"), "determined-agent"),
    "--master_host", "192.168.0.4",
    "--master_port", "8080",
    "run"
]


class States(enum.Enum):
    DEAD = "DEAD"
    DB = "DB"
    HASURA = "HASURA"
    MASTER = "MASTER"
    AGENT = "AGENT"

    def __str__(self):
        if self == States.DEAD: return "DEAD"
        if self == States.DB: return "DB"
        if self == States.HASURA: return "HASURA"
        if self == States.MASTER: return "MASTER"
        if self == States.AGENT: return "AGENT"

    def get_index(self):
        return {
            States.DEAD: 0,
            States.DB: 1,
            States.HASURA: 2,
            States.MASTER: 3,
            States.AGENT: 4,
        }[self]

    def __cmp__(self, other):
        s = self.get_index()
        o = other.get_index()
        if s < o: return -1
        return int(s > o)

    def __lt__(self, other):
        return self.get_index() < other.get_index()

    @staticmethod
    def from_index(index):
        return (
            States.DEAD,
            States.DB,
            States.HASURA,
            States.MASTER,
            States.AGENT,
        )[min(max(index ,0), 4)]

    def __sub__(self, num):
        return States.from_index(self.get_index() - num)

    def __add__(self, num):
        return States.from_index(self.get_index() - num)

    def __rsub__(self, num):
        return States.from_index(self.get_index() - num)

    def __radd__(self, num):
        return States.from_index(self.get_index() - num)


def asbytes(msg):
    if isinstance(msg, bytes):
        return msg
    return msg.encode('utf8')


def nonblock(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

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

class Process:
    """
    A long-running process may have precommands to run first and postcommands before it is ready.
    """

    @abc.abstractmethod
    def get_precommand(self):
        """Return the next AtomicOperation or None, at which point it is safe to run_command)."""
        pass

    @abc.abstractmethod
    def get_postcommand(self):
        """Return the next AtomicOperation or None, at which point the command is up."""
        pass

    @abc.abstractmethod
    def run_command(self):
        """Start the long-running process."""
        pass

    @abc.abstractmethod
    def running(self):
        """Is the long-running process still un-waited?"""
        pass

    @abc.abstractmethod
    def kill(self):
        """Kill the long-running process (atomic operations must be canceled normally)."""
        pass

class Master(Process):
    def __init__(self, poll, log, pipe_wr, set_target, next_thing):
        self.proc = None
        self.out = None
        self.err = None
        self.dying = False

        self.poll = poll
        self.pipe_wr = pipe_wr
        self.log = log
        self.next_thing = next_thing
        self.set_target = set_target

        self._reset()

    def _reset(self):
        self.postcommands_run = 0

    def _maybe_wait(self):
        """wait() on master if both stdout and stderr are empty."""
        if not self.dying:
            self.log(f"master closing unexpectedly!\n")
            self.set_target(States.DEAD)

        if self.out is None and self.err is None:
            ret = self.proc.wait()
            self.log(f"master exited with {ret}\n")
            self.log(f"NDET: master exited with {ret}\n", "master")
            self.proc = None
            self._reset()
            self.next_thing()

    def _handle_out(self, ev):
        if ev & Poll.IN_FLAGS:
            self.log(os.read(self.out, 4096), "master")
        if ev & Poll.ERR_FLAGS:
            self.poll.unregister(self._handle_out)
            os.close(self.out)
            self.out = None
            self._maybe_wait()

    def _handle_err(self, ev):
        if ev & Poll.IN_FLAGS:
            self.log(os.read(self.err, 4096), "master")
        if ev & Poll.ERR_FLAGS:
            self.poll.unregister(self._handle_err)
            os.close(self.err)
            self.err = None
            self._maybe_wait()

    def get_precommand(self):
        return None

    def run_command(self):
        self.dying = False
        self.proc = subprocess.Popen(
            MASTER_CMD,
            # ["master.sh"],
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

    def get_postcommand(self):
        if self.postcommands_run == 1:
            return None

        self.postcommands_run += 1

        atomic_op = ConnCheck("localhost", 8080, self.pipe_wr, "MASTER")
        atomic_op.start()
        return atomic_op

    def running(self):
        return self.proc is not None

    def kill(self):
        self.dying = True
        self.proc.kill()

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

class ConnCheck(threading.Thread):
    """ConnCheck is an AtomicOperation."""

    def __init__(self, host, port, report_fd, success_msg):
        self.host = host
        self.port = port
        self.report_fd = report_fd
        self.success_msg = asbytes(success_msg)
        self.quit = False

        super().__init__()

    def __str__(self):
        return "connecting"

    def run(self):
        success = False
        try:
            for _ in range(15):
                if self.quit:
                    break
                s = socket.socket()
                try:
                    waittime = time.time() + 0.5
                    s.settimeout(0.5)
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
            os.write(self.report_fd, self.success_msg if success else b'FAIL')

    def cancel(self):
        self.quit = True


class NDet:
    def __init__(self):
        self.quitting = False

        # atomic_op is intermediate steps like calling `make` or connecting to a server
        self.atomic_op = None
        # the pipe is used by the atomic_op to pass messages to the poll loop
        self.pipe_rd, self.pipe_wr = os.pipe2(os.O_CLOEXEC | os.O_NONBLOCK)

        self.poll = Poll()

        self.poll.register(sys.stdin.fileno(), Poll.IN_FLAGS, self.handle_stdin)
        self.poll.register(self.pipe_rd, Poll.IN_FLAGS, self.handle_pipe)

        self.state = States.DEAD
        self.target = States.DEAD

        self.logs = {
            "ndet": b"",
            "master": b"",
        }
        self.active_stream = ""

        self.set_active_stream("ndet")

        # Master process
        self.master = Master(self.poll, self.log, self.pipe_wr, self.set_target, self.next_thing)


    def log(self, msg, stream="ndet"):
        """Append to a log stream."""

        msg = asbytes(msg)

        self.logs[stream] += msg
        if self.active_stream == stream:
            os.write(sys.stdout.fileno(), msg)

        self.print_bar()

    def set_active_stream(self, new_stream):
        if new_stream == self.active_stream:
            return

        self.erase_screen()
        self.place_cursor(2, 1)
        # dump logs
        os.write(sys.stdout.fileno(), self.logs[new_stream][-16*1024:])
        self.print_bar()

        self.active_stream = new_stream

    def advance_process(self, process):
        """
        Either:
          - start the next precommand,
          - start the real command (and maybe a postcommand), or
          - start the next postcommand
        """
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
        # Nothing to do.
        if self.state == self.target:
            return

        # Advance state?
        if self.state < self.target:
            # Wait for the atomic operation to finish.
            if self.atomic_op is not None:
                return

            if self.state == States.DEAD:
                self.transition(States.MASTER)
                self.advance_process(self.master)
            else:
                raise NotImplementedError()

        # Regress state.
        if self.state > self.target:
            # Cancel any atomic operations first.
            if self.atomic_op is not None:
                self.atomic_op.cancel()
                return

            if self.state == States.MASTER:
                if self.master.running():
                    self.master.kill()
                    return
                self.transition(States.DEAD)
            else:
                raise NotImplementedError()

    def set_target(self, target):
        """For when you choose a new target state."""
        if target == self.target:
            return

        self.target = target

        self.next_thing()

    def transition(self, new_state):
        """For when you arrive at a new state."""
        self.state = new_state

        self.next_thing()

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
        state_msg = str(self.state) + (f"({self.atomic_op})" if self.atomic_op is not None else "")
        msg = f"state:{state_msg} target:{self.target} stream:{self.active_stream}".encode("utf8")
        os.write(sys.stdout.fileno(), invert_colors(msg))
        self.place_cursor(row, col)

    def run(self):
        self.set_target(States.MASTER)
        while not (self.quitting and self.state == States.DEAD):
            self.print_bar()
            self.poll.poll()

    def handle_stdin(self, ev):
        c = sys.stdin.read(1)
        if c == '\x03':
            if self.quitting or self.state == States.DEAD:
                raise KeyboardInterrupt()
            self.log("quitting...\n")
            self.quitting = True
            self.set_target(States.DEAD)
        elif c == "m":
            self.set_active_stream("master")
        elif c == "c":
            self.set_active_stream("ndet")
        elif c == "1":
            self.set_target(States.DEAD)
            self.log("set target dead!\n")
        elif c == "2":
            self.set_target(States.MASTER)
            self.log("set target master!\n")

    def handle_pipe(self, ev):
        self.atomic_op.join()
        self.atomic_op = None

        if ev & Poll.IN_FLAGS:
            msg = os.read(self.pipe_rd, 4096).decode("utf8")
            if msg == "FAIL":
                # set the target state to be one less than wherever-we-are
                self.target = max(self.state - 1, self.target)

            self.next_thing()

        if ev & Poll.ERR_FLAGS:
            # Just die.
            raise ValueError("pipe failed!")

if __name__ == "__main__":
    with terminal_config():
        NDet().run()
