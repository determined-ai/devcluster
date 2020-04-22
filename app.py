import curses
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

# STATES:
#
# DOWN
# PRE_DB
# DB
# PRE_HASURA
# HASURA
# PRE_MASTER
# MASTER
# AGENT

class States(enum.Enum):
    DEAD = "DEAD"
    PRE_DB = "PRE_DB"
    DB = "DB"
    PRE_HASURA = "PRE_HASURA"
    HASURA = "HASURA"
    PRE_MASTER = "PRE_MASTER"
    MASTER = "MASTER"
    AGENT = "AGENT"


def asbytes(msg):
    if isinstance(msg, bytes):
        return msg
    return msg.encode('utf8')


class ConnCheck(threading.Thread):
    def __init__(self, host, port, report_fd, success_msg):
        self.host = host
        self.port = port
        self.report_fd = report_fd
        self.success_msg = asbytes(success_msg)
        self.quit = False

        super().__init__()

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
            if not self.quit:
                os.write(self.report_fd, self.success_msg if success else b'FAIL')

class NDet:
    def __init__(self):
        self.quitting = False

        self.master_proc = None
        self.master_out = None
        self.master_err = None
        self.master_dying = False

        self.connector = None
        self.conn_read, self.conn_write = os.pipe2(os.O_CLOEXEC | os.O_NONBLOCK)

        self.in_poll_flags = select.POLLIN | select.POLLPRI
        self.err_poll_flags = select.POLLERR | select.POLLHUP | select.POLLNVAL

        self.poll = select.poll()
        self.poll.register(sys.stdin.fileno(), self.in_poll_flags)
        self.poll.register(self.conn_read, self.in_poll_flags)

        self.state = States.DEAD
        self.target = States.DEAD

        self.logs = {
            "ndet": b"",
            "master": b"",
        }
        self.active_stream = ""

        self.set_active_stream("ndet")


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

    def nonblock(self, fd):
        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

    def close_connector(self):
        if self.connector is not None:
            self.connector.quit = True
            self.connector.join()
            self.connector = None

    def reset_connector(self, host, port, next_state):
        self.close_connector()

        self.connector = ConnCheck(host, port, self.conn_write, next_state)
        self.connector.start()

    def next_thing(self):
        # Nothing to do.
        if self.state == self.target:
            return

        self.log(f"next thing: state = {self.state}, target = {self.target}\n")

        if self.state == States.DEAD:
            self.start_master()
            return

        elif self.state == States.MASTER:
            if self.target == States.DEAD:
                self.kill_master()
                return

        raise NotImplementedError()


    def set_target(self, target):
        """For when you choose a new target state."""
        if target == self.target:
            return

        if self.state in {States.PRE_DB, States.PRE_HASURA, States.PRE_MASTER}:
            # Wait till the intermediate state resolves
            return

        self.target = target

        self.next_thing()

    def transition(self, new_state):
        """For when you arrive at a new state."""
        self.state = new_state

        if self.state == States.DEAD and self.quitting:
            sys.exit(0)

        self.next_thing()

    def start_master(self):
        self.master_dying = False
        self.master_proc = subprocess.Popen(
            MASTER_CMD,
            # ["master.sh"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.master_out = self.master_proc.stdout.fileno()
        self.master_err = self.master_proc.stderr.fileno()

        self.nonblock(self.master_out)
        self.nonblock(self.master_err)

        self.poll.register(self.master_out, self.in_poll_flags)
        self.poll.register(self.master_err, self.in_poll_flags)

        self.state = States.PRE_MASTER
        self.reset_connector("localhost", 8080, "MASTER")

    def kill_master(self):
        self.master_dying = True
        self.master_proc.kill()
        self.state = States.PRE_MASTER

    def maybe_wait_master(self):
        """wait() on master if both stdout and stderr are empty."""
        if self.master_out is None and self.master_err is None:
            ret = self.master_proc.wait()
            self.log(f"master exited with {ret}\n")
            self.log(f"NDET: master exited with {ret}\n", "master")
            self.master_proc = None

            self.close_connector()

            self.transition(States.DEAD)
            self.log(f"after transition, self.state is {self.state}\n")

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
        msg = f"state:{self.state} target:{self.target} stream:{self.active_stream}".encode("utf8")
        os.write(sys.stdout.fileno(), invert_colors(msg))
        self.place_cursor(row, col)

    def loop(self):
        while True:
            self.print_bar()
            ready = self.poll.poll()
            for fd, ev in ready:
                # console input
                if fd == sys.stdin.fileno():
                    self.handle_stdin(ev)

                # connector pipe
                elif fd == self.conn_read:
                    self.handle_connector(ev)

                # master fds
                elif fd == self.master_out:
                    self.handle_master_out(ev)
                elif fd == self.master_err:
                    self.handle_master_err(ev)

    def run(self):

        self.set_target(States.MASTER)

        try:
            self.loop()
        finally:
            if self.master_proc is not None:
                raise NotImplementedError("emergency shut down doesn't work")

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

    def handle_connector(self, ev):
        self.connector.join()
        self.connector = None

        if ev & self.in_poll_flags:
            msg = os.read(self.conn_read, 4096).decode("utf8")
            if msg == "FAIL":
                if self.state == States.PRE_MASTER:
                    self.kill_master()
                else:
                    raise NotImplementedError()
                return

            msg = States(msg)
            if self.state == States.PRE_MASTER and msg == States.MASTER:
                self.transition(States.MASTER)
            else:
                raise NotImplementedError()

        if ev & self.err_poll_flags:
            # Just die.
            raise ValueError("pipe failed!")

    def handle_master_out(self, ev):
        if ev & self.in_poll_flags:
            self.log(os.read(self.master_out, 4096), "master")
        if ev & self.err_poll_flags:
            self.poll.unregister(self.master_out)
            os.close(self.master_out)
            self.master_out = None
            if self.master_dying:
                self.maybe_wait_master()
            else:
                self.log(b'master stdout closed unexpectedly\n')
                self.set_target(States.DEAD)

    def handle_master_err(self, ev):
        if ev & self.in_poll_flags:
            self.log(os.read(self.master_err, 4096), "master")
        if ev & self.err_poll_flags:
            self.poll.unregister(self.master_err)
            os.close(self.master_err)
            self.master_err = None
            if self.master_dying:
                self.maybe_wait_master()
            else:
                self.log(b'master stderr closed unexpectedly\n')
                self.set_target(States.DEAD)

if __name__ == "__main__":
    with terminal_config():
        NDet().run()
