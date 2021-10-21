import contextlib
import fcntl
import os
import termios
import subprocess
import select
import sys
import typing


class ImpossibleException(Exception):
    """Mypy isn't always smart enough."""

    pass


Text = typing.Union[str, bytes]


def asbytes(msg: Text) -> bytes:
    if isinstance(msg, bytes):
        return msg
    return msg.encode("utf8")


def nonblock(fd: int) -> None:
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK | os.O_CLOEXEC)


def fore_num(num: int) -> bytes:
    """Set terminal background color to a numbered color (0-255)."""
    return b"\x1b[38;5;%dm" % (num)


def back_num(num: int) -> bytes:
    """Set terminal background color to a numbered color (0-255)."""
    return b"\x1b[48;5;%dm" % (num)


# "res"et coloring in terminal.
res = b"\x1b[m"


_has_csr = None


def has_csr() -> bool:
    global _has_csr
    if _has_csr is None:
        try:
            p = subprocess.run(
                ["infocmp"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
            )
            p.check_returncode()
            # We only handle one form of change_scroll_region.
            _has_csr = b"csr=\\E[%i%p1%d;%p2%dr," in p.stdout
        except Exception:
            _has_csr = False
    return _has_csr


@contextlib.contextmanager
def terminal_config() -> typing.Iterator[None]:
    fd = sys.stdin.fileno()
    # old and new are of the form [iflag, oflag, cflag, lflag, ispeed, ospeed, cc]
    old = termios.tcgetattr(fd)
    new = termios.tcgetattr(fd)

    # raw terminal settings from `man 3 termios`
    new[0] = new[0] & ~(  # type: ignore
        termios.IGNBRK
        | termios.BRKINT
        | termios.PARMRK
        | termios.ISTRIP
        | termios.INLCR
        | termios.IGNCR
        | termios.ICRNL
        | termios.IXON
    )
    # new[1] = new[1] & ~termios.OPOST;
    new[2] = new[2] & ~(termios.CSIZE | termios.PARENB)  # type: ignore
    new[2] = new[2] | termios.CS8  # type: ignore
    new[3] = new[3] & ~(  # type: ignore
        termios.ECHO | termios.ECHONL | termios.ICANON | termios.ISIG | termios.IEXTEN
    )

    try:
        # enable alternate screen buffer
        os.write(sys.stdout.fileno(), b"\x1b[?1049h")
        if has_csr():
            # set scrolling region to not include status bar:
            os.write(sys.stdout.fileno(), b"\x1b[3r")
        # make the terminal raw
        termios.tcsetattr(fd, termios.TCSADRAIN, new)
        yield
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
        if has_csr():
            # reset scrolling region:
            os.write(sys.stdout.fileno(), b"\x1b[r")
        # disable alternate screen buffer
        os.write(sys.stdout.fileno(), b"\x1b[?1049l")


Handler = typing.Callable[[int, int], None]


class Poll:
    IN_FLAGS = select.POLLIN | select.POLLPRI
    ERR_FLAGS = select.POLLERR | select.POLLHUP | select.POLLNVAL

    def __init__(self) -> None:
        # Maps file descriptors to handler functions.
        self.handlers = {}  # type: typing.Dict[int, Handler]
        # Maps handlers back to file descriptors.
        self.fds = {}  # type: typing.Dict[Handler, int]
        self._poll = select.poll()

    def register(self, fd: int, flags: int, handler: Handler) -> None:
        self._poll.register(fd, flags)
        self.handlers[fd] = handler
        self.fds[handler] = fd

    def unregister(self, handler: Handler) -> None:
        fd = self.fds[handler]
        self._poll.unregister(fd)
        del self.fds[handler]
        del self.handlers[fd]

    def poll(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        ready = self._poll.poll()
        for fd, ev in ready:
            handler = self.handlers[fd]
            handler(ev, fd)
