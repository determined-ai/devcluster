import abc
import os
import re
import threading
import time
import socket
import subprocess
import typing

import devcluster as dc


class AtomicOperation(metaclass=abc.ABCMeta):
    """
    Only have one atomic operation in flight at a time.  You must wait for it to finish but you may
    request it ends early if you know you will ignore its output.

    An example would be a connector which is trying to connect to the master binary, except if the
    master binary has already exited, we will want to exit the connector.
    """

    @abc.abstractmethod
    def __str__(self) -> str:
        """Return a one-word summary of what the operation is"""
        pass

    @abc.abstractmethod
    def cancel(self) -> None:
        pass

    @abc.abstractmethod
    def join(self) -> None:
        pass


class ConnCheck(threading.Thread, AtomicOperation):
    """ConnCheck is an AtomicOperation."""

    def __init__(self, host: str, port: int, report_fd: int):
        self.host = host
        self.port = port
        self.report_fd = report_fd
        self.quit = False

        threading.Thread.__init__(self)

        # AtomicOperations should not need a start() call.
        self.start()

    def __str__(self) -> str:
        return "connecting"

    def run(self) -> None:
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
                except (socket.timeout, ConnectionError):
                    now = time.time()
                    if now < waittime:
                        time.sleep(waittime - now)
                    continue
                s.close()
                success = True
                break
        finally:
            # "S"uccess or "F"ail
            os.write(self.report_fd, b"S" if success else b"F")

    def cancel(self) -> None:
        self.quit = True


class LogCheck(AtomicOperation):
    """
    Wait for a log stream to print out a phrase before allowing the state to progress.
    """

    def __init__(
        self,
        logger: dc.Logger,
        stream: str,
        report_fd: int,
        regex: typing.Union[str, bytes],
    ):
        self.logger = logger
        self.stream = stream
        self.report_fd = report_fd

        self.pattern = re.compile(dc.asbytes(regex))

        self.canceled = False

        self.logger.add_callback(self.log_cb)

    def __str__(self) -> str:
        return "checking"

    def cancel(self) -> None:
        if not self.canceled:
            self.canceled = True
            os.write(self.report_fd, b"F")
            self.logger.remove_callback(self.log_cb)

    def join(self) -> None:
        pass

    def log_cb(self, log: dc.Log) -> None:
        if log.stream != self.stream:
            return

        if len(self.pattern.findall(log.msg)) == 0:
            return

        os.write(self.report_fd, b"S")
        self.logger.remove_callback(self.log_cb)


# AtomicCB(success: bool, stdout: bytes) -> None
AtomicCB = typing.Callable[[bool, bytes], None]


class AtomicSubprocess(AtomicOperation):
    def __init__(
        self,
        poll: dc.Poll,
        logger: dc.Logger,
        stream: str,
        report_fd: int,
        cmd: typing.List[str],
        quiet: bool = False,
        callbacks: typing.Sequence[AtomicCB] = (),
    ) -> None:
        self.poll = poll
        self.logger = logger
        self.stream = stream
        self.report_fd = report_fd
        self.quiet = quiet
        self.callbacks = callbacks
        self.captured_stdout = []  # type: typing.List[bytes]

        self.start_time = time.time()

        self.dying = False
        self.proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )  # type: typing.Optional[subprocess.Popen]
        assert self.proc.stdout
        self.out = self.proc.stdout.fileno()  # type: typing.Optional[int]
        assert self.proc.stderr
        self.err = self.proc.stderr.fileno()  # type: typing.Optional[int]

        if self.out is not None:
            dc.nonblock(self.out)
        dc.nonblock(self.err)

        if self.out is not None:
            self.poll.register(self.out, dc.Poll.IN_FLAGS, self._handle_out)
        self.poll.register(self.err, dc.Poll.IN_FLAGS, self._handle_err)

    def __str__(self) -> str:
        return "building"

    def _maybe_wait(self) -> None:
        """Only respond after both stdout and stderr have closed."""
        assert self.proc
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
                self.logger.log(
                    f" ----- {self} complete! (%.2fs) -----\n" % (build_time),
                    self.stream,
                )
                success = True

            for cb in self.callbacks:
                cb(success, b"".join(self.captured_stdout))

            os.write(self.report_fd, b"S" if success else b"F")

    def _handle_out(self, ev: int, _: int) -> None:
        assert self.out
        if ev & dc.Poll.IN_FLAGS:
            chunk = os.read(self.out, 4096)
            if not self.quiet:
                self.logger.log(chunk, self.stream)
            self.captured_stdout.append(chunk)
        if ev & dc.Poll.ERR_FLAGS:
            self.poll.unregister(self._handle_out)
            os.close(self.out)
            self.out = None
            self._maybe_wait()

    def _handle_err(self, ev: int, _: int) -> None:
        assert self.err
        if ev & dc.Poll.IN_FLAGS:
            self.logger.log(os.read(self.err, 4096), self.stream)
        if ev & dc.Poll.ERR_FLAGS:
            self.poll.unregister(self._handle_err)
            os.close(self.err)
            self.err = None
            self._maybe_wait()

    def cancel(self) -> None:
        assert self.proc
        self.dying = True
        self.proc.kill()

    def join(self) -> None:
        pass


class DockerRunAtomic(AtomicSubprocess):
    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        kwargs["quiet"] = True
        super().__init__(*args, **kwargs)

    def __str__(self) -> str:
        return "starting"

    def cancel(self) -> None:
        # Don't support canceling at all; it creates a race condition where we don't know when
        # we can docker kill the container.
        pass
