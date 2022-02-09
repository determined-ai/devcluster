import abc
import os
import selectors
import signal
import subprocess
import time
import typing

import devcluster as dc


class Stage:
    @abc.abstractmethod
    def run_command(self) -> None:
        pass

    @abc.abstractmethod
    def running(self) -> bool:
        pass

    @abc.abstractmethod
    def crashed(self) -> bool:
        """Return True if the Stage started but then died since the last .reset()."""
        pass

    def killable(self) -> bool:
        """By default, killable() returns running().  It's more complex for docker"""
        return self.running()

    @abc.abstractmethod
    def kill(self, sig: typing.Optional[signal.Signals] = None) -> None:
        pass

    @abc.abstractmethod
    def reset(self) -> None:
        pass

    @abc.abstractmethod
    def get_precommand(self) -> typing.Optional[dc.AtomicOperation]:
        """Return the next AtomicOperation or None, at which point it is safe to run_command)."""
        pass

    @abc.abstractmethod
    def get_postcommand(self) -> typing.Optional[dc.AtomicOperation]:
        """Return the next AtomicOperation or None, at which point the command is up."""
        pass

    @abc.abstractmethod
    def log_name(self) -> str:
        """return the name of this stage, it must be unique"""
        pass


class DeadStage(Stage):
    """A noop stage for the base state of the state machine"""

    def __init__(self, state_machine: dc.StateMachine) -> None:
        self.state_machine = state_machine
        self._running = False

    def run_command(self) -> None:
        self._running = True

    def running(self) -> bool:
        return self._running

    def crashed(self) -> bool:
        return False

    def kill(self, sig: typing.Optional[signal.Signals] = None) -> None:
        self._running = False
        self.state_machine.next_thing()

    def reset(self) -> None:
        pass

    def get_precommand(self) -> typing.Optional[dc.AtomicOperation]:
        pass

    def get_postcommand(self) -> typing.Optional[dc.AtomicOperation]:
        pass

    def log_name(self) -> str:
        return "dead"


class BaseProcess(Stage, metaclass=abc.ABCMeta):
    """
    The parts of Process and DockerProcess which are reused.
    """

    def __init__(
        self,
        poll: dc.Poll,
        logger: dc.Logger,
        state_machine: dc.StateMachine,
        name: str,
        pre: typing.List[dc.AtomicConfig],
        post: typing.List[dc.AtomicConfig],
    ) -> None:
        self.proc = None  # type: typing.Optional[subprocess.Popen]
        self.out = None  # type: typing.Optional[int]
        self.err = None  # type: typing.Optional[int]
        self.dying = False

        self.poll = poll
        self.logger = logger
        self.state_machine = state_machine

        self.name = name
        self.pre = pre
        self.post = post

        self.reset()

    def reset(self) -> None:
        self.precmds_run = 0
        self.postcmds_run = 0
        self._crashed = False

    @abc.abstractmethod
    def wait(self) -> int:
        pass

    def _maybe_wait(self) -> None:
        """wait() on proc if both stdout and stderr are empty."""
        if not self.dying:
            self.logger.log(f"{self.log_name()} closing unexpectedly!\n")
            self._crashed = True
            self.state_machine.report_crash()

        if self.out is None and self.err is None:
            ret = self.wait()
            self.logger.log(f"{self.log_name()} exited with {ret}\n")
            self.logger.log(
                f" ----- {self.log_name()} exited with {ret} -----\n", self.log_name()
            )
            self.proc = None
            self.state_machine.next_thing()

    def _handle_out(self, ev: int, _: int) -> None:
        assert self.out
        if ev & dc.Poll.IN_FLAGS:
            self.logger.log(os.read(self.out, 4096), self.log_name())
        if ev & dc.Poll.ERR_FLAGS:
            self.poll.unregister(self._handle_out)
            os.close(self.out)
            self.out = None
            self._maybe_wait()

    def _handle_err(self, ev: int, _: int) -> None:
        assert self.err
        if ev & dc.Poll.IN_FLAGS:
            self.logger.log(os.read(self.err, 4096), self.log_name())
        if ev & dc.Poll.ERR_FLAGS:
            self.poll.unregister(self._handle_err)
            os.close(self.err)
            self.err = None
            self._maybe_wait()

    def get_precommand(self) -> typing.Optional[dc.AtomicOperation]:
        if self.precmds_run < len(self.pre):
            precmd_config = self.pre[self.precmds_run]
            self.precmds_run += 1

            return precmd_config.build_atomic(
                self.poll,
                self.logger,
                self.log_name(),
                self.state_machine.get_report_fd(),
            )
        return None

    def get_postcommand(self) -> typing.Optional[dc.AtomicOperation]:
        if self.postcmds_run < len(self.post):
            postcmd_config = self.post[self.postcmds_run]
            self.postcmds_run += 1

            return postcmd_config.build_atomic(
                self.poll,
                self.logger,
                self.log_name(),
                self.state_machine.get_report_fd(),
            )
        return None

    def running(self) -> bool:
        return self.proc is not None

    def crashed(self) -> bool:
        return self._crashed

    def log_name(self) -> str:
        return self.name


class Process(BaseProcess):
    """
    A long-running process may have precommands to run first and postcommands before it is ready.
    """

    def __init__(
        self,
        config: dc.CustomConfig,
        poll: dc.Poll,
        logger: dc.Logger,
        state_machine: dc.StateMachine,
        process_tracker: dc.ProcessTracker,
    ) -> None:
        super().__init__(
            poll, logger, state_machine, config.name, config.pre, config.post
        )
        self.process_tracker = process_tracker
        self.config = config

    def wait(self) -> int:
        assert self.proc
        out = self.proc.wait()
        self.process_tracker.report_pid_killed(self.proc.pid)
        return out

    def run_command(self) -> None:
        self.dying = False
        env = dict(os.environ)
        env.update(self.config.env)
        self.proc = subprocess.Popen(
            self.config.cmd,
            env=env,
            cwd=self.config.cwd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            # Run with a different process group to isolate from signals in the parent process.
            preexec_fn=os.setpgrp,
        )
        assert self.proc.stdout
        assert self.proc.stderr
        self.out = self.proc.stdout.fileno()
        self.err = self.proc.stderr.fileno()

        dc.nonblock(self.out)
        dc.nonblock(self.err)

        self.poll.register(self.out, dc.Poll.IN_FLAGS, self._handle_out)
        self.poll.register(self.err, dc.Poll.IN_FLAGS, self._handle_err)

        self.process_tracker.report_pid_started(
            self.proc.pid, " ".join(self.config.cmd)
        )

    def kill(self, sig: typing.Optional[signal.Signals] = None) -> None:
        # kill via signal
        self.dying = True
        assert self.proc
        if sig is None:
            self.proc.kill()
        else:
            self.proc.send_signal(sig)


class DockerProcess(BaseProcess):
    """
    A long-running process in docker with special startup and kill semantics.
    """

    def __init__(
        self,
        config: dc.CustomDockerConfig,
        poll: dc.Poll,
        logger: dc.Logger,
        state_machine: dc.StateMachine,
        process_tracker: dc.ProcessTracker,
    ) -> None:
        super().__init__(
            poll, logger, state_machine, config.name, config.pre, config.post
        )
        self.process_tracker = process_tracker
        self.config = config
        self.container_id = ""

        # docker run --detach has to be an AtomicOperation because it is way too slow and causes
        # the UI to hang.  This has far-reaching implications, since `running()` is no longer
        # based on the main subprocess, and you may then have to `kill()` or `wait()` while the
        # main subprocess hasn't even been launched.
        self.docker_started = False

    def after_container_start(self, success: bool, stdout: bytes) -> None:
        self.docker_started = success
        if success:
            self.container_id = stdout.strip().decode("utf8")
            self.process_tracker.report_container_started(self.container_id)

    def get_precommand(self) -> typing.Optional[dc.AtomicOperation]:
        # Inherit the precmds behavior from Process.
        precmd = super().get_precommand()
        if precmd is not None:
            return precmd

        if not self.docker_started:
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
            return dc.DockerRunAtomic(
                self.poll,
                self.logger,
                self.log_name(),
                self.state_machine.get_report_fd(),
                run_args,
                callbacks=[self.after_container_start],
            )

        return None

    def killable(self) -> bool:
        return self.docker_started or self.proc is not None

    def run_command(self) -> None:
        self.dying = False
        self.proc = subprocess.Popen(
            ["docker", "container", "logs", "-f", self.config.container_name],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            # Run with a different process group to isolate from signals in the parent process.
            preexec_fn=os.setpgrp,
        )
        assert self.proc.stdout
        assert self.proc.stderr
        self.out = self.proc.stdout.fileno()
        self.err = self.proc.stderr.fileno()

        dc.nonblock(self.out)
        dc.nonblock(self.err)

        self.poll.register(self.out, dc.Poll.IN_FLAGS, self._handle_out)
        self.poll.register(self.err, dc.Poll.IN_FLAGS, self._handle_err)

        self.process_tracker.report_pid_started(self.proc.pid, "docker container logs")

    def docker_wait(self, timeout: typing.Optional[float] = None) -> int:
        # Wait for the container.
        self.docker_started = False

        run_args = ["docker", "wait", self.config.container_name]

        p = subprocess.Popen(
            run_args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert p.stdout
        assert p.stderr

        if timeout:
            # Normally we only use docker_wait() when we already have reason to believe that the
            # container is already dead (both stdout and stderr have closed).
            # But there's a special case where if a stage is canceled before it is done with
            # startup, we end up waiting for the startup to finish and kill the container an instant
            # later.  Seemingly only on macs, the container seems to miss the early signal and
            # docker_wait() will hang forever.  We introduce this wait-with-timeout as more of a
            # workaround than anything else.  On Linux, the wait ought to be basically instant and
            # the timeout case ought to behave almost identically to the non-timeout case.
            try:
                err = readall_with_timeout(p.stderr, timeout)
            except TimeoutError:
                # stop trying to `docker wait` and just nuke the container.
                p.kill()
                p.wait()
                self.logger.log(
                    f" ----- `docker wait` for {self.log_name()} took too long, force-removing the "
                    "container -----\n",
                    self.log_name(),
                )
                self.docker_rm(force=True)
                return -1
        else:
            err = p.stderr.read()

        ret = p.wait()
        if ret != 0:
            self.logger.log(
                dc.asbytes(
                    f"`docker wait` for {self.log_name()} failed with {ret} saying\n"
                )
                + dc.asbytes(err)
            )
            self.logger.log(
                f" ----- `docker wait` for {self.log_name()} exited with {ret} -----\n",
                self.log_name(),
            )
            self.docker_rm(force=True)
            return -1

        docker_exit_val = int(p.stdout.read().strip())

        self.docker_rm(force=False)

        return docker_exit_val

    def docker_rm(self, force: bool = False) -> None:
        run_args = ["docker", "container", "rm"]
        if force:
            run_args += ["--force"]
        run_args += [self.config.container_name]

        p = subprocess.Popen(
            run_args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        assert p.stderr

        err = p.stderr.read()
        ret = p.wait()
        if ret != 0:
            self.logger.log(
                dc.asbytes(
                    f"`docker container rm` for {self.log_name()} failed with {ret} saying:\n"
                )
                + dc.asbytes(err)
            )
            self.logger.log(
                f" ----- `docker container rm` for {self.log_name()} exited with {ret} -----\n",
                self.log_name(),
            )
        else:
            self.process_tracker.report_container_killed(self.container_id)

    def wait(self) -> int:
        assert self.proc
        ret = self.proc.wait()
        if ret != 0:
            self.logger.log(
                f"`docker container logs` for {self.log_name()} exited with {ret}\n"
            )
            self.logger.log(
                f" ----- `docker container logs` for {self.log_name()} exited with {ret} -----\n",
                self.log_name(),
            )

        self.process_tracker.report_pid_killed(self.proc.pid)

        return self.docker_wait()

    def kill(self, sig: typing.Optional[signal.Signals] = None) -> None:
        self.dying = True
        sigstr = self.config.kill_signal if sig is None else str(sig.value)
        kill_cmd = [
            "docker",
            "kill",
            f"--signal={sigstr}",
            self.config.container_name,
        ]
        p = subprocess.Popen(
            kill_cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        assert p.stderr
        err = p.stderr.read()
        ret = p.wait()
        if ret != 0:
            kill_str = " ".join(kill_cmd)
            self.logger.log(dc.asbytes(f'"{kill_str}" says:\n') + dc.asbytes(err))

        # If we got canceled, immediately wait on the docker container to exit; there won't be any
        # closing file descriptors to alert us that the container has closed.
        if self.proc is None:
            self.docker_wait(timeout=0.5)
            self.state_machine.next_thing()


def readall_with_timeout(f: typing.Any, timeout: float) -> bytes:
    """Use selectors to read from a file descriptor with a timeout."""
    dc.nonblock(f)
    now = time.time()
    deadline = now + timeout

    buffer = b""
    # TODO: convert the whole devcluster to use selectors instead of select.poll.
    with selectors.DefaultSelector() as sel:
        sel.register(f, selectors.EVENT_READ)
        keep_going = True
        while keep_going:
            for _, events in sel.select(timeout=deadline - now):
                if not (events & selectors.EVENT_READ):
                    raise ValueError("error selecting on file object")
                data = f.read(4096)
                if not data:
                    # Normal EOF.
                    keep_going = False
                    break
                buffer += data
            now = time.time()
            if now >= deadline:
                raise TimeoutError()

    return buffer
