import abc
import os
import subprocess
import typing

import devcluster as dc


class Stage:
    @abc.abstractmethod
    def run_command(self) -> None:
        pass

    @abc.abstractmethod
    def running(self) -> bool:
        pass

    def killable(self) -> bool:
        """By default, killable() returns running().  It's more complex for docker"""
        return self.running()

    @abc.abstractmethod
    def kill(self) -> None:
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

    def kill(self) -> None:
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

    @abc.abstractmethod
    def wait(self) -> int:
        pass

    def _maybe_wait(self) -> None:
        """wait() on proc if both stdout and stderr are empty."""
        if not self.dying:
            self.logger.log(f"{self.log_name()} closing unexpectedly!\n")
            # TODO: don't always go to dead state
            self.state_machine.set_target(0)

        if self.out is None and self.err is None:
            ret = self.wait()
            self.logger.log(f"{self.log_name()} exited with {ret}\n")
            self.logger.log(
                f" ----- {self.log_name()} exited with {ret} -----\n", self.log_name()
            )
            self.proc = None
            self.reset()
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
    ) -> None:
        super().__init__(
            poll, logger, state_machine, config.name, config.pre, config.post
        )
        self.config = config

    def wait(self) -> int:
        assert self.proc
        return self.proc.wait()

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

    def kill(self) -> None:
        # kill via signal
        self.dying = True
        assert self.proc
        self.proc.kill()


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
    ) -> None:
        super().__init__(
            poll, logger, state_machine, config.name, config.pre, config.post
        )
        self.config = config

        # docker run --detach has to be an AtomicOperation because it is way too slow and causes
        # the UI to hang.  This has far-reaching implications, since `running()` is not longer
        # based on the main subprocess, and you may then have to `kill()` or `wait()` while the
        # main subprocess hasn't even been launched.
        self.docker_started = False

    def after_container_start(self, success: bool) -> None:
        self.docker_started = success
        if not success:
            self.logger.log(
                "failed to start a docker container, possibly try:\n\n"
                f"    docker kill {self.config.container_name}; "
                f"docker container rm {self.config.container_name}\n\n"
            )

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

    def docker_wait(self) -> int:
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

        return docker_exit_val

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

        return self.docker_wait()

    def kill(self) -> None:
        self.dying = True
        kill_cmd = [
            "docker",
            "kill",
            f"--signal={self.config.kill_signal}",
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
        # TODO: configure some async thing to wake up the poll loop via the report_fd so this
        # doesn't block.
        if self.proc is None:
            self.docker_wait()
            self.reset()
            self.state_machine.next_thing()
