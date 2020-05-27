import abc
import os
import subprocess

import devcluster as dc


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
    def reset(self):
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

    def run_command(self):
        self._running = True

    def running(self):
        return self._running

    def kill(self):
        self._running = False
        self.state_machine.next_thing()

    def reset(self):
        pass

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

        self.reset()

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
            self.logger.log(
                f" ----- {self.log_name()} exited with {ret} -----\n", self.log_name()
            )
            self.proc = None
            self.reset()
            self.state_machine.next_thing()

    def _handle_out(self, ev):
        if ev & dc.Poll.IN_FLAGS:
            self.logger.log(os.read(self.out, 4096), self.log_name())
        if ev & dc.Poll.ERR_FLAGS:
            self.poll.unregister(self._handle_out)
            os.close(self.out)
            self.out = None
            self._maybe_wait()

    def _handle_err(self, ev):
        if ev & dc.Poll.IN_FLAGS:
            self.logger.log(os.read(self.err, 4096), self.log_name())
        if ev & dc.Poll.ERR_FLAGS:
            self.poll.unregister(self._handle_err)
            os.close(self.err)
            self.err = None
            self._maybe_wait()

    def get_precommand(self):
        if self.precmds_run < len(self.config.pre):
            precmd_config = self.config.pre[self.precmds_run]
            self.precmds_run += 1

            return precmd_config.build_atomic(
                self.poll,
                self.logger,
                self.log_name(),
                self.state_machine.get_report_fd(),
            )
        return None

    def get_postcommand(self):
        if self.postcmds_run < len(self.config.post):
            postcmd_config = self.config.post[self.postcmds_run]
            self.postcmds_run += 1

            return postcmd_config.build_atomic(
                self.poll,
                self.logger,
                self.log_name(),
                self.state_machine.get_report_fd(),
            )
        return None

    def run_command(self):
        self.dying = False
        self.proc = subprocess.Popen(
            self.config.cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            # Run with a different process group to isolate from signals in the parent process.
            preexec_fn=os.setpgrp,
        )
        self.out = self.proc.stdout.fileno()
        self.err = self.proc.stderr.fileno()

        dc.nonblock(self.out)
        dc.nonblock(self.err)

        self.poll.register(self.out, dc.Poll.IN_FLAGS, self._handle_out)
        self.poll.register(self.err, dc.Poll.IN_FLAGS, self._handle_err)

    def running(self):
        return self.proc is not None

    def kill(self):
        # kill via signal
        self.dying = True
        self.proc.kill()

    def log_name(self):
        return self.config.name

    def reset(self):
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

        if not self.docker_started:
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
            return dc.DockerRunAtomic(
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
            # Run with a different process group to isolate from signals in the parent process.
            preexec_fn=os.setpgrp,
        )
        self.out = self.proc.stdout.fileno()
        self.err = self.proc.stderr.fileno()

        dc.nonblock(self.out)
        dc.nonblock(self.err)

        self.poll.register(self.out, dc.Poll.IN_FLAGS, self._handle_out)
        self.poll.register(self.err, dc.Poll.IN_FLAGS, self._handle_err)

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

    def wait(self):
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

    def kill(self):
        self.dying = True
        kill_cmd = ["docker", "kill", "--signal=TERM", self.config.container_name]
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
            self.logger.log(dc.asbytes(f'"{kill_str}" says:\n') + dc.asbytes(err))

        # If we got canceled, immediately wait on the docker container to exit; there won't be any
        # closing file descriptors to alert us that the container has closed.
        # TODO: configure some async thing to wake up the poll loop via the report_fd so this
        # doesn't block.
        if self.proc is None:
            self.docker_wait()
            self.reset()
            self.state_machine.next_thing()
