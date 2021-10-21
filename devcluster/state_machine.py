import json
import os
import time
import subprocess
import typing

import devcluster as dc


CommandEndCB = typing.Callable[["Command"], None]


class Command:
    def __init__(
        self,
        command: typing.Union[str, typing.List[str]],
        logger: dc.Logger,
        poll: dc.Poll,
        end_cb: CommandEndCB,
    ):
        self.command = command
        self.cmd_str = dc.asbytes(
            command if isinstance(command, str) else subprocess.list2cmdline(command)
        )
        self.logger = logger
        self.poll = poll
        self.end_cb = end_cb
        self.logger.log(
            dc.fore_num(3) + dc.asbytes(b"starting `%s`\n" % self.cmd_str) + dc.res
        )
        self.start = time.time()
        self.p = subprocess.Popen(
            command,
            shell=isinstance(command, str),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert self.p.stdout and self.p.stderr
        self.out = self.p.stdout.fileno()  # type: typing.Optional[int]
        self.err = self.p.stderr.fileno()  # type: typing.Optional[int]
        self.poll.register(self.out, dc.Poll.IN_FLAGS, self._handle_out)
        self.poll.register(self.err, dc.Poll.IN_FLAGS, self._handle_err)

        self.killing = False

    def _handle_out(self, ev: int, _: int) -> None:
        assert self.out
        if ev & dc.Poll.IN_FLAGS:
            self.logger.log(os.read(self.out, 4096))
        if ev & dc.Poll.ERR_FLAGS:
            self.poll.unregister(self._handle_out)
            os.close(self.out)
            self.out = None
            self._maybe_wait()

    def _handle_err(self, ev: int, _: int) -> None:
        assert self.err
        if ev & dc.Poll.IN_FLAGS:
            self.logger.log(os.read(self.err, 4096))
        if ev & dc.Poll.ERR_FLAGS:
            self.poll.unregister(self._handle_err)
            os.close(self.err)
            self.err = None
            self._maybe_wait()

    def cancel(self) -> None:
        self.logger.log(dc.fore_num(3) + b"killing %s...\n" % self.cmd_str + dc.res)
        self.killing = True
        self.p.kill()

    def _maybe_wait(self) -> None:
        """wait() on self.p if both stdout and stderr are empty."""
        if self.out is not None or self.err is not None:
            return
        ret = self.p.wait()
        duration = time.time() - self.start
        if self.killing:
            msg = b"`%s` killed after %.2fs\n" % (self.cmd_str, duration)
            self.logger.log(dc.fore_num(3) + msg + dc.res)
        elif ret == 0:
            msg = b"`%s` complete (%.2fs)\n" % (self.cmd_str, duration)
            self.logger.log(dc.fore_num(3) + msg + dc.res)
        else:
            msg = b"`%s` failed with %d (%.2fs)\n" % (self.cmd_str, ret, duration)
            self.logger.log(dc.fore_num(1) + msg + dc.res)
        self.end_cb(self)


class Status:
    """
    The only parameter to a StatusCB.  Part of the devcluster API; property list is append-only.
    """

    def __init__(
        self,
        state_idx: int,
        state_str: str,
        target_idx: int,
        target_str: str,
        atomic_str: str,
        crashed: typing.Sequence[bool],
    ) -> None:
        self.state_idx = state_idx
        self.state_str = state_str
        self.target_idx = target_idx
        self.target_str = target_str
        self.atomic_str = atomic_str
        self.crashed = crashed

    def to_dict(self) -> typing.Any:
        return vars(self)

    @classmethod
    def from_dict(self, j: typing.Any) -> "Status":
        return Status(**j)


StatusCB = typing.Callable[[Status], None]
ReportCB = typing.Callable[[], None]


class StateMachineHandle:
    """
    The StateMachine maybe be exposed to the Console directly, or over the network.
    """

    def __init__(
        self,
        set_target_or_restart: typing.Callable[[int], None],
        run_command: typing.Callable[[str], None],
        quit_cb: typing.Callable[[], None],
        dump_state: typing.Callable[[], None],
    ):
        self.set_target_or_restart = set_target_or_restart
        self.run_command = run_command
        self.quit = quit_cb
        self.dump_state = dump_state


class StateMachine:
    def __init__(
        self,
        logger: dc.Logger,
        poll: dc.Poll,
        command_configs: typing.Dict[str, dc.CommandConfig],
    ) -> None:
        self.logger = logger
        self.poll = poll
        self.command_configs = command_configs

        self.quitting = False

        # atomic_op is intermediate steps like calling `make` or connecting to a server.
        # We only support having one run at a time (since they're atomic...)
        self.atomic_op = None  # type: typing.Optional[dc.AtomicOperation]

        # the pipe is used by the atomic_op to pass messages to the poll loop
        self.pipe_rd, self.pipe_wr = os.pipe()
        dc.nonblock(self.pipe_rd)
        dc.nonblock(self.pipe_wr)
        poll.register(self.pipe_rd, dc.Poll.IN_FLAGS, self.handle_pipe)

        self.stages = [dc.DeadStage(self)]  # type: typing.List[dc.Stage]
        # state is the current stage we are running
        self.state = 0
        # target is the stage the user has requested we run
        self.target = 0
        # standing_up is the stage we are standing up (there is only one at a time)
        self.standing_up = None  # type: typing.Optional[int]

        # While a Stage can track if it has crashed(), only the StateMachine knows if an
        # AtomicOp for a stage has failed.  We track that per-stage.
        self.atomic_crashed = [False]

        # we queue up restart requests to handle them one at a time.
        self.want_restarts = []  # type: typing.List[int]

        self.old_status = None  # type: typing.Any

        self.callbacks = []  # type: typing.List[StatusCB]

        self.report_callbacks = {}  # type: typing.Dict[str, ReportCB]

        # commands maps the UI key to the command
        self.commands = {}  # type: typing.Dict[str, Command]

    def dump_state(self) -> None:
        """Useful for debugging the state machine if the state machine deadlocks."""
        state = {
            "state": self.state,
            "target": self.target,
            "standing_up": self.standing_up,
            "atomic_op": self.atomic_op and str(self.atomic_op),
            "stages": [
                {
                    "name": stage.log_name(),
                    "killable": stage.killable(),
                    "running": stage.running(),
                    "crashed": stage.crashed(),
                    "atomic_crashed": self.atomic_crashed[i],
                }
                for i, stage in enumerate(self.stages)
            ],
        }
        self.logger.log("state_machine: " + json.dumps(state, indent="  ") + "\n")

    def run_command(self, cmd_str: str) -> None:
        if self.quitting:
            msg = "ignoring command while we are quitting\n"
            self.logger.log(dc.fore_num(3) + dc.asbytes(msg) + dc.res)
            return
        if cmd_str in self.commands:
            msg = f"command {cmd_str} is still running, please wait...\n"
            self.logger.log(dc.fore_num(3) + dc.asbytes(msg) + dc.res)
            return

        try:
            command = Command(
                cmd_str,
                self.logger,
                self.poll,
                self.command_end,
            )
        except FileNotFoundError as e:
            self.logger.log(dc.fore_num(1) + dc.asbytes(str(e)) + dc.res)
        self.commands[cmd_str] = command

    def command_end(self, command: Command) -> None:
        for key in self.commands:
            if self.commands[key] == command:
                del self.commands[key]
                break

    def get_report_fd(self) -> int:
        return self.pipe_wr

    def add_stage(self, stage: "dc.Stage") -> None:
        self.stages.append(stage)
        self.atomic_crashed.append(False)

    def add_callback(self, cb: StatusCB) -> None:
        self.callbacks.append(cb)

    def add_report_callback(self, report_msg: str, cb: ReportCB) -> None:
        self.report_callbacks[report_msg] = cb

    def is_crashed(self, stage_id: int) -> bool:
        """
        Logical OR of the crash information tracked by each Stage
        with the AtomicOp crash information that we track here.
        """
        return self.stages[stage_id].crashed() or self.atomic_crashed[stage_id]

    def advance_stage(self, stage_id: int) -> None:
        """
        Either:
          - start the next precommand,
          - start the real command (and maybe a postcommand), or
          - start the next postcommand
        """
        if self.is_crashed(stage_id):
            if self.atomic_op is not None:
                # Cancel any pending atomic ops.
                self.atomic_op.cancel()
            return

        # Never do anything if there is an atomic operation to finish.
        if self.atomic_op is not None:
            return

        process = self.stages[stage_id]
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

    def next_thing(self) -> None:
        """
        Should be called either when:
          - an atomic operation completes
          - a long-running process is closed
          - a stage has crashed
        """

        while True:
            # Execute any stages that we are currently in the process of standing up.
            if self.standing_up is not None:
                # If standing_up won't be running when we reach the target state, cancel it.
                if self.standing_up > self.target:
                    if self.atomic_op is None:
                        # Safe to cancel further work for this stage.
                        self.standing_up = None
                        # Check for more work.
                        continue
                    else:
                        self.atomic_op.cancel()
                else:
                    self.advance_stage(self.standing_up)
                    if self.atomic_op is None:
                        # Done standing up this stage.
                        self.standing_up = None
                        # Check for more work.
                        continue

            # Regress state.
            elif self.state > self.target:
                # Do we need to kill the stage?
                if self.stages[self.state].killable():
                    self.stages[self.state].kill()
                # Is it done running? (though usually if it was killable it won't be yet)
                if not self.stages[self.state].running():
                    self.stages[self.state].reset()
                    self.atomic_crashed[self.state] = False
                    self.state -= 1
                    # Check for more work.
                    continue

            # Check for any pending restarts.
            elif self.want_restarts:
                want_restart = self.want_restarts[0]
                if self.stages[want_restart].running():
                    # Need to shut down before restarting.
                    # This arises if a postcmd has failed but the stage kept running.
                    if self.stages[self.state].killable():
                        self.stages[self.state].kill()
                else:
                    self.stages[want_restart].reset()
                    self.atomic_crashed[want_restart] = False
                    self.standing_up = want_restart
                    self.want_restarts.pop(0)
                    # Start executing on this restart.
                    continue

            # Advance state.
            elif self.state < self.target:
                # Only advance automatically if there are no detected crashes.
                if not any(self.is_crashed(i) for i in range(len(self.stages))):
                    self.state += 1
                    self.standing_up = self.state
                    # Check for more work.
                    continue

            # exiting the loop is the "normal" behavior
            break

        # Notify changes of state.
        crashed = tuple(self.is_crashed(i) for i in range(len(self.stages)))
        new_status = (self.state, self.atomic_op, self.target, crashed)
        if self.old_status != new_status:
            status = self.gen_state_cb()
            for cb in self.callbacks:
                cb(status)

            self.old_status = new_status

    def gen_state_cb(self) -> Status:
        return Status(
            state_idx=self.state,
            state_str=self.stages[self.state].log_name(),
            target_idx=self.target,
            target_str=self.stages[self.target].log_name(),
            atomic_str=str(self.atomic_op),
            crashed=tuple(self.is_crashed(i) for i in range(len(self.stages))),
        )

    def set_target_or_restart(self, target: int) -> None:
        """
        The UI reacts differently to keys based on if the stage is crashed, but
        the UI can't actually read the crash state to distinguish key inputs
        without a race condition, so that decision is made here.
        """
        if target < len(self.stages) and self.is_crashed(target):
            self.restart_stage(target)
        else:
            self.set_target(target)

    def restart_stage(self, target: int) -> None:
        if not self.is_crashed(target):
            raise ValueError(f"stage[{target}] is not crashed, and so cannot restart")
        self.want_restarts.append(target)
        self.next_thing()

    def set_target(self, target: int) -> None:
        """For when you choose a new target state."""
        if target == self.target:
            return

        self.target = target

        self.next_thing()

    def report_crash(self) -> None:
        self.next_thing()

    def quit(self) -> None:
        # Raise an error on the second try.
        if self.quitting:
            raise ValueError("quitting forcibly")
        # Exit gracefully on the first try.
        self.logger.log("quitting...\n")
        self.quitting = True
        self.set_target(0)
        for command in self.commands.values():
            command.cancel()

    def transition(self, new_state: int) -> None:
        """For when you arrive at a new state."""
        self.state = new_state

        self.next_thing()

    def handle_pipe(self, ev: int, _: int) -> None:
        if ev & dc.Poll.IN_FLAGS:
            msg = os.read(self.pipe_rd, 4096).decode("utf8")
            for c in msg:
                # check if we have a listener for this callback
                cb = self.report_callbacks.get(c)
                if cb is not None:
                    cb()
                    continue

                assert self.atomic_op
                self.atomic_op.join()
                self.atomic_op = None
                if c == "F":  # Fail
                    # Mark this atomic as crashed.
                    assert self.standing_up is not None
                    self.atomic_crashed[self.standing_up] = True

                self.next_thing()
            return

        if ev & dc.Poll.ERR_FLAGS:
            # Just die.
            raise ValueError("pipe failed!")

    def should_run(self) -> bool:
        return not (self.quitting and self.state == 0 and len(self.commands) == 0)
