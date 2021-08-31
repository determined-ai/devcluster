import os
import time
import select
import subprocess
import sys
import typing

import devcluster as dc


_save_cursor = None


def tput(*args: str) -> bytes:
    cmd = ["tput", *args]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()

    if p.returncode != 0:
        err_str = err.decode("utf-8").strip()
        err_msg = str(
            f"'{' '.join(cmd)}' exited non-zero({p.returncode}):\n"
            f"stdout: '{out.decode('utf-8').strip()}'\nstderr: '{err_str}'\n"
        )
        if "unknown terminal" in err_str:
            err_msg += str(
                "If using a conda environment you may need to "
                "'export TERMINFO=\"/usr/share/terminfo\"'"
            )
        raise RuntimeError(err_msg)

    return out.strip()


def save_cursor() -> bytes:
    global _save_cursor
    if _save_cursor is None:
        _save_cursor = tput("sc")
    return _save_cursor


_restore_cursor = None


def restore_cursor() -> bytes:
    global _restore_cursor
    if _restore_cursor is None:
        _restore_cursor = tput("rc")
    return _restore_cursor


_cols = None


def get_cols(recheck: bool = False) -> int:
    global _cols
    if _cols is None or recheck:
        _cols = int(tput("cols"))
    return _cols


_rows = None


def get_rows(recheck: bool = False) -> int:
    global _rows
    if _rows is None or recheck:
        _rows = int(tput("lines"))
    return _rows


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


def separate_lines(msg: bytes) -> typing.List[bytes]:
    lines = msg.split(b"\n")
    return [l + b"\n" for l in lines[:-1]] + ([lines[-1]] if lines[-1] else [])


StreamItem = typing.Tuple[float, bytes]
Streams = typing.Dict[str, typing.List[StreamItem]]
LogCB = typing.Callable[[bytes, str], None]


class Logger:
    def __init__(
        self,
        streams: typing.List[str],
        log_dir: typing.Optional[str],
        init_streams: typing.Optional[Streams] = None,
        init_index: typing.Optional[typing.Dict[int, str]] = None,
    ):
        all_streams = ["console"] + streams

        if init_streams is None:
            self.streams = {stream: [] for stream in all_streams}  # type: Streams
        else:
            self.streams = init_streams

        if init_index is None:
            self.index = {i: stream for i, stream in enumerate(all_streams)}
        else:
            self.index = init_index

        self.callbacks = []  # type: typing.List[LogCB]

        self.log_dir = log_dir
        if log_dir is not None:
            os.makedirs(log_dir, exist_ok=True)

    def log(self, msg: dc.Text, stream: str = "console") -> None:
        """Append to a log stream."""

        now = time.time()
        msg = dc.asbytes(msg)

        # Split the message along embedded line breaks, to improve scrolling granularity.
        # They will all have the same timestamp, but python sorting is stable so that is OK.
        lines = separate_lines(msg)

        if self.log_dir is not None:
            with open(os.path.join(self.log_dir, stream + ".log"), "ab") as f:
                f.write(msg)

        for line in lines:
            self.streams[stream].append((now, line))

            for cb in self.callbacks:
                cb(line, stream)

    def add_callback(self, cb: LogCB) -> None:
        self.callbacks.append(cb)

    def remove_callback(self, cb: LogCB) -> None:
        self.callbacks.remove(cb)


CommandEndCB = typing.Callable[["Command"], None]


class Command:
    def __init__(
        self,
        command: typing.Union[str, typing.List[str]],
        logger: Logger,
        poll: Poll,
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
            fore_num(3) + dc.asbytes(b"starting `%s`\n" % self.cmd_str) + res
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
        self.poll.register(self.out, Poll.IN_FLAGS, self._handle_out)
        self.poll.register(self.err, Poll.IN_FLAGS, self._handle_err)

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
        self.logger.log(fore_num(3) + b"killing %s...\n" % self.cmd_str + res)
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
            self.logger.log(fore_num(3) + msg + res)
        elif ret == 0:
            msg = b"`%s` complete (%.2fs)\n" % (self.cmd_str, duration)
            self.logger.log(fore_num(3) + msg + res)
        else:
            msg = b"`%s` failed with %d (%.2fs)\n" % (self.cmd_str, ret, duration)
            self.logger.log(fore_num(1) + msg + res)
        self.end_cb(self)


# StateCB args are: state_str, atomic_str, target_str
StateCB = typing.Callable[[str, str, str], None]
ReportCB = typing.Callable[[], None]
StateMachineStatus = typing.Tuple[int, typing.Optional["dc.AtomicOperation"], int]


class StateMachine:
    def __init__(
        self,
        logger: Logger,
        poll: Poll,
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
        poll.register(self.pipe_rd, Poll.IN_FLAGS, self.handle_pipe)

        self.stages = [dc.DeadStage(self)]  # type: typing.List[dc.Stage]
        self.state = 0
        self.target = 0

        self.old_status = (
            self.state,
            self.atomic_op,
            self.target,
        )  # type: StateMachineStatus

        self.callbacks = []  # type: typing.List[StateCB]

        self.report_callbacks = {}  # type: typing.Dict[str, ReportCB]

        # commands maps the UI key to the command
        self.commands = {}  # type: typing.Dict[str, Command]

    def run_command(self, cmd_str: str) -> None:
        if self.quitting:
            msg = "ignoring command while we are quitting\n"
            self.logger.log(fore_num(3) + dc.asbytes(msg) + res)
            return
        if cmd_str in self.commands:
            msg = f"command {cmd_str} is still running, please wait...\n"
            self.logger.log(fore_num(3) + dc.asbytes(msg) + res)
            return

        try:
            command = Command(
                cmd_str,
                self.logger,
                self.poll,
                self.command_end,
            )
        except FileNotFoundError as e:
            self.logger.log(fore_num(1) + dc.asbytes(str(e)) + res)
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

    def add_callback(self, cb: StateCB) -> None:
        self.callbacks.append(cb)

    def add_report_callback(self, report_msg: str, cb: ReportCB) -> None:
        self.report_callbacks[report_msg] = cb

    def advance_stage(self) -> None:
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

    def next_thing(self) -> None:
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
                    self.stages[self.state].reset()
                    self.transition(self.state - 1)

        # Notify changes of state.
        new_status = (self.state, self.atomic_op, self.target)
        if self.old_status != new_status:
            state_str, atomic_str, target_str = self.gen_state_cb()
            for cb in self.callbacks:
                cb(state_str, atomic_str, target_str)

            self.old_status = new_status

    def gen_state_cb(self) -> typing.Tuple[str, str, str]:
        state_str = self.stages[self.state].log_name().upper()
        atomic_str = str(self.atomic_op) if self.atomic_op else ""
        target_str = self.stages[self.target].log_name().upper()
        return state_str, atomic_str, target_str

    def set_target(self, target: int) -> None:
        """For when you choose a new target state."""
        if target == self.target:
            return

        self.target = target

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
        if ev & Poll.IN_FLAGS:
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
                    # set the target state to be one less than wherever-we-are
                    self.target = min(self.state - 1, self.target)

                self.next_thing()
            return

        if ev & Poll.ERR_FLAGS:
            # Just die.
            raise ValueError("pipe failed!")

    def should_run(self) -> bool:
        return not (self.quitting and self.state == 0 and len(self.commands) == 0)


def fore_rgb(rgb: int) -> bytes:
    return b"\x1b[38;2;%d;%d;%dm" % (rgb >> 16, (rgb & 0xFF00) >> 8, rgb & 0xFF)


def fore_num(num: int) -> bytes:
    return b"\x1b[38;5;%dm" % (num)


def back_rgb(rgb: int) -> bytes:
    return b"\x1b[48;2;%d;%d;%dm" % (rgb >> 16, (rgb & 0xFF00) >> 8, rgb & 0xFF)


def back_num(num: int) -> bytes:
    return b"\x1b[48;5;%dm" % (num)


res = b"\x1b[m"


class Console:
    def __init__(
        self,
        logger: Logger,
        poll: Poll,
        stages: typing.List[str],
        set_target_cb: typing.Callable[[int], None],
        command_configs: typing.Dict[str, dc.CommandConfig],
        run_command_cb: typing.Callable[[str], None],
        quit_cb: typing.Callable[[], None],
    ):
        self.logger = logger
        self.logger.add_callback(self.log_cb)

        self.poll = poll
        self.poll.register(sys.stdin.fileno(), Poll.IN_FLAGS, self.handle_stdin)

        self.stages = ["dead"] + stages
        self.set_target_cb = set_target_cb
        self.command_configs = command_configs
        self.run_command_cb = run_command_cb
        self.quit_cb = quit_cb

        self.status = ("state", "substate", "target")

        # default to all streams active
        self.active_streams = set(self.logger.streams)

        # "scroll" = how many log chunks at the bottom of the current stream to not render
        # TODO: do scroll by lines instead of by log chunks (which can be more or less than a line)
        self.scroll = 0

        # Cycle through marker colors.
        self.marker_color = 0

        self.last_bar_state = None  # type: typing.Any

    def start(self) -> None:
        self.redraw()

    def log_cb(self, msg: bytes, stream: str) -> None:
        if stream in self.active_streams:
            if self.scroll:
                # don't tail the logs when we are scrolling
                self.scroll += 1
            else:
                self.print_bar(msg)

    def state_cb(self, state: str, substate: str, target: str) -> None:
        self.status = (state, substate, target)
        self.print_bar()

    def redraw(self) -> None:
        # build new log output
        new_logs = []
        for stream in self.active_streams:
            new_logs.extend(self.logger.streams[stream])
        # sort the streams chronologically
        new_logs.sort(key=lambda x: x[0])

        # correct self.scroll if it is longer than the total number of logs we have to render
        self.scroll = min(len(new_logs), self.scroll)

        # don't render logs that we've scrolled past
        new_logs = new_logs[: len(new_logs) - self.scroll]

        # assume at least 1 in 2 log packets has a newline (we just need to fill up a screen)
        tail_len = get_cols() * 2
        new_logs = new_logs[-tail_len:]

        # Empty the logs with newlines because it reduces flicker, especially for has_csr()
        prebar_bytes = (
            # go to the bottom of the scroll region...
            self.place_cursor(get_rows(), 1)
            # write enough newlines to flush the screen...
            + b"\n" * (get_rows() - 2)
            # go to the top of the scroll region
            + self.place_cursor(3, 1)
            # write the logs out
            + b"".join(x[1] for x in new_logs)
        )

        if self.scroll:
            prebar_bytes += fore_num(3) + b"(scrolling, 'x' to return to bottom)" + res

        self.print_bar(prebar_bytes)

    def handle_window_change(self) -> None:
        """This should get called some time after a SIGWINCH."""
        _ = get_cols(True)
        _ = get_rows(True)
        if dc.has_csr():
            # Scrolling region has to be reconfigured.
            os.write(sys.stdout.fileno(), b"\x1b[3r")
        self.redraw()

    def set_stream(
        self, stream: typing.Union[str, int], val: typing.Optional[int]
    ) -> None:
        if isinstance(stream, int):
            stream = self.logger.index[stream]

        if val is None:
            # toggle when val is None
            val = stream not in self.active_streams

        if val:
            self.active_streams.add(stream)
        else:
            self.active_streams.remove(stream)

        self.scroll = 0
        self.redraw()

    def try_set_target(self, idx: int) -> None:
        if idx >= len(self.stages):
            return
        self.set_target_cb(idx)

    def try_toggle_stream(self, idx: int) -> None:
        if idx not in self.logger.index:
            return
        self.set_stream(idx, None)

    def act_scroll(self, val: int) -> None:
        self.scroll = max(0, self.scroll + val)
        self.redraw()

    def act_scroll_reset(self) -> None:
        if self.scroll:
            self.scroll = 0
            self.redraw()

    def act_marker(self) -> None:
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        marker = f"-- {time.time()} -- {t} --------------\n"
        color = (self.marker_color + 3) % 5 + 10
        self.marker_color += 1
        self.logger.log(fore_num(color) + dc.asbytes(marker) + res)

    def act_noop(self) -> None:
        """Used to ignore a default keybinding."""
        pass

    def handle_key(self, key: str) -> None:
        # 0-9: set target state
        if key == "0" or key == "`":
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

        # customizable commands
        elif key in self.command_configs:
            cmdstr = self.command_configs[key].command
            if not cmdstr.startswith(":"):
                self.run_command_cb(cmdstr)
            else:
                # Console action.
                if cmdstr == ":quit":
                    self.quit_cb()
                elif cmdstr == ":scroll-up":
                    self.act_scroll(1)
                elif cmdstr == ":scroll-up-10":
                    self.act_scroll(10)
                elif cmdstr == ":scroll-dn":
                    self.act_scroll(-1)
                elif cmdstr == ":scroll-dn-10":
                    self.act_scroll(-10)
                elif cmdstr == ":scroll-reset":
                    self.act_scroll_reset()
                elif cmdstr == ":marker":
                    self.act_marker()
                elif cmdstr == ":noop":
                    self.act_noop()
                else:
                    self.logger.log(
                        fore_num(9)
                        + dc.asbytes(f'"{cmdstr}" is not a devcluster command\n')
                        + res
                    )

        # Default keybindings
        elif key == "\x03":
            self.quit_cb()
        elif key == "q":
            self.quit_cb()
        elif key == "k":
            self.act_scroll(1)
        elif key == "u":
            self.act_scroll(10)
        elif key == "j":
            self.act_scroll(-1)
        elif key == "d":
            self.act_scroll(-10)
        elif key == "x":
            self.act_scroll_reset()
        elif key == " ":
            self.act_marker()

    def handle_stdin(self, ev: int, _: int) -> None:
        if ev & Poll.IN_FLAGS:
            key = sys.stdin.read(1)
            self.handle_key(key)
        elif ev & Poll.ERR_FLAGS:
            raise ValueError("stdin closed!")

    def place_cursor(self, row: int, col: int) -> bytes:
        return b"\x1b[%d;%dH" % (row, col)

    def erase_line(self) -> bytes:
        return b"\x1b[2K"

    def erase_screen(self) -> bytes:
        return b"\x1b[2J"

    def erase_after(self) -> bytes:
        return b"\x1b[J"

    def print_bar(self, prebar_bytes: bytes = b"") -> None:
        cols = get_cols()
        state, _, target = self.status

        # When change_scroll_region is available, we only write the bar when we have to modify it.
        new_bar_state = (
            get_cols(),
            get_rows(),
            state,
            target,
            tuple(self.active_streams),
        )
        if dc.has_csr() and self.last_bar_state == new_bar_state:
            if prebar_bytes:
                os.write(sys.stdout.fileno(), prebar_bytes)
            return
        self.last_bar_state = new_bar_state

        blue = fore_num(202) + back_num(17)
        orange = fore_num(17) + back_num(202)

        bar1 = b"state: "
        # printable length
        bar1_len = len(bar1)

        # Decide on colors.
        if state.lower() == "dead":
            colors = [orange] + [blue] * (len(self.stages) - 1)
        else:
            colors = [blue] * len(self.stages)
            for i in range(1, len(self.stages)):
                colors[i] = orange
                if self.stages[i].lower() == state.lower():
                    break

        for i, (stage, color) in enumerate(zip(self.stages, colors)):
            # Target stage is donoted with <
            if stage.lower() == target.lower():
                post = b"< "
            else:
                post = b"  "

            # TODO: truncate this properly for narrow consoles
            binding = b"  (`)" if i == 0 else b"  (%d)" % i
            bar1 += binding + color + dc.asbytes(stage.upper()) + blue + post
            bar1_len += 7 + len(stage)

        # fill bar
        bar1 += b" " * (cols - bar1_len)

        bar2 = blue + b"logs: "
        bar2_len = 6

        def get_binding(idx: int) -> bytes:
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

        def get_log_name(name: str) -> str:
            return "console" if name == "dead" else name

        for i, stage in enumerate(self.stages):
            binding = get_binding(i)
            name = get_log_name(stage)

            if name in self.active_streams:
                color = orange
            else:
                color = blue

            # TODO: truncate this properly for narrow consoles
            bar2 += binding + color + dc.asbytes(name.upper()) + blue + b"    "
            bar2_len += 7 + len(name)

        bar2 += b" " * (cols - bar2_len)

        bar_bytes = b"".join(
            [back_num(17), fore_num(202), bar1, fore_num(7), bar2, res]
        )

        os.write(
            sys.stdout.fileno(),
            b"".join(
                [
                    prebar_bytes,
                    save_cursor(),
                    self.place_cursor(1, 1),
                    bar_bytes,
                    restore_cursor(),
                ]
            ),
        )
