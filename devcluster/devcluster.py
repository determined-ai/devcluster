import os
import time
import select
import subprocess
import sys

import devcluster as dc


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


class Logger:
    def __init__(self, streams, log_dir, init_streams=None, init_index=None):
        all_streams = ["console"] + streams

        if init_streams is None:
            self.streams = {stream: [] for stream in all_streams}
        else:
            self.streams = init_streams

        if init_index is None:
            self.index = {i: stream for i, stream in enumerate(all_streams)}
        else:
            self.index = init_index

        self.callbacks = []

        self.log_dir = log_dir
        if log_dir is not None:
            os.makedirs(self.log_dir, exist_ok=True)

    def log(self, msg, stream="console"):
        """Append to a log stream."""

        now = time.time()
        msg = dc.asbytes(msg)
        self.streams[stream].append((now, msg))

        if self.log_dir is not None:
            with open(os.path.join(self.log_dir, stream + ".log"), "ab") as f:
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
        dc.nonblock(self.pipe_rd)
        dc.nonblock(self.pipe_wr)
        poll.register(self.pipe_rd, Poll.IN_FLAGS, self.handle_pipe)

        self.stages = [dc.DeadStage(self)]
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
                    self.stages[self.state].reset()
                    self.transition(self.state - 1)

        # Notify changes of state.
        new_status = (self.state, self.atomic_op, self.target)
        if self.old_status != new_status:
            state_str, atomic_str, target_str = self.gen_state_cb()
            for cb in self.callbacks:
                cb(state_str, atomic_str, target_str)

            self.old_status = new_status

    def gen_state_cb(self):
        state_str = self.stages[self.state].log_name().upper()
        atomic_str = str(self.atomic_op) if self.atomic_op else ""
        target_str = self.stages[self.target].log_name().upper()
        return state_str, atomic_str, target_str

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
    return b"\x1b[38;2;%d;%d;%dm" % (rgb >> 16, (rgb & 0xFF00) >> 8, rgb & 0xFF)


def fore_num(num):
    return b"\x1b[38;5;%dm" % (num)


def back_rgb(rgb):
    return b"\x1b[48;2;%d;%d;%dm" % (rgb >> 16, (rgb & 0xFF00) >> 8, rgb & 0xFF)


def back_num(num):
    return b"\x1b[48;5;%dm" % (num)


res = b"\x1b[m"


class Console:
    def __init__(self, logger, poll, stages, set_target_cb, quit_cb):
        self.logger = logger
        self.logger.add_callback(self.log_cb)

        self.poll = poll
        self.poll.register(sys.stdin.fileno(), Poll.IN_FLAGS, self.handle_stdin)

        self.stages = ["dead"] + stages
        self.set_target_cb = set_target_cb
        self.quit_cb = quit_cb

        self.status = ("state", "substate", "target")

        # default to all streams active
        self.active_streams = set(self.logger.streams)

        # "scroll" = how many log chunks at the bottom of the current stream to not render
        # TODO: do scroll by lines instead of by log chunks (which can be more or less than a line)
        self.scroll = 0

    def start(self):
        self.redraw()

    def log_cb(self, msg, stream):
        if stream in self.active_streams:
            if self.scroll:
                # don't tail the logs when we are scrolling
                self.scroll += 1
            else:
                self.print_bar(msg)

    def state_cb(self, state, substate, target):
        self.status = (state, substate, target)
        self.print_bar()

    def redraw(self):
        # assume at least 1 in 2 log packets has a newline (we just need to fill up a screen)
        tail_len = get_cols() * 2 + self.scroll

        # build new log output
        new_logs = []
        for stream in self.active_streams:
            new_logs.extend(self.logger.streams[stream][-tail_len:])
        # sort the streams chronologically
        new_logs.sort(key=lambda x: x[0])

        # correct self.scroll if it is longer than the total number of logs we have to render
        self.scroll = min(len(new_logs), self.scroll)

        # don't render logs that we've scrolled past
        new_logs = new_logs[: len(new_logs) - self.scroll]

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
            val = stream not in self.active_streams

        if val:
            self.active_streams.add(stream)
        else:
            self.active_streams.remove(stream)

        self.scroll = 0
        self.redraw()

    def try_set_target(self, idx):
        if idx >= len(self.stages):
            return
        self.set_target_cb(idx)

    def try_toggle_stream(self, idx):
        if idx not in self.logger.index:
            return
        self.set_stream(idx, None)

    def handle_key(self, key):
        if key == "\x03":
            self.quit_cb()
        elif key == "q":
            self.quit_cb()

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

        # scrolling
        elif key == "u":
            self.scroll += 1
            self.redraw()
        elif key == "U":
            self.scroll += 10
            self.redraw()
        elif key == "d":
            self.scroll = max(0, self.scroll - 1)
            self.redraw()
        elif key == "D":
            self.scroll = max(0, self.scroll - 10)
            self.redraw()

    def handle_stdin(self, ev):
        if ev & Poll.IN_FLAGS:
            key = sys.stdin.read(1)
            self.handle_key(key)
        elif ev & Poll.ERR_FLAGS:
            raise ValueError("stdin closed!")

    def place_cursor(self, row, col):
        return b"\x1b[%d;%dH" % (row, col)

    def erase_line(self):
        return b"\x1b[2K"

    def erase_screen(self):
        return b"\x1b[2J"

    def print_bar(self, prebar_bytes=b""):
        cols = get_cols()
        state, _, target = self.status

        blue = fore_num(202) + back_num(17)
        orange = fore_num(17) + back_num(202)

        bar1 = b"state: "
        # printable length
        bar1_len = len(bar1)
        for i, stage in enumerate(self.stages):
            # Active stage is orange
            if stage.lower() == state.lower():
                color = orange
            else:
                color = blue

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
                    self.erase_line(),
                    bar_bytes,
                    restore_cursor(),
                ]
            ),
        )
