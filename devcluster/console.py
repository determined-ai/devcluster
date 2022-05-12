import os
import time
import subprocess
import sys
import typing

import devcluster as dc


_save_cursor = None


def tput(*args: str) -> bytes:
    cmd = ["tput", *args]
    # We can't capture stderr because it causes tput to return incorrect result
    # on macOS.
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = p.communicate()

    if p.returncode != 0:
        err_msg = str(
            f"'{' '.join(cmd)}' exited non-zero({p.returncode}):\n"
            f"stdout: '{out.decode('utf-8').strip()}'\n"
        )

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


class Console:
    def __init__(
        self,
        logger: dc.Logger,
        poll: dc.Poll,
        stages: typing.List[str],
        command_configs: typing.Dict[str, dc.CommandConfig],
        state_machine_handle: dc.StateMachineHandle,
    ):
        self.logger = logger
        self.logger.add_callback(self.log_cb)

        self.poll = poll
        self.poll.register(sys.stdin.fileno(), dc.Poll.IN_FLAGS, self.handle_stdin)

        self.stages = ["dead"] + stages
        self.command_configs = command_configs
        self.state_machine_handle = state_machine_handle

        self.status = dc.Status(
            state_idx=0,
            target_idx=0,
            stages=tuple(dc.StageStatus.DOWN for _ in self.stages),
        )

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

    def log_cb(self, log: dc.Log) -> None:
        if log.stream in self.active_streams:
            if self.scroll:
                # don't tail the logs when we are scrolling
                self.scroll += 1
            else:
                self.print_bar(log.msg)

    def status_cb(self, status: dc.Status) -> None:
        self.status = status
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
            prebar_bytes += (
                dc.fore_num(3) + b"(scrolling, 'x' to return to bottom)" + dc.res
            )

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
        self.state_machine_handle.set_target_or_restart(idx)

    def try_toggle_stream(self, idx: int) -> None:
        if idx not in self.logger.index:
            return
        self.set_stream(idx, None)

    def try_kill_stage(self, idx: int) -> None:
        if idx >= len(self.stages):
            return
        self.state_machine_handle.kill_stage(idx)

    def try_restart_stage(self, idx: int) -> None:
        if idx >= len(self.stages):
            return
        self.state_machine_handle.restart_stage(idx)

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
        self.logger.log(dc.fore_num(color) + dc.asbytes(marker) + dc.res)

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
                self.state_machine_handle.run_command(cmdstr)
            else:
                # Console action.
                if cmdstr == ":quit":
                    self.state_machine_handle.quit()
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
                elif cmdstr.startswith(":kill-stage:"):
                    self.try_kill_stage(int(cmdstr[len(":kill-stage:") :]))
                elif cmdstr.startswith(":restart-stage:"):
                    self.try_restart_stage(int(cmdstr[len(":restart-stage:") :]))
                else:
                    self.logger.log(
                        dc.fore_num(9)
                        + dc.asbytes(f'"{cmdstr}" is not a devcluster command\n')
                        + dc.res
                    )

        # Default keybindings
        elif key == "\x03":  # ctrl-c
            self.state_machine_handle.quit()
        elif key == "\x04":  # ctrl-d
            self.state_machine_handle.dump_state()
        elif key == "q":
            self.state_machine_handle.quit()
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
        else:
            self.logger.log(
                dc.fore_num(9)
                + dc.asbytes(f'"{key}" is not a known shortcut\n')
                + dc.res
            )

    def handle_stdin(self, ev: int, _: int) -> None:
        if ev & dc.Poll.IN_FLAGS:
            key = sys.stdin.read(1)
            self.handle_key(key)
        elif ev & dc.Poll.ERR_FLAGS:
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

        # When change_scroll_region is available, we only write the bar when we have to modify it.
        new_bar_state = (
            get_cols(),
            get_rows(),
            self.status.state_idx,
            self.status.target_idx,
            tuple(s.value for s in self.status.stages),
            tuple(self.active_streams),
        )
        if dc.has_csr() and self.last_bar_state == new_bar_state:
            if prebar_bytes:
                os.write(sys.stdout.fileno(), prebar_bytes)
            return
        self.last_bar_state = new_bar_state

        blue = dc.fore_num(202) + dc.back_num(17)
        orange = dc.fore_num(17) + dc.back_num(202)
        red = dc.fore_num(16) + dc.back_num(1)

        bar1 = b"state: "
        # printable length
        bar1_len = len(bar1)

        # Decide on colors.
        if all(s == dc.StageStatus.DOWN for s in self.status.stages):
            # Special coloring rules: only when nothing is running is the DEAD
            # state colored orange.
            colors = [orange] + [blue] * (len(self.stages) - 1)
        else:
            colors = [blue]
            for stage_status in self.status.stages[1:]:
                if stage_status == dc.StageStatus.DOWN:
                    colors.append(blue)
                elif stage_status == dc.StageStatus.CRASHED:
                    colors.append(red)
                else:
                    colors.append(orange)

        for i, (stage, color) in enumerate(zip(self.stages, colors)):
            # Target stage is donoted with <
            if i == self.status.target_idx:
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
            [dc.back_num(17), dc.fore_num(202), bar1, dc.fore_num(7), bar2, dc.res]
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
