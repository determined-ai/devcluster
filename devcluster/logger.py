import os
import time
import typing

import devcluster as dc


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
