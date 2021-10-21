import base64
import os
import time
import typing

import devcluster as dc


class Log:
    """The only parameter to a LogCB.  Part of the devcluster API; property list is append-only."""

    def __init__(self, msg: bytes, stream: str) -> None:
        self.msg = msg
        self.stream = stream

    def to_dict(self) -> typing.Any:
        return {"msg": base64.b64encode(self.msg).decode("utf8"), "stream": self.stream}

    @classmethod
    def from_dict(self, j: typing.Any) -> "Log":
        msg = base64.b64decode(j["msg"])
        stream = j["stream"]
        return Log(msg=msg, stream=stream)


LogCB = typing.Callable[[Log], None]
StreamItem = typing.Tuple[float, bytes]
Streams = typing.Dict[str, typing.List[StreamItem]]


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
        lines = msg.splitlines(keepends=True)

        if self.log_dir is not None:
            with open(os.path.join(self.log_dir, stream + ".log"), "ab") as f:
                f.write(msg)

        for line in lines:
            self.streams[stream].append((now, line))
            log = Log(line, stream)

            for cb in self.callbacks:
                cb(log)

    def add_callback(self, cb: LogCB) -> None:
        self.callbacks.append(cb)

    def remove_callback(self, cb: LogCB) -> None:
        self.callbacks.remove(cb)
