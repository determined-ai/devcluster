import contextlib
import json
import os
import queue
import re
import shutil
import signal as _signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import typing

import yaml

import devcluster as dc


class DevclusterError(Exception):
    """DevclusterError indicates that the devcluster subprocess died."""

    pass


class StageError(Exception):
    """
    StageError indicates that a particular stage we were interested in
    crashed while we were waiting for some condition.
    """

    pass


Event = typing.Union[dc.Log, dc.Status]
EventCB = typing.Callable[[Event], None]


class Devcluster:
    """
    Devcluster is the Python API for using devcluster.

    Because the working directory is important to devcluster, we choose to run
    devcluster in a subprocess and interact with it via a socket.
    """

    def __init__(
        self,
        config: typing.Union[str, typing.Dict[str, typing.Any]],
        initial_target_stage: typing.Union[int, str] = 0,
        env: typing.Optional[typing.Dict[str, str]] = None,
        event_cbs: typing.Sequence[EventCB] = (),
    ) -> None:
        self._env = env
        self._initial_target_stage = initial_target_stage

        if isinstance(config, str):
            with open(config) as f:
                self._config_text = f.read()
            self._config_dict = yaml.safe_load(self._config_text)
        else:
            self._config_dict = config
            self._config_text = yaml.dump(config)

        cfg = dc.Config(self._config_dict)
        self._stage_names = ["dead"] + [stage.name.lower() for stage in cfg.stages]
        self._stage_map = {
            name: i for i, name in enumerate(self._stage_names)
        }  # type: typing.Dict[typing.Union[str, int], int]
        self._stage_map.update({i: i for i in range(len(self._stage_names))})
        self._target_map = {
            i: name for i, name in enumerate(self._stage_names)
        }  # type: typing.Dict[typing.Union[str, int], str]
        self._target_map.update(
            {name: name for i, name in enumerate(self._stage_names)}
        )

        self._tmp = None  # type: typing.Optional[str]
        self._proc = None  # type: typing.Optional[subprocess.Popen]
        self._sock = None  # type: typing.Optional[socket.socket]
        self._sock_failed = False
        self._conn_thread = None  # type: typing.Optional[threading.Thread]

        self._lock = threading.Lock()
        self._status = None  # type: typing.Optional[dc.Status]
        self._queues = set()  # type: typing.Set[queue.Queue]
        self._event_cbs = event_cbs

    @property
    def stages(self) -> typing.List[str]:
        """Return the list of stage names configured for this cluster."""
        return self._stage_names

    def get_status(self) -> dc.Status:
        """Return the latest dc.Status object known to the cluster."""
        assert self._status, "you can't call get_state() unless Devcluster is started"
        return self._status

    def health_check(self) -> bool:
        """
        Raise a DevclusterError if the devcluster process has died.
        """
        if self._proc is None:
            raise ValueError(
                "health_check() is not meaningful before .start() or after .close()"
            )
        if self._proc.poll() is not None:
            raise DevclusterError("process has died")
        if self._sock_failed:
            raise DevclusterError("connection to socket has broken")
        # return True so it's easy to chain with `and`.
        return True

    def close(self) -> None:
        if self._tmp is not None:
            shutil.rmtree(self._tmp)
            self._tmp = None

        if self._proc is not None:
            # Shut down devcluster nicely.
            self._proc.send_signal(_signal.Signals.SIGTERM)
            try:
                _ = self._proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self._proc.send_signal(_signal.Signals.SIGKILL)
                _ = self._proc.wait()
            assert self._proc.stderr
            # Capture any stacktraces.
            err = self._proc.stderr.read()
            if err:
                print("detected devcluster output on stderr:", file=sys.stderr)
                print(err.decode("utf8"), file=sys.stderr)
            self._proc = None

        if self._conn_thread is not None:
            # We already killed the process, so this should exit shortly.
            self._conn_thread.join()

        if self._sock is not None:
            self._sock.close()
            self._sock = None

    def start(self) -> None:
        """
        Start the devcluster subprocess, and wait for the configured initial target state.
        """
        try:
            self._start()
        except Exception:
            self.close()
            raise

    def _start(self) -> None:
        self._tmp = tempfile.mkdtemp()
        config_path = os.path.join(self._tmp, "config.yaml")
        sock_path = os.path.join(self._tmp, "sock")
        with open(config_path, "w") as f:
            f.write(self._config_text)

        env = self._env and dict(**self._env) or dict(**os.environ)
        env["PYTHONUNBUFFERED"] = "1"
        cmd = [
            sys.executable,
            "-m",
            "devcluster",
            "--config",
            config_path,
            "--quiet",
            "--target-stage",
            "dead",
            "--listen",
            sock_path,
        ]
        self._proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=env,
        )

        # Wait for the listener to be ready.
        assert self._proc.stderr
        stderr = b""
        for line in self._proc.stderr:
            if f"devcluster is listening on {sock_path}".encode("utf8") in line:
                break
            stderr += line
        else:
            self.close()
            raise RuntimeError(
                "devcluster failed on startup:\n" + stderr.decode("utf8")
            )

        # Connect to the devcluster subprocess and start processing events.
        self._sock = dc.connection_from_spec(sock_path)
        init, buf = dc.get_init_from_server(self._sock)
        self._status = dc.Status.from_dict(init["first_status"])

        self._conn_thread = threading.Thread(
            target=self._read_sock_in_thread, args=(self._sock, buf)
        )
        self._conn_thread.start()

        self.set_target(self._initial_target_stage, wait=True)

    def _read_sock_in_thread(self, sock: socket.socket, initial_buf: bytes) -> None:
        """
        This connection operates on a separate background_thread.
        """
        buffer = initial_buf
        try:
            while True:
                try:
                    new_bytes = sock.recv(4096)
                except ConnectionError:
                    break
                if len(new_bytes) == 0:
                    break
                buffer += new_bytes
                while b"\n" in buffer:
                    # Separate buffer into jmsgs.
                    end = buffer.find(b"\n")
                    line = buffer[:end]
                    buffer = buffer[end + 1 :]
                    jmsg = json.loads(line.decode("utf8"))
                    # Process the jmsg.
                    for k, v in jmsg.items():
                        if k == "log_cb":
                            log = dc.Log.from_dict(v)
                            self._event_cb(log)
                            for cb in self._event_cbs:
                                cb(log)
                        elif k == "status_cb":
                            status = dc.Status.from_dict(v)
                            self._status = status
                            self._event_cb(status)
                            for cb in self._event_cbs:
                                cb(status)
                        else:
                            raise ValueError(f"invalid jmsg: '{k}'\n")
        finally:
            self._sock_failed = True
            # Always end any in-progress waits.
            self._event_cb(None)

    def __enter__(self) -> "Devcluster":
        self.start()
        return self

    def __exit__(self, *_: typing.Any) -> None:
        self.close()

    def _event_cb(self, event: typing.Optional[Event]) -> None:
        with self._lock:
            queues = list(self._queues)
        for q in queues:
            q.put(event)

    def _send_jmsg(self, jsmg: typing.Any) -> None:
        """
        Send a command jmsg over the socket.

        Raises:
          - DevclusterError if the socket is broken.
        """
        assert self._sock, "devcluster must be running"
        try:
            self._sock.sendall(json.dumps(jsmg).encode("utf8") + b"\n")
        except ConnectionError as e:
            raise DevclusterError() from e

    @contextlib.contextmanager
    def wait_for_event(
        self,
        condition: typing.Callable[[typing.Union[dc.Log, dc.Status]], bool],
        timeout: typing.Optional[float],
    ) -> typing.Iterator[None]:
        """
        Wait for the provided condition function to return True, checking against
        every Status and Log update from the devcluster subprocess.

        wait_for_event() is a context manager.  The wait occurs when the context
        manager exits, but the condition is checked against all events since the
        context manager was opened.

        Raises:
          - TimeoutError if timeout is provided and is exceeded
        """
        q = queue.Queue()  # type: queue.Queue
        with self._lock:
            self._queues.add(q)
            # We always want to check against current status first.
            assert self._status, "devcluster must be running"
            q.put(self._status)

        try:
            # Let calling code run, accumulating events in the queue.
            yield

            # Handle events that are already in the queue.
            while not q.empty():
                event = q.get()
                if event is None:
                    # The background thread has crashed.
                    raise DevclusterError()
                if condition(event):
                    return

            # Handle new events in real-time.
            deadline = timeout and time.time() + timeout
            while True:
                # There are events not already in the queue.
                t = deadline and deadline - time.time()
                if t is not None and t <= 0:
                    # Don't start q.get() because time is already up.
                    raise TimeoutError()
                try:
                    event = q.get(timeout=t)
                except queue.Empty:
                    # Convert to TimeoutError.
                    raise TimeoutError()
                if event is None:
                    # The background thread has crashed.
                    raise DevclusterError()
                # Finally, check the provided condition.
                if condition(event):
                    return
        finally:
            with self._lock:
                self._queues.remove(q)

    def set_target(
        self,
        target: typing.Union[int, str],
        wait: bool = True,
        timeout: typing.Optional[float] = None,
    ) -> None:
        """
        Set a target state for the cluster.

        Optionally, wait for the cluster to reach the target state.

        Raises:
          - DevclusterError if the devcluster subprocess fails
          - TimeoutError if wait==True, timeout not None, and timeout is exceeded
          - StageError if one or more stages crash while waiting
        """
        target_idx = self._stage_map[target]
        jmsg = {"set_target": target_idx}

        if not wait:
            self._send_jmsg(jmsg)
            return

        def condition(status: Event) -> bool:
            if not isinstance(status, dc.Status):
                return False
            if any(s == dc.StageStatus.CRASHED for s in status.stages):
                raise StageError()
            return status.state_idx == target_idx and (
                target_idx == 0 or status.stages[target_idx] == dc.StageStatus.UP
            )

        with self.wait_for_event(condition, timeout):
            self._send_jmsg(jmsg)

    def restart_stage(
        self,
        target: typing.Union[int, str],
        wait: bool = True,
        timeout: typing.Optional[float] = None,
    ) -> None:
        """
        Start/Restart a stage of the cluster.

        Optionally, wait for that stage to reach the UP state.

        Raises:
          - DevclusterError if the devcluster subprocess fails
          - TimeoutError if wait==True, timeout not None, and timeout is exceeded
          - StageError if the target stage crashes while waiting
        """
        target_idx = self._stage_map[target]
        jmsg = {"restart_stage": target_idx}

        if not wait:
            self._send_jmsg(jmsg)
            return

        # We want to detect crashes, but we have to tolerate the case where the stage _starts_ in
        # a crashed state.  We process every single state update sequentially, so this should work.
        assert self._status, "devcluster must be running"
        saw_non_crashed = self._status.stages[target_idx] != dc.StageStatus.CRASHED

        def condition(status: Event) -> bool:
            if not isinstance(status, dc.Status):
                return False
            nonlocal saw_non_crashed
            if saw_non_crashed:
                if status.stages[target_idx] == dc.StageStatus.CRASHED:
                    raise StageError()
            elif status.stages[target_idx] != dc.StageStatus.CRASHED:
                saw_non_crashed = True
            return status.stages[target_idx] == dc.StageStatus.UP

        with self.wait_for_event(condition, timeout):
            self._send_jmsg(jmsg)

    def kill_stage(
        self,
        target: typing.Union[int, str],
        signal: typing.Optional[_signal.Signals] = None,
        wait: bool = True,
        timeout: typing.Optional[float] = None,
    ) -> None:
        """
        Kill a stage of the cluster.

        Optionally, wait for that stage to reach the DOWN/CRASHED state.

        Raises:
          - DevclusterError if the devcluster subprocess fails
          - TimeoutError if wait==True, timeout not None, and timeout is exceeded
        """
        target_idx = self._stage_map[target]
        jmsg = {"kill_stage": {"target": target_idx, "signal": signal}}

        if not wait:
            self._send_jmsg(jmsg)

        def condition(status: Event) -> bool:
            if not isinstance(status, dc.Status):
                return False
            return status.stages[target_idx] in (
                dc.StageStatus.DOWN,
                dc.StageStatus.CRASHED,
            )

        with self.wait_for_event(condition, timeout):
            self._send_jmsg(jmsg)

    @contextlib.contextmanager
    def wait_for_stage_log(
        self,
        stage: typing.Union[int, str],
        regex: bytes,
        ignore_crashes: bool = False,
        timeout: typing.Optional[float] = None,
        verbose: bool = False,
    ) -> typing.Iterator[None]:
        """
        Wait for a regex to appear on a stage log line.  The action which you expect to trigger the
        log line should occur inside of the context manager, and the waiting will happen when
        the context manager exits.

        During the wait, this raises:
          - DevclusterError if the devcluster subprocess fails
          - TimeoutError if the log was not detected in time
            (the timeout starts when the context manager exits)
          - StageError if the stage crashes, unless ignore_crashes==True
        """
        stage_idx = self._stage_map[stage]
        stream_name = self._target_map[stage]

        compiled_regex = re.compile(regex)

        def condition(event: Event) -> bool:
            if isinstance(event, dc.Status):
                status = event
                if not ignore_crashes:
                    if status.stages[stage_idx] == dc.StageStatus.CRASHED:
                        raise StageError()
                return False

            log = event
            if log.stream != stream_name:
                return False
            if verbose:
                print(log.msg)
            return compiled_regex.search(log.msg) is not None

        with self.wait_for_event(condition, timeout):
            yield

    @contextlib.contextmanager
    def wait_for_console_log(
        self,
        regex: bytes,
        timeout: typing.Optional[float] = None,
        verbose: bool = False,
    ) -> typing.Iterator[None]:
        """
        Wait for a regex to appear in the console logs.  The action which you expect to trigger the
        log line should occur inside of the context manager, and the waiting will happen when
        the context manager exits.

        During the wait, this raises:
          - DevclusterError if the devcluster subprocess fails
          - TimeoutError if the log was not detected in time
            (the timeout starts when the context manager exits)
        """
        compiled_regex = re.compile(regex)

        def condition(log: Event) -> bool:
            if not isinstance(log, dc.Log):
                return False
            if log.stream != "console":
                return False
            if verbose:
                print(log.msg)
            return compiled_regex.search(log.msg) is not None

        with self.wait_for_event(condition, timeout):
            yield
