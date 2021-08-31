import base64
import contextlib
import json
import os
import pickle
import traceback
import signal
import socket
import sys
import typing

import devcluster as dc

TCPAddr = typing.Tuple[str, int]
UnixAddr = str


def read_addr_spec(
    spec: str,
) -> typing.Tuple[typing.Optional[TCPAddr], typing.Optional[UnixAddr]]:
    assert spec
    addr = None
    sock = None

    if set(spec).issubset("0123456789"):
        # plain port number
        addr = ("", int(spec))
    elif ":" in spec:
        # host:port
        temp = spec.split(":")
        addr = (":".join(temp[:-1]), int(temp[-1]))
    elif "/" in spec:
        sock = spec
    else:
        raise ValueError(
            f"address spec '{spec}' is neither a port number, nor host:port, nor a path"
        )

    return addr, sock


def listener_from_spec(spec: str) -> socket.socket:
    addr, sock = read_addr_spec(spec)
    if addr is not None:
        # TCP
        l = socket.socket()
        l.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        l.bind(addr)
        l.listen()
    else:
        # Unix
        assert sock is not None
        l = socket.socket(family=socket.AF_UNIX)
        if os.path.exists(sock):
            os.remove(sock)
        l.bind(sock)
        l.listen()

    return l


def connection_from_spec(spec: str) -> socket.socket:
    addr, sock = read_addr_spec(spec)
    if addr is not None:
        # TCP
        c = socket.socket()
        c.connect(addr)
    else:
        # Unix
        assert sock is not None
        c = socket.socket(family=socket.AF_UNIX)
        c.connect(sock)

    return c


class OneshotCB:
    """
    Watch the state to print a message when the final state is up and to quit if anything fails.
    """

    def __init__(self, quit_cb: typing.Callable[[], None]) -> None:
        self.first_target = None  # type: typing.Optional[str]
        self.up = False
        self.failing = False
        self.quit_cb = quit_cb

    def state_cb(self, state: str, substate: str, target: str) -> None:
        if self.first_target is None:
            self.first_target = target

        # Did we reach the target stat?
        if state == self.first_target and substate == "" and not self.up:
            self.up = True
            os.write(sys.stderr.fileno(), b"devcluster is up\n")

        # Is the cluster failing?
        if target != self.first_target and not self.failing:
            self.failing = True
            os.write(sys.stderr.fileno(), b"devcluster is failing\n")
            self.quit_cb()

    def log_cb(self, msg: bytes, stream: str) -> None:
        lines = msg.split(b"\n")
        if lines and lines[-1] == b"":
            lines = lines[:-1]
        for line in lines:
            os.write(sys.stdout.fileno(), dc.asbytes(stream) + b": " + line + b"\n")


# Jmsg is a message from the json-based protocol between server and client.
Jmsg = typing.Any
JmsgCB = typing.Callable[[Jmsg], None]

ConnectionCloseCB = typing.Callable[["Connection"], None]


class Connection:
    def __init__(
        self,
        poll: dc.Poll,
        sock: socket.socket,
        read_cb: JmsgCB,
        close_cb: ConnectionCloseCB,
        starting_buffer: bytes = b"",
    ) -> None:
        self.poll = poll
        self.sock = sock
        self.read_cb = read_cb
        self.close_cb = close_cb

        self.buffer = starting_buffer

        self.poll.register(self.sock.fileno(), dc.Poll.IN_FLAGS, self.handle_sock)

    def write(self, jmsg: Jmsg) -> None:
        stuff = json.dumps(jmsg).encode("utf8") + b"\n"
        self.sock.send(stuff)

    def handle_sock(self, ev: int, _: int) -> None:
        if ev & dc.Poll.IN_FLAGS:
            try:
                new_bytes = self.sock.recv(4096)
            except ConnectionError:
                self.close()
                return
            if len(new_bytes) == 0:
                self.close()
                return
            self.buffer += new_bytes
            while b"\n" in self.buffer:
                end = self.buffer.find(b"\n")
                line = self.buffer[:end]
                self.buffer = self.buffer[end + 1 :]
                jmsg = json.loads(line.decode("utf8"))
                self.read_cb(jmsg)
        elif ev & dc.Poll.ERR_FLAGS:
            self.close()

    def close(self) -> None:
        self.poll.unregister(self.handle_sock)
        self.sock.close()
        self.close_cb(self)


class Server:
    def __init__(
        self,
        config: dc.Config,
        listeners: typing.List[str],
        quiet: bool = False,
        oneshot: bool = False,
        initial_target_stage: typing.Optional[str] = None,
    ) -> None:
        self.config = config
        self.poll = dc.Poll()

        # map of fd's to socket objects
        self.listeners = {}

        for spec in listeners:
            l = listener_from_spec(spec)
            self.poll.register(l.fileno(), dc.Poll.IN_FLAGS, self.handle_listener)
            self.listeners[l.fileno()] = l

        self.stage_names = [stage_config.name for stage_config in config.stages]

        self.initial_target_stage_idx = len(self.stage_names)
        if initial_target_stage is not None:
            if initial_target_stage not in self.stage_names:
                raise ValueError(
                    f"bad initial target stage: {initial_target_stage}. "
                    f"available options: {self.stage_names}"
                )

            self.initial_target_stage_idx = (
                self.stage_names.index(initial_target_stage) + 1
            )

        self.logger = dc.Logger(self.stage_names, config.temp_dir)
        self.logger.add_callback(self.log_cb)

        self.state_machine = dc.StateMachine(self.logger, self.poll, config.commands)
        self.state_machine.add_callback(self.state_machine_cb)

        for stage_config in config.stages:
            self.state_machine.add_stage(
                stage_config.build_stage(self.poll, self.logger, self.state_machine)
            )

        if quiet or oneshot:
            # Don't use a Console.
            self.console = None
        else:
            self.console = dc.Console(
                self.logger,
                self.poll,
                self.stage_names,
                self.state_machine.set_target,
                config.commands,
                self.state_machine.run_command,
                self.state_machine.quit,
            )
            self.state_machine.add_callback(self.console.state_cb)

            def _sigwinch_handler(signum: typing.Any, frame: typing.Any) -> None:
                """Enqueue a call to _sigwinch_callback() via poll()."""
                os.write(self.state_machine.get_report_fd(), b"W")

            def _sigwinch_callback() -> None:
                """Handle the SIGWINCH when it is safe"""
                assert self.console
                self.console.handle_window_change()

            self.state_machine.add_report_callback("W", _sigwinch_callback)
            signal.signal(signal.SIGWINCH, _sigwinch_handler)

        if oneshot:
            # In case of a signal state_machine.quit() may have been called, so we check try to
            # detect if we need to call quit or not from the OneshotCB.
            def quit_cb() -> None:
                if not self.state_machine.quitting:
                    self.state_machine.quit()

            oneshot_cb = OneshotCB(quit_cb)
            self.state_machine.add_callback(oneshot_cb.state_cb)

            self.logger.add_callback(oneshot_cb.log_cb)

        self.clients = set()  # type: typing.Set[Connection]

        self.command_config = config.commands

        # Write a traceback on SIGUSR1 (10)
        def _traceback_signal(signum: typing.Any, frame: typing.Any) -> None:
            if self.console is None:
                # print right to stdout
                print("------")
                traceback.print_stack(frame)
            else:
                # print to a file, since we'll have a Console on stdout.
                with open(os.path.join(config.temp_dir, "traceback"), "a") as f:
                    f.write("------\n")
                    traceback.print_stack(frame, file=f)

        signal.signal(signal.SIGUSR1, _traceback_signal)

        def _quit_signal(signum: typing.Any, frame: typing.Any) -> None:
            """Enqueue a call to _quit_in_loop() via poll()."""
            os.write(self.state_machine.get_report_fd(), b"Q")

        def _quit_in_loop() -> None:
            self.state_machine.quit()

        self.state_machine.add_report_callback("Q", _quit_in_loop)
        signal.signal(signal.SIGTERM, _quit_signal)
        signal.signal(signal.SIGINT, _quit_signal)

    def run(self) -> None:
        with contextlib.ExitStack() as ex:
            if self.console:
                # Configure the terminal.
                ex.enter_context(dc.terminal_config())

                # Draw the initial screen.
                self.console.start()

            self.state_machine.set_target(self.initial_target_stage_idx)

            if self.console:
                # TODO: handle startup_input without console
                for c in self.config.startup_input:
                    self.console.handle_key(c)

            while self.state_machine.should_run():
                self.poll.poll()

    def handle_listener(self, ev: int, fd: int) -> None:
        if ev & dc.Poll.IN_FLAGS:
            # accept a connection
            l = self.listeners[fd]
            sock, _ = l.accept()
            client = Connection(
                self.poll, sock, self.jmsg_cb, self.client_conn_close_cb
            )
            self.clients.add(client)
            # start by sending some initial state
            init = {
                "stages": [s.log_name() for s in self.state_machine.stages[1:]],
                "logger_streams": self.logger.streams,
                "logger_index": self.logger.index,
                "first_state": self.state_machine.gen_state_cb(),
                "command_configs": self.command_config,
            }
            client.write({"init": base64.b64encode(pickle.dumps(init)).decode("utf8")})

        elif ev & dc.Poll.ERR_FLAGS:
            raise ValueError("listener failed!")

    def jmsg_cb(self, jmsg: Jmsg) -> None:
        for k, v in jmsg.items():
            if k == "set_target":
                self.state_machine.set_target(v)
            elif k == "run_cmd":
                self.state_machine.run_command(v)
            elif k == "quit":
                self.state_machine.quit()
            else:
                raise ValueError(f"invalid jmsg: '{k}'\n")

    def log_cb(self, msg: bytes, stream: str) -> None:
        # the server listens to log callbacks and broadcasts them to all clients
        jmsg = {"log_cb": [base64.b64encode(msg).decode("utf8"), stream]}
        for client in self.clients:
            client.write(jmsg)

    def state_machine_cb(self, state: str, atomic: str, target: str) -> None:
        # the server listens to state machine callbacks and broadcasts them to all clients
        jmsg = {"state_cb": [state, atomic, target]}
        for client in self.clients:
            client.write(jmsg)

    def client_conn_close_cb(self, client: Connection) -> None:
        self.clients.remove(client)


class Client:
    def __init__(self, spec: str) -> None:
        sock = connection_from_spec(spec)

        buf = b""
        while b"\n" not in buf:
            new_bytes = sock.recv(4096)
            if len(new_bytes) == 0:
                raise ValueError("connection closed during initial download")
            buf += new_bytes

        end = buf.find(b"\n")
        line = buf[:end]
        buf = buf[end + 1 :]

        jmsg = json.loads(line.decode("utf8"))
        init = pickle.loads(base64.b64decode(jmsg["init"]))
        self.first_state = init["first_state"]

        self.poll = dc.Poll()

        self.logger = dc.Logger(
            init["stages"], None, init["logger_streams"], init["logger_index"]
        )
        self.console = dc.Console(
            self.logger,
            self.poll,
            init["stages"],
            self.set_target,
            init["command_configs"],
            self.run_command,
            self.quit,
        )

        self.server = Connection(
            self.poll,
            sock,
            self.jmsg_cb,
            self.server_conn_close_cb,
            starting_buffer=buf,
        )

        # this is primarily used by the SIGWINCH handler
        self.pipe_rd, self.pipe_wr = os.pipe()
        dc.nonblock(self.pipe_rd)
        dc.nonblock(self.pipe_wr)
        self.poll.register(self.pipe_rd, dc.Poll.IN_FLAGS, self.handle_pipe)

        def _sigwinch_handler(signum: typing.Any, frame: typing.Any) -> None:
            """Enqueue a call to _sigwinch_callback() via self.poll()."""
            os.write(self.pipe_wr, b"W")

        signal.signal(signal.SIGWINCH, _sigwinch_handler)

        self.tracebacks = []  # type: typing.List[typing.List[str]]

        # Write a traceback to console output on SIGUSR1 (10)
        def _traceback_signal(signum: typing.Any, frame: typing.Any) -> None:
            self.tracebacks.append(traceback.format_stack(frame))
            os.write(self.pipe_wr, b"T")

        signal.signal(signal.SIGUSR1, _traceback_signal)

        self.keep_going = True

    def run(self) -> None:
        with dc.terminal_config():
            self.console.start()
            self.console.state_cb(*self.first_state)

            # for c in config.startup_input:
            #     self.console.handle_key(c)

            while self.keep_going:
                self.poll.poll()

    def handle_pipe(self, ev: int, _: int) -> None:
        if ev & dc.Poll.IN_FLAGS:
            buf = os.read(self.pipe_rd, 4096)
            for c in buf.decode("utf8"):
                if c == "W":
                    # SIGWINCH
                    self.console.handle_window_change()
                elif c == "T":
                    # SIGUSR1, or "print me a stack trace"
                    for t in self.tracebacks:
                        self.logger.log("\n".join(t))
                    self.tracbacks = []  # type: typing.List[typing.List[str]]
                else:
                    raise ValueError(f"invalid value in Client.handle_pipe(): {c}")
        elif ev & dc.Poll.ERR_FLAGS:
            raise ValueError("stdin closed!")

    def jmsg_cb(self, jmsg: Jmsg) -> None:
        for key, value in jmsg.items():
            if key == "log_cb":
                # replay logs through our own Logger
                self.logger.log(base64.b64decode(value[0]), value[1])
            elif key == "state_cb":
                self.console.state_cb(*value)
            else:
                raise ValueError(f"unexpected jmsg: {key}")

    def server_conn_close_cb(self, server_conn: Connection) -> None:
        raise ValueError("Connection to server closed")

    def set_target(self, idx: int) -> None:
        self.server.write({"set_target": idx})

    def run_command(self, cmdstr: str) -> None:
        self.server.write({"run_cmd": cmdstr})

    def quit(self) -> None:
        self.keep_going = False
