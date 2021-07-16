import base64
import json
import os
import pickle
import traceback
import signal
import socket

import devcluster as dc


class Connection:
    def __init__(self, poll, sock, read_cb, close_cb, starting_buffer=b""):
        self.poll = poll
        self.sock = sock
        self.read_cb = read_cb
        self.close_cb = close_cb

        self.buffer = starting_buffer

        self.poll.register(self.sock.fileno(), dc.Poll.IN_FLAGS, self.handle_sock)

    def write(self, jmsg):
        stuff = json.dumps(jmsg).encode("utf8") + b"\n"
        self.sock.send(stuff)

    def handle_sock(self, ev):
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

    def close(self):
        self.poll.unregister(self.handle_sock)
        self.sock.close()
        self.close_cb(self)


class Server:
    def __init__(self, config, bindhost, bindport):
        self.poll = dc.Poll()

        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind((bindhost, bindport))
        self.listener.listen()
        self.poll.register(
            self.listener.fileno(), dc.Poll.IN_FLAGS, self.handle_listener
        )

        self.stage_names = [stage_config.name for stage_config in config.stages]

        self.logger = dc.Logger(self.stage_names, config.temp_dir)
        self.logger.add_callback(self.log_cb)

        self.state_machine = dc.StateMachine(self.logger, self.poll, config.commands)
        self.state_machine.add_callback(self.state_machine_cb)

        for stage_config in config.stages:
            self.state_machine.add_stage(
                stage_config.build_stage(self.poll, self.logger, self.state_machine)
            )

        self.clients = set()

        self.command_config = config.commands

        # Write a traceback to stdout on SIGUSR1 (10)
        def _traceback_signal(signum, frame):
            traceback.print_stack(frame)

        signal.signal(signal.SIGUSR1, _traceback_signal)

    def run(self):
        def _quit_signal(signum, frame):
            """Enqueue a call to _quit_in_loop() via poll()."""
            os.write(self.state_machine.get_report_fd(), b"Q")

        def _quit_in_loop():
            self.state_machine.quit()

        self.state_machine.add_report_callback("Q", _quit_in_loop)
        signal.signal(signal.SIGTERM, _quit_signal)
        signal.signal(signal.SIGINT, _quit_signal)

        self.state_machine.set_target(len(self.stage_names))

        # TODO: handle startup_input in server mode
        # for c in config.startup_input:
        #     console.handle_key(c)

        while self.state_machine.should_run():
            self.poll.poll()

    def handle_listener(self, ev):
        if ev & dc.Poll.IN_FLAGS:
            # accept a connection
            sock, _ = self.listener.accept()
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

    def jmsg_cb(self, jmsg):
        for k, v in jmsg.items():
            if k == "set_target":
                self.state_machine.set_target(v)
            if k == "run_cmd":
                self.state_machine.run_cmd(v)
            elif k == "quit":
                self.state_machine.quit()
            else:
                raise ValueError(f"invalid jmsg: {k}\n")

    def log_cb(self, msg, stream):
        # the server listens to log callbacks and broadcasts them to all clients
        jmsg = {"log_cb": [base64.b64encode(msg).decode("utf8"), stream]}
        for client in self.clients:
            client.write(jmsg)

    def state_machine_cb(self, state, atomic, target):
        # the server listens to state machine callbacks and broadcasts them to all clients
        jmsg = {"state_cb": [state, atomic, target]}
        for client in self.clients:
            client.write(jmsg)

    def client_conn_close_cb(self, client):
        self.clients.remove(client)


class Client:
    def __init__(self, host, port):
        sock = socket.socket()
        sock.connect((host, port))

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

        def _sigwinch_handler(signum, frame):
            """Enqueue a call to _sigwinch_callback() via self.poll()."""
            os.write(self.pipe_wr, b"W")

        signal.signal(signal.SIGWINCH, _sigwinch_handler)

        self.tracebacks = []

        # Write a traceback to console output on SIGUSR1 (10)
        def _traceback_signal(signum, frame):
            self.tracebacks.append(traceback.format_stack(frame))
            os.write(self.pipe_wr, b"T")

        signal.signal(signal.SIGUSR1, _traceback_signal)

        self.keep_going = True

    def run(self):
        self.console.start()
        self.console.state_cb(*self.first_state)

        # for c in config.startup_input:
        #     self.console.handle_key(c)

        while self.keep_going:
            self.poll.poll()

    def handle_pipe(self, ev):
        if ev & dc.Poll.IN_FLAGS:
            buf = self.pipe_rd.read(4096)
            for c in buf:
                if c == "W":
                    # SIGWINCH
                    self.console.handle_window_change()
                elif c == "T":
                    # SIGUSR1, or "print me a stack trace"
                    for t in self.tracebacks:
                        self.logger.log(t)
                    self.tracbacks = []
                else:
                    raise ValueError(f"invalid value in Client.handle_pipe(): {c}")
        elif ev & dc.Poll.ERR_FLAGS:
            raise ValueError("stdin closed!")

    def jmsg_cb(self, jmsg):
        for key, value in jmsg.items():
            if key == "log_cb":
                # replay logs through our own Logger
                self.logger.log(base64.b64decode(value[0]), value[1])
            elif key == "state_cb":
                self.console.state_cb(*value)
            else:
                raise ValueError(f"unexpected jmsg: {key}")

    def server_conn_close_cb(self, server_conn):
        raise ValueError("Connection to server closed")

    def set_target(self, idx):
        self.server.write({"set_target": idx})

    def run_command(self, cmdstr):
        self.server.write({"run_cmd": cmdstr})

    def quit(self):
        self.keep_going = False
