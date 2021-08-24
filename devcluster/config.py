import abc
import re
import yaml
import os
import string

import devcluster as dc


def check_keys(allowed, required, config, name):
    extra = set(config.keys()).difference(allowed)
    assert len(extra) == 0, f"invalid keys for {name}: {extra}"
    missing = required.difference(set(config.keys()))
    assert len(missing) == 0, f"missing required keys for {name}: {missing}"


def check_list_of_strings(l, msg):
    assert isinstance(l, list), msg
    for s in l:
        assert isinstance(s, str), msg


def check_list_of_dicts(l, msg):
    assert isinstance(l, list), msg
    for s in l:
        assert isinstance(s, dict), msg


def check_dict_with_string_keys(d, msg):
    assert isinstance(d, dict), msg
    for k in d:
        assert isinstance(k, str), msg


def read_path(path):
    """Expand ~'s in a non-None path."""
    if path is None:
        return None
    return os.path.expanduser(path)


def expand_env(value, env):
    """Expand string variables in the config file"""
    if isinstance(value, str):
        return string.Template(value).safe_substitute(env)
    if isinstance(value, dict):
        return {k: expand_env(v, env) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env(l, env) for l in value]
    return value


class StageConfig:
    @staticmethod
    def read(config, temp_dir):
        allowed = {"db", "master", "agent", "custom", "custom_docker"}
        # required = set()

        assert isinstance(
            config, dict
        ), "StageConfig must be a dictionary with a single key"
        assert len(config), "StageConfig must be a dictionary with a single key"
        typ, val = next(iter(config.items()))
        assert typ in allowed, f"{typ} is not one of {allowed}"

        if typ == "custom":
            return CustomConfig(val)
        if typ == "custom_docker":
            return CustomDockerConfig(val)
        elif typ == "db":
            return DBConfig(val)
        elif typ == "master":
            return MasterConfig(val, temp_dir)
        elif typ == "agent":
            return AgentConfig(val, temp_dir)

    @abc.abstractmethod
    def build_stage(self):
        pass


class AtomicConfig:
    @staticmethod
    def read(config):
        allowed = {"custom", "sh", "conncheck", "logcheck"}
        # required = set()

        assert isinstance(
            config, dict
        ), "AtomicConfig must be a dictionary with a single key"
        assert len(config), "AtomicConfig must be a dictionary with a single key"
        typ, val = next(iter(config.items()))
        assert typ in allowed, f"{typ} is not one of {allowed}"

        if typ == "custom":
            return CustomAtomicConfig(val)
        if typ == "sh":
            return ShellAtomicConfig(val)
        elif typ == "conncheck":
            return ConnCheckConfig(val)
        elif typ == "logcheck":
            return LogCheckConfig(val)

    @abc.abstractmethod
    def build_atomic(self, poll, logger, stream, report_fd):
        pass


class DBConfig(StageConfig):
    """DBConfig is a canned stage that runs the database in docker"""

    def __init__(self, config):
        allowed = {
            "name",
            "port",
            "password",
            "db_name",
            "data_dir",
            "container_name",
            "image_name",
        }
        required = set()
        check_keys(allowed, required, config, type(self).__name__)

        self.port = int(config.get("port", 5432))
        self.password = str(config.get("password", "postgres"))
        self.db_name = str(config.get("db_name", "determined"))
        self.container_name = str(config.get("container_name", "determined_db"))
        self.data_dir = read_path(config.get("data_dir"))
        self.name = str(config.get("name", "db"))
        self.image_name = str(config.get("image_name", "postgres:10.14"))

    def build_stage(self, poll, logger, state_machine):

        if self.data_dir:
            run_args = ["-v", f"{self.data_dir}:/var/lib/postgresql/data"]
        else:
            run_args = []

        run_args += [
            "-p",
            f"{self.port}:5432",
            "-e",
            f"POSTGRES_DB={self.db_name}",
            "-e",
            f"POSTGRES_PASSWORD={self.password}",
            self.image_name,
            "-N",
            "10000",
        ]

        custom_config = CustomDockerConfig(
            {
                "name": "db",
                "container_name": self.container_name,
                "kill_signal": "TERM",
                "run_args": run_args,
                "post": [
                    {"logcheck": {"regex": "listening on IP"}},
                ],
            }
        )

        return dc.DockerProcess(custom_config, poll, logger, state_machine)


class MasterConfig(StageConfig):
    def __init__(self, config, temp_dir):
        allowed = {"pre", "post", "binary", "config_file", "name"}
        required = set()
        check_keys(allowed, required, config, type(self).__name__)

        self.config_file = config.get("config_file", {})

        check_list_of_dicts(
            config.get("pre", []), "CustomConfig.pre must be a list of dicts"
        )
        self.pre = config.get("pre", [])
        self.post = config.get(
            "post",
            [{"logcheck": {"regex": "accepting incoming connections on port"}}],
        )

        self.binary = read_path(config.get("binary", "master/build/determined-master"))

        self.name = config.get("name", "master")
        self.temp_dir = temp_dir

    def build_stage(self, poll, logger, state_machine):
        config_path = os.path.join(self.temp_dir, f"{self.name}.conf")
        with open(config_path, "w") as f:
            f.write(yaml.dump(self.config_file))

        cmd = [
            self.binary,
            "--config-file",
            config_path,
        ]

        custom_config = CustomConfig(
            {
                "cmd": cmd,
                "name": self.name,
                "pre": self.pre,
                # TODO: don't hardcode 8080
                "post": self.post,
            }
        )

        return dc.Process(custom_config, poll, logger, state_machine)


class AgentConfig(StageConfig):
    def __init__(self, config, temp_dir):
        allowed = {"pre", "binary", "config_file", "name"}
        required = set()
        check_keys(allowed, required, config, type(self).__name__)

        self.binary = read_path(config.get("binary", "agent/build/determined-agent"))

        self.config_file = config.get("config_file", {})

        check_list_of_dicts(
            config.get("pre", []), "CustomConfig.pre must be a list of dicts"
        )
        self.pre = config.get("pre", [])

        self.name = config.get("name", "agent")
        self.temp_dir = temp_dir

    def build_stage(self, poll, logger, state_machine):
        config_path = os.path.join(self.temp_dir, f"{self.name}.conf")
        with open(config_path, "w") as f:
            f.write(yaml.dump(self.config_file))

        cmd = [
            self.binary,
            "run",
            "--config-file",
            config_path,
        ]

        custom_config = CustomConfig({"cmd": cmd, "name": self.name, "pre": self.pre})

        return dc.Process(custom_config, poll, logger, state_machine)


class ConnCheckConfig:
    def __init__(self, config):
        allowed = {"host", "port"}
        required = {"port"}
        check_keys(allowed, required, config, type(self).__name__)

        self.host = config.get("host", "localhost")
        self.port = config["port"]

    def build_atomic(self, poll, logger, stream, report_fd):
        return dc.ConnCheck(self.host, self.port, report_fd)


class LogCheckConfig:
    def __init__(self, config):
        allowed = {"regex", "stream"}
        required = {"regex"}
        check_keys(allowed, required, config, type(self).__name__)

        self.regex = config["regex"]
        self.stream = config.get("stream")

        # confirm that the regex is compilable
        re.compile(dc.asbytes(self.regex))

    def build_atomic(self, poll, logger, stream, report_fd):
        # Allow the configured stream to overwrite the default stream.
        s = stream if self.stream is None else self.stream
        return dc.LogCheck(logger, s, report_fd, self.regex)


class CustomAtomicConfig:
    def __init__(self, config):
        check_list_of_strings(config, "AtomicConfig.custom must be a list of strings")
        self.cmd = config

    def build_atomic(self, poll, logger, stream, report_fd):
        return dc.AtomicSubprocess(poll, logger, stream, report_fd, self.cmd)


class ShellAtomicConfig:
    def __init__(self, config):
        assert isinstance(config, str), "AtomicConfig.sh must be a single string"
        self.cmd = ["sh", "-c", config]

    def build_atomic(self, poll, logger, stream, report_fd):
        return dc.AtomicSubprocess(poll, logger, stream, report_fd, self.cmd)


class CustomConfig(StageConfig):
    def __init__(self, config):
        allowed = {"cmd", "name", "env", "cwd", "pre", "post"}
        required = {"cmd", "name"}

        check_keys(allowed, required, config, type(self).__name__)

        self.cmd = config["cmd"]
        check_list_of_strings(self.cmd, "CustomConfig.cmd must be a list of strings")

        self.name = config["name"]
        assert isinstance(self.name, str), "CustomConfig.name must be a string"

        self.env = config.get("env", {})
        check_dict_with_string_keys(
            self.env, "CustomConfig.pre must be a list of dicts"
        )

        self.cwd = read_path(config.get("cwd"))
        if self.cwd is not None:
            assert isinstance(self.cwd, str), "CustomConfig.name must be a string"
            assert os.path.exists(
                self.cwd
            ), f"cwd setting not valid, {self.cwd} does not exist"
            assert os.path.isdir(
                self.cwd
            ), f"cwd setting not valid, {self.cwd} is not a directory"

        check_list_of_dicts(
            config.get("pre", []), "CustomConfig.pre must be a list of dicts"
        )
        self.pre = [AtomicConfig.read(pre) for pre in config.get("pre", [])]

        check_list_of_dicts(
            config.get("post", []), "CustomConfig.post must be a list of dicts"
        )
        self.post = [AtomicConfig.read(post) for post in config.get("post", [])]

    def build_stage(self, poll, logger, state_machine):
        return dc.Process(self, poll, logger, state_machine)


class CustomDockerConfig(StageConfig):
    def __init__(self, config):
        allowed = {"name", "container_name", "run_args", "kill_signal", "pre", "post"}
        required = {"name", "container_name", "run_args"}

        check_keys(allowed, required, config, type(self).__name__)

        self.container_name = config["container_name"]

        self.run_args = config["run_args"]
        check_list_of_strings(
            self.run_args, "CustomConfig.run_args must be a list of strings"
        )

        self.kill_signal = config.get("kill_signal", "KILL")
        assert isinstance(
            self.kill_signal, str
        ), "CustomConfig.kill_signal must be a string"

        self.name = config["name"]
        assert isinstance(self.name, str), "CustomConfig.name must be a string"

        check_list_of_dicts(
            config.get("pre", []), "CustomConfig.pre must be a list of dicts"
        )
        self.pre = [AtomicConfig.read(pre) for pre in config.get("pre", [])]

        check_list_of_dicts(
            config.get("post", []), "CustomConfig.post must be a list of dicts"
        )
        self.post = [AtomicConfig.read(post) for post in config.get("post", [])]

    def build_stage(self, poll, logger, state_machine):
        return dc.DockerProcess(self, poll, logger, state_machine)


class CommandConfig:
    def __init__(self, command):
        self.command = command

    @staticmethod
    def read(config):
        # allowable command configs:
        # commands:
        #   a: echo hello world
        #   b: ["echo", "hello", "world"]

        msg = "CommandConfig must be either a string or a list of strings"
        assert isinstance(config, (str, list)), msg
        if isinstance(config, list):
            check_list_of_strings(config, msg)
        return CommandConfig(config)


class Config:
    def __init__(self, config):
        allowed = {"stages", "commands", "startup_input", "temp_dir", "cwd"}
        required = {"stages"}
        check_keys(allowed, required, config, type(self).__name__)

        self.temp_dir = config.get("temp_dir", "/tmp/devcluster")

        check_list_of_dicts(config["stages"], "stages must be a list of dicts")
        self.stages = [
            StageConfig.read(stage, self.temp_dir) for stage in config["stages"]
        ]
        self.startup_input = config.get("startup_input", "")

        commands = config.get("commands", {})
        check_dict_with_string_keys(commands, "commands must be a dict of strings")
        self.commands = {k: CommandConfig.read(v) for k, v in commands.items()}

        self.cwd = read_path(config.get("cwd"))
        if self.cwd is not None:
            assert isinstance(self.cwd, str), "cwd must be a string"
