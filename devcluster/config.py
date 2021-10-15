import abc
import re
import yaml
import os
import string
import typing

import devcluster as dc


def check_string(s: typing.Any, msg: str) -> str:
    assert isinstance(s, str), msg
    return s


def check_keys(
    allowed: typing.Set[str], required: typing.Set[str], config: typing.Dict, name: str
) -> None:
    extra = set(config.keys()).difference(allowed)
    assert len(extra) == 0, f"invalid keys for {name}: {extra}"
    missing = required.difference(set(config.keys()))
    assert len(missing) == 0, f"missing required keys for {name}: {missing}"


def check_list_of_strings(l: typing.Any, msg: str) -> typing.List[str]:
    assert isinstance(l, list), msg
    for s in l:
        assert isinstance(s, str), msg
    return l


def check_list_of_dicts(l: typing.Any, msg: str) -> typing.List[typing.Dict]:
    assert isinstance(l, list), msg
    for s in l:
        assert isinstance(s, dict), msg
    return l


def check_dict_with_string_keys(
    d: typing.Any, msg: str
) -> typing.Dict[str, typing.Any]:
    assert isinstance(d, dict), msg
    for k in d:
        assert isinstance(k, str), msg
    return d


def check_dict_of_strings(d: typing.Any, msg: str) -> typing.Dict[str, str]:
    assert isinstance(d, dict), msg
    for k, v in d.items():
        assert isinstance(k, str), msg
        assert isinstance(v, str), msg
    return d


def read_path(path: typing.Optional[str]) -> typing.Optional[str]:
    """Expand ~'s in a non-None path."""
    if path is None:
        return None
    return os.path.expanduser(path)


def expand_env(value: typing.Any, env: typing.Dict[str, str]) -> typing.Any:
    """Expand string variables in the config file"""
    if isinstance(value, str):
        return string.Template(value).safe_substitute(env)
    if isinstance(value, dict):
        return {k: expand_env(v, env) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env(l, env) for l in value]
    return value


class StageConfig(metaclass=abc.ABCMeta):
    # All stage configs must define a name.
    name: str

    @staticmethod
    def read(config: typing.Any, temp_dir: str) -> "StageConfig":
        allowed = {"elastic", "db", "master", "agent", "custom", "custom_docker"}
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
        elif typ == "elastic":
            return ElasticConfig(val)
        elif typ == "db":
            return DBConfig(val)
        elif typ == "master":
            return MasterConfig(val, temp_dir)
        elif typ == "agent":
            return AgentConfig(val, temp_dir)
        raise dc.ImpossibleException()

    @abc.abstractmethod
    def build_stage(
        self,
        poll: "dc.Poll",
        logger: "dc.Logger",
        state_machine: "dc.StateMachine",
        process_tracker: "dc.ProcessTracker",
    ) -> "dc.Stage":
        pass


class AtomicConfig(metaclass=abc.ABCMeta):
    @staticmethod
    def read(config: typing.Any) -> "AtomicConfig":
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
        raise dc.ImpossibleException()

    @abc.abstractmethod
    def build_atomic(
        self, poll: "dc.Poll", logger: "dc.Logger", stream: str, report_fd: int
    ) -> "dc.AtomicOperation":
        pass


class ElasticConfig(StageConfig):
    """ElasticConfig is a canned stage that runs Elastic in docker"""

    def __init__(self, config: typing.Any) -> None:
        allowed = {
            "name",
            "api_port",
            "elastic_port",
            "environment",
            "data_dir",
            "container_name",
            "image_name",
            "cmdline",
            "post",
        }
        required = set()  # type: typing.Set[str]
        check_keys(allowed, required, config, type(self).__name__)

        self.api_port = int(config.get("api_port", 9200))
        self.elastic_port = int(config.get("elastic_port", 9300))

        self.environment = check_dict_of_strings(
            config.get(
                "environment",
                {
                    "discovery.type": "single-node",
                    "cluster.routing.allocation.disk.threshold_enabled": "false",
                    "logger.level": "WARN",
                },
            ),
            "ElasticConfig.environment must be a dict of strings to strings",
        )
        self.data_dir = read_path(config.get("data_dir"))
        self.container_name = str(config.get("container_name", "determined_elastic"))
        self.name = str(config.get("name", "elastic"))
        self.image_name = str(config.get("image_name", "elasticsearch:7.14.0"))
        self.post = config.get("post", [{"conncheck": {"port": 9200}}])

        check_list_of_strings(
            config.get("cmdline", []), "ElasticConfig.cmdline must be a list of strings"
        )
        self.cmdline = config.get("cmdline", [])

    def build_stage(
        self,
        poll: "dc.Poll",
        logger: "dc.Logger",
        state_machine: "dc.StateMachine",
        process_tracker: "dc.ProcessTracker",
    ) -> "dc.Stage":

        if self.data_dir:
            # elastic is gonna have a bad day if this directory doesn't exist yet.
            try:
                os.makedirs(os.path.join(self.data_dir, "nodes"), exist_ok=True)
            except Exception:
                pass
            run_args = ["-v", f"{self.data_dir}:/usr/share/elasticsearch/data"]
        else:
            run_args = []

        for k, v in self.environment.items():
            run_args += ["-e", f"{k}={v}"]

        run_args += [
            "-p",
            f"{self.api_port}:9200",
            "-p",
            f"{self.elastic_port}:9300",
            self.image_name,
            *self.cmdline,
        ]

        custom_config = CustomDockerConfig(
            {
                "name": self.name,
                "container_name": self.container_name,
                "kill_signal": "TERM",
                "run_args": run_args,
                "post": self.post,
            }
        )

        return dc.DockerProcess(
            custom_config, poll, logger, state_machine, process_tracker
        )


class DBConfig(StageConfig):
    """DBConfig is a canned stage that runs the database in docker"""

    def __init__(self, config: typing.Any) -> None:
        allowed = {
            "name",
            "port",
            "password",
            "db_name",
            "data_dir",
            "container_name",
            "image_name",
            "cmdline",
            "post",
        }
        required = set()  # type: typing.Set[str]
        check_keys(allowed, required, config, type(self).__name__)

        self.port = int(config.get("port", 5432))
        self.password = str(config.get("password", "postgres"))
        self.db_name = str(config.get("db_name", "determined"))
        self.container_name = str(config.get("container_name", "determined_db"))
        self.data_dir = read_path(config.get("data_dir"))
        self.name = str(config.get("name", "db"))
        self.image_name = str(config.get("image_name", "postgres:10.14"))
        self.post = config.get("post", [{"logcheck": {"regex": "listening on IP"}}])

        check_list_of_strings(
            config.get("cmdline", []), "DBConfig.cmdline must be a list of strings"
        )
        self.cmdline = config.get("cmdline", ["postgres"])

    def build_stage(
        self,
        poll: "dc.Poll",
        logger: "dc.Logger",
        state_machine: "dc.StateMachine",
        process_tracker: "dc.ProcessTracker",
    ) -> "dc.Stage":

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
            *self.cmdline,
        ]

        custom_config = CustomDockerConfig(
            {
                "name": self.name,
                "container_name": self.container_name,
                "kill_signal": "TERM",
                "run_args": run_args,
                "post": self.post,
            }
        )

        return dc.DockerProcess(
            custom_config, poll, logger, state_machine, process_tracker
        )


class MasterConfig(StageConfig):
    def __init__(self, config: typing.Any, temp_dir: str) -> None:
        allowed = {"pre", "post", "cmdline", "config_file", "name"}
        required = set()  # type: typing.Set[str]
        check_keys(allowed, required, config, type(self).__name__)

        self.config_file = config.get("config_file", {})

        check_list_of_dicts(
            config.get("pre", []), "MasterConfig.pre must be a list of dicts"
        )
        self.pre = config.get("pre", [])
        self.post = config.get(
            "post",
            [{"logcheck": {"regex": "accepting incoming connections on port"}}],
        )

        check_list_of_strings(
            config.get("cmdline", []), "MasterConfig.cmdline must be a list of strings"
        )
        self.cmdline = config.get(
            "cmdline", ["master/build/determined-master", "--config-file", ":config"]
        )
        self.cmdline = [read_path(s) for s in self.cmdline]

        self.name = config.get("name", "master")
        self.temp_dir = temp_dir

    def build_stage(
        self,
        poll: "dc.Poll",
        logger: "dc.Logger",
        state_machine: "dc.StateMachine",
        process_tracker: "dc.ProcessTracker",
    ) -> "dc.Stage":
        config_path = os.path.join(self.temp_dir, f"{self.name}.conf")
        with open(config_path, "w") as f:
            f.write(yaml.dump(self.config_file))

        cmd = [config_path if arg == ":config" else arg for arg in self.cmdline]

        custom_config = CustomConfig(
            {
                "cmd": cmd,
                "name": self.name,
                "pre": self.pre,
                "post": self.post,
            }
        )

        return dc.Process(custom_config, poll, logger, state_machine, process_tracker)


class AgentConfig(StageConfig):
    def __init__(self, config: typing.Any, temp_dir: str) -> None:
        allowed = {"pre", "cmdline", "config_file", "name"}
        required = set()  # type: typing.Set[str]
        check_keys(allowed, required, config, type(self).__name__)

        self.config_file = config.get("config_file", {})

        check_list_of_dicts(
            config.get("pre", []), "AgentConfig.pre must be a list of dicts"
        )
        self.pre = config.get("pre", [])

        check_list_of_strings(
            config.get("cmdline", []), "AgentConfig.cmdline must be a list of strings"
        )
        self.cmdline = config.get(
            "cmdline",
            ["agent/build/determined-agent", "run", "--config-file", ":config"],
        )
        self.cmdline = [read_path(s) for s in self.cmdline]

        self.name = config.get("name", "agent")
        self.temp_dir = temp_dir

    def build_stage(
        self,
        poll: "dc.Poll",
        logger: "dc.Logger",
        state_machine: "dc.StateMachine",
        process_tracker: "dc.ProcessTracker",
    ) -> "dc.Stage":
        config_path = os.path.join(self.temp_dir, f"{self.name}.conf")
        with open(config_path, "w") as f:
            f.write(yaml.dump(self.config_file))

        cmd = [config_path if arg == ":config" else arg for arg in self.cmdline]

        custom_config = CustomConfig({"cmd": cmd, "name": self.name, "pre": self.pre})

        return dc.Process(custom_config, poll, logger, state_machine, process_tracker)


class ConnCheckConfig(AtomicConfig):
    def __init__(self, config: typing.Any) -> None:
        allowed = {"host", "port"}
        required = {"port"}
        check_keys(allowed, required, config, type(self).__name__)

        self.host = config.get("host", "localhost")
        self.port = config["port"]

    def build_atomic(
        self, poll: "dc.Poll", logger: "dc.Logger", stream: str, report_fd: int
    ) -> "dc.AtomicOperation":
        return dc.ConnCheck(self.host, self.port, report_fd)


class LogCheckConfig(AtomicConfig):
    def __init__(self, config: typing.Any) -> None:
        allowed = {"regex", "stream"}
        required = {"regex"}
        check_keys(allowed, required, config, type(self).__name__)

        self.regex = config["regex"]
        self.stream = config.get("stream")

        # confirm that the regex is compilable
        re.compile(dc.asbytes(self.regex))

    def build_atomic(
        self, poll: "dc.Poll", logger: "dc.Logger", stream: str, report_fd: int
    ) -> "dc.AtomicOperation":
        # Allow the configured stream to overwrite the default stream.
        s = stream if self.stream is None else self.stream
        return dc.LogCheck(logger, s, report_fd, self.regex)


class CustomAtomicConfig(AtomicConfig):
    def __init__(self, config: typing.Any) -> None:
        check_list_of_strings(config, "AtomicConfig.custom must be a list of strings")
        self.cmd = config

    def build_atomic(
        self, poll: "dc.Poll", logger: "dc.Logger", stream: str, report_fd: int
    ) -> "dc.AtomicOperation":
        return dc.AtomicSubprocess(poll, logger, stream, report_fd, self.cmd)


class ShellAtomicConfig(AtomicConfig):
    def __init__(self, config: typing.Any) -> None:
        assert isinstance(config, str), "AtomicConfig.sh must be a single string"
        self.cmd = ["sh", "-c", config]

    def build_atomic(
        self, poll: "dc.Poll", logger: "dc.Logger", stream: str, report_fd: int
    ) -> "dc.AtomicOperation":
        return dc.AtomicSubprocess(poll, logger, stream, report_fd, self.cmd)


class CustomConfig(StageConfig):
    def __init__(self, config: typing.Any) -> None:
        allowed = {"cmd", "name", "env", "cwd", "pre", "post"}
        required = {"cmd", "name"}

        check_keys(allowed, required, config, type(self).__name__)

        self.cmd = config["cmd"]
        check_list_of_strings(self.cmd, "CustomConfig.cmd must be a list of strings")

        self.name = check_string(config["name"], "CustomConfig.name must be a string")

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

    def build_stage(
        self,
        poll: "dc.Poll",
        logger: "dc.Logger",
        state_machine: "dc.StateMachine",
        process_tracker: "dc.ProcessTracker",
    ) -> "dc.Stage":
        return dc.Process(self, poll, logger, state_machine, process_tracker)


class CustomDockerConfig(StageConfig):
    def __init__(self, config: typing.Any) -> None:
        allowed = {"name", "container_name", "run_args", "kill_signal", "pre", "post"}
        required = {"name", "container_name", "run_args"}

        check_keys(allowed, required, config, type(self).__name__)

        self.container_name = config["container_name"]

        self.run_args = config["run_args"]
        check_list_of_strings(
            self.run_args, "CustomDockerConfig.run_args must be a list of strings"
        )

        self.kill_signal = config.get("kill_signal", "KILL")
        assert isinstance(
            self.kill_signal, str
        ), "CustomDockerConfig.kill_signal must be a string"

        self.name = config["name"]
        assert isinstance(self.name, str), "CustomDockerConfig.name must be a string"

        check_list_of_dicts(
            config.get("pre", []), "CustomDockerConfig.pre must be a list of dicts"
        )
        self.pre = [AtomicConfig.read(pre) for pre in config.get("pre", [])]

        check_list_of_dicts(
            config.get("post", []), "CustomDockerConfig.post must be a list of dicts"
        )
        self.post = [AtomicConfig.read(post) for post in config.get("post", [])]

    def build_stage(
        self,
        poll: "dc.Poll",
        logger: "dc.Logger",
        state_machine: "dc.StateMachine",
        process_tracker: "dc.ProcessTracker",
    ) -> "dc.Stage":
        return dc.DockerProcess(self, poll, logger, state_machine, process_tracker)


class CommandConfig:
    def __init__(self, command: str) -> None:
        self.command = command

    @staticmethod
    def read(config: typing.Any) -> "CommandConfig":
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
    def __init__(self, config: typing.Any) -> None:
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
