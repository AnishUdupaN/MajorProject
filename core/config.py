"""Configuration parsing and management for master and worker nodes."""

from __future__ import annotations

import configparser
import platform
from dataclasses import dataclass
from pathlib import Path

from core.constants import (
    CONFIG_KEY_EXECUTE_COMMAND,
    CONFIG_KEY_MAX_NODES,
    CONFIG_KEY_MERGE_COMMAND,
    CONFIG_KEY_SPLIT_COMMAND,
    CONFIG_KEYS,
)


@dataclass
class Config:
    """System task configuration parsed from an INI file."""

    split_command: str
    execute_command: str
    merge_command: str
    max_nodes: int
    min_nodes: int = 1
    on_high_usage: str = "kill"
    long_runnable: bool = False
    cpu_threshold_percent: float = 20.0
    cpu_threshold_seconds: float = 20.0
    nice_initial: int = 5
    nice_step: int = 5
    nice_max: int = 19
    nice_check_interval_seconds: float = 5.0
    max_throttle_duration_seconds: float = 600.0
    unthrottle_threshold_seconds: float = 20.0
    # Binary distribution settings
    binary_name: str = ""
    binaries_directory: str = "binaries"
    require_binary: bool = False
    resource_type: str = "auto"
    min_vram_mb: int = 0

    @property
    def min_devices(self) -> int:
        return self.min_nodes

    # Platform-specific command overrides
    split_command_macos: str = ""
    split_command_linux: str = ""
    execute_command_macos: str = ""
    execute_command_linux: str = ""
    merge_command_macos: str = ""
    merge_command_linux: str = ""

    def get_execute_command(self, os_type: str | None = None) -> str:
        """Return platform-specific execute command, falling back to execute_command."""
        if not os_type:
            os_type = platform.system().lower()
        else:
            os_type = os_type.lower()
        if ("darwin" in os_type or "mac" in os_type) and self.execute_command_macos:
            return self.execute_command_macos
        elif ("linux" in os_type) and self.execute_command_linux:
            return self.execute_command_linux
        return self.execute_command

    def get_split_command(self, os_type: str | None = None) -> str:
        """Return platform-specific split command, falling back to split_command."""
        if not os_type:
            os_type = platform.system().lower()
        else:
            os_type = os_type.lower()
        if ("darwin" in os_type or "mac" in os_type) and self.split_command_macos:
            return self.split_command_macos
        elif ("linux" in os_type) and self.split_command_linux:
            return self.split_command_linux
        return self.split_command

    def get_merge_command(self, os_type: str | None = None) -> str:
        """Return platform-specific merge command, falling back to merge_command."""
        if not os_type:
            os_type = platform.system().lower()
        else:
            os_type = os_type.lower()
        if ("darwin" in os_type or "mac" in os_type) and self.merge_command_macos:
            return self.merge_command_macos
        elif ("linux" in os_type) and self.merge_command_linux:
            return self.merge_command_linux
        return self.merge_command


def get_available_configs(base_dir: str | Path = ".") -> list[str]:
    """Discover available .ini configuration files in config/ directory and root."""
    base_path = Path(base_dir).resolve()
    configs: list[str] = []

    # Check config/ directory (under base_path and project root)
    for c_dir in [base_path / "config", Path(__file__).resolve().parent.parent / "config"]:
        if c_dir.is_dir():
            for file in sorted(c_dir.glob("*.ini")):
                try:
                    rel_path = str(file.relative_to(base_path))
                except ValueError:
                    rel_path = str(file)
                if rel_path not in configs:
                    configs.append(rel_path)

    # Check root config.ini if it exists
    root_ini = base_path / "config.ini"
    if root_ini.is_file() and "config.ini" not in configs:
        configs.append("config.ini")

    # Check any other root *.ini files
    for file in sorted(base_path.glob("*.ini")):
        try:
            rel_path = str(file.relative_to(base_path))
        except ValueError:
            rel_path = file.name
        if rel_path not in configs:
            configs.append(rel_path)

    return configs


def load_config(config_path: str) -> Config:
    """Load and validate an INI configuration file into a Config dataclass."""
    path = Path(config_path)
    if not path.is_file():
        raise ValueError(f"Config file not found: {config_path}")

    parser = configparser.ConfigParser(interpolation=None)
    parser.read(path)

    if "DEFAULT" not in parser:
        raise ValueError("Config file is missing a [DEFAULT] section")

    section = parser["DEFAULT"]
    missing_keys = [key for key in CONFIG_KEYS if key not in section]
    if missing_keys:
        raise ValueError(
            "Config file is missing required fields: " + ", ".join(missing_keys)
        )

    max_nodes_raw = section[CONFIG_KEY_MAX_NODES].strip()
    try:
        max_nodes = int(max_nodes_raw)
    except ValueError as exc:
        raise ValueError(
            f"max_nodes must be a positive integer, got {max_nodes_raw!r}"
        ) from exc

    if max_nodes < 1:
        raise ValueError(f"max_nodes must be a positive integer, got {max_nodes}")

    min_nodes = section.getint(
        "min_nodes", fallback=section.getint("min_devices", fallback=1)
    )

    on_high_usage = section.get("on_high_usage", "kill").strip().lower()
    if on_high_usage not in ("kill", "throttle"):
        on_high_usage = "kill"

    return Config(
        split_command=section[CONFIG_KEY_SPLIT_COMMAND].strip(),
        execute_command=section[CONFIG_KEY_EXECUTE_COMMAND].strip(),
        merge_command=section[CONFIG_KEY_MERGE_COMMAND].strip(),
        max_nodes=max_nodes,
        min_nodes=min_nodes,
        on_high_usage=on_high_usage,
        long_runnable=section.getboolean("long_runnable", fallback=False),
        cpu_threshold_percent=section.getfloat("cpu_threshold_percent", fallback=20.0),
        cpu_threshold_seconds=section.getfloat("cpu_threshold_seconds", fallback=20.0),
        nice_initial=section.getint("nice_initial", fallback=5),
        nice_step=section.getint("nice_step", fallback=5),
        nice_max=section.getint("nice_max", fallback=19),
        nice_check_interval_seconds=section.getfloat(
            "nice_check_interval_seconds", fallback=5.0
        ),
        max_throttle_duration_seconds=section.getfloat(
            "max_throttle_duration_seconds", fallback=600.0
        ),
        unthrottle_threshold_seconds=section.getfloat(
            "unthrottle_threshold_seconds", fallback=20.0
        ),
        binary_name=section.get("binary_name", "").strip(),
        binaries_directory=section.get("binaries_directory", "binaries").strip(),
        require_binary=section.getboolean("require_binary", fallback=False),
        resource_type=section.get("resource_type", "auto").strip().lower(),
        min_vram_mb=section.getint("min_vram_mb", fallback=0),
        split_command_macos=section.get("split_command_macos", "").strip(),
        split_command_linux=section.get("split_command_linux", "").strip(),
        execute_command_macos=section.get("execute_command_macos", "").strip(),
        execute_command_linux=section.get("execute_command_linux", "").strip(),
        merge_command_macos=section.get("merge_command_macos", "").strip(),
        merge_command_linux=section.get("merge_command_linux", "").strip(),
    )
