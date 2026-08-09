"""Config file loader used by both master and worker startup."""

import configparser
from dataclasses import dataclass
from pathlib import Path

from constants import (
    CONFIG_KEY_EXECUTE_COMMAND,
    CONFIG_KEY_MAX_NODES,
    CONFIG_KEY_MERGE_COMMAND,
    CONFIG_KEY_SPLIT_COMMAND,
    CONFIG_KEYS,
)


@dataclass(frozen=True)
class Config:
    """The four config parameters from the spec."""

    split_command: str
    execute_command: str
    merge_command: str
    max_nodes: int


def load_config(config_path: str) -> Config:
    """Load and validate the config file with exactly 4 fields."""
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
            "Config file is missing required fields: "
            + ", ".join(missing_keys)
        )

    max_nodes_raw = section[CONFIG_KEY_MAX_NODES].strip()
    try:
        max_nodes = int(max_nodes_raw)
    except ValueError as exc:
        raise ValueError(
            f"max_nodes must be a positive integer, got {max_nodes_raw!r}"
        ) from exc

    if max_nodes < 1:
        raise ValueError(
            f"max_nodes must be a positive integer, got {max_nodes}"
        )

    return Config(
        split_command=section[CONFIG_KEY_SPLIT_COMMAND].strip(),
        execute_command=section[CONFIG_KEY_EXECUTE_COMMAND].strip(),
        merge_command=section[CONFIG_KEY_MERGE_COMMAND].strip(),
        max_nodes=max_nodes,
    )
