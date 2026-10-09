"""Core shared library module for distributed video processing.
Consolidates constants, config loading, device mapping, networking, control messaging,
HTTP file transfers, task execution monitoring, video splitting/merging, and UI dashboard.
"""

import argparse
import atexit
import configparser
import hashlib
import json
import os
import platform
import re
import select
import shlex
import shutil
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from urllib.parse import unquote

# ==============================================================================
# SECTION 1: CONSTANTS
# ==============================================================================

CONTROL_PORT = 5000
FIXED_PORT = CONTROL_PORT

DEVICES_JSON_FILE = "devices.json"
DEVICE_ID_PREFIX = "node"

CONFIG_KEY_SPLIT_COMMAND = "split_command"
CONFIG_KEY_EXECUTE_COMMAND = "execute_command"
CONFIG_KEY_MERGE_COMMAND = "merge_command"
CONFIG_KEY_MAX_NODES = "max_nodes"

CONFIG_KEYS = (
    CONFIG_KEY_SPLIT_COMMAND,
    CONFIG_KEY_EXECUTE_COMMAND,
    CONFIG_KEY_MERGE_COMMAND,
    CONFIG_KEY_MAX_NODES,
)

PROGRESS_SPLITTING_FILE = "splitting file"
PROGRESS_SENDING_FILE = "sending file"
PROGRESS_EXECUTING = "executing"
PROGRESS_RECEIVING_FILES = "receiving files"
PROGRESS_FINISHED = "finished"
PROGRESS_MERGING_FILES = "merging files"

MESSAGE_TYPE_READY = "ready"
MESSAGE_TYPE_FILE_RECEIVED = "file_received"
MESSAGE_TYPE_FINISHED = "finished"
MESSAGE_TYPE_FAILED = "failed"
MESSAGE_TYPE_SHUTDOWN = "shutdown"
MESSAGE_TYPE_AUTH = "auth"
# Binary handshake message types
MESSAGE_TYPE_BINARY_INFO_REQUEST = "binary_info_request"
MESSAGE_TYPE_BINARY_INFO = "binary_info"
MESSAGE_TYPE_BINARY_READY = "binary_ready"
MESSAGE_TYPE_WORKER_TELEMETRY = "worker_telemetry"
MESSAGE_TYPE_PIN_CHALLENGE = "pin_challenge"
MESSAGE_TYPE_PIN_RESPONSE = "pin_response"
MESSAGE_TYPE_PIN_ACCEPTED = "pin_accepted"
MESSAGE_TYPE_PIN_REJECTED = "pin_rejected"

# Message schema validation: required keys for each message type.
MESSAGE_REQUIRED_KEYS = {
    MESSAGE_TYPE_READY: ['type', 'master_ip_address', 'file_transfer_port'],
    MESSAGE_TYPE_FILE_RECEIVED: ['type'],
    MESSAGE_TYPE_FINISHED: ['type', 'execution_time', 'part_filename'],
    MESSAGE_TYPE_FAILED: ['type', 'reason'],
    MESSAGE_TYPE_SHUTDOWN: ['type'],
    MESSAGE_TYPE_AUTH: ['type', 'secret'],
    # Binary handshake schemas
    MESSAGE_TYPE_BINARY_INFO_REQUEST: ['type', 'binary_name'],
    MESSAGE_TYPE_BINARY_INFO: ['type', 'os_name', 'arch', 'binary_name', 'has_binary', 'md5'],
    MESSAGE_TYPE_BINARY_READY: ['type'],
    MESSAGE_TYPE_WORKER_TELEMETRY: ['type', 'single_core_score', 'multi_core_score', 'os_type', 'cpu_count'],
    MESSAGE_TYPE_PIN_CHALLENGE: ['type', 'master_name'],
    MESSAGE_TYPE_PIN_RESPONSE: ['type', 'pin'],
    MESSAGE_TYPE_PIN_ACCEPTED: ['type'],
    MESSAGE_TYPE_PIN_REJECTED: ['type', 'reason', 'attempts_left'],
}


def validate_message(msg: dict) -> None:
    """Validate that a received message has the correct structure and required keys."""
    if not isinstance(msg, dict):
        raise ValueError(f"Expected dict message, got {type(msg).__name__}")
    msg_type = msg.get('type')
    if msg_type is None:
        raise ValueError("Message missing required 'type' field")
    required = MESSAGE_REQUIRED_KEYS.get(msg_type)
    if required is None:
        raise ValueError(f"Unknown message type: {msg_type!r}")
    missing = [key for key in required if key not in msg]
    if missing:
        raise ValueError(f"Message type '{msg_type}' missing required keys: {', '.join(missing)}")


# Shared-secret authentication via environment variable.
SHARED_SECRET_ENV_VAR = "DIST_SHARED_SECRET"


def get_shared_secret() -> str | None:
    """Read the optional shared secret from environment variable."""
    secret = os.environ.get(SHARED_SECRET_ENV_VAR, "").strip()
    return secret if secret else None


def send_auth_message(connection: socket.socket, secret: str) -> None:
    """Send an authentication message with the shared secret."""
    send_json_message(connection, {"type": MESSAGE_TYPE_AUTH, "secret": secret})


def verify_auth_message(connection: socket.socket, expected_secret: str) -> bool:
    """Receive and verify an authentication message. Returns True if valid."""
    try:
        msg = receive_json_message(connection)
        if msg.get("type") != MESSAGE_TYPE_AUTH:
            return False
        return msg.get("secret") == expected_secret
    except (ValueError, ConnectionError, json.JSONDecodeError):
        return False

PART_FILENAME_PREFIX = "part"
PART_FILENAME_SUFFIX = ".mkv"

# Role-specific working directories under the project root.
MASTER_DIRECTORY = "master"
WORKER_DIRECTORY = "worker"

MASTER_INPUT_DIRECTORY = f"{MASTER_DIRECTORY}/input"
MASTER_OUTPUT_DIRECTORY = f"{MASTER_DIRECTORY}/output"
WORKER_INPUT_DIRECTORY = f"{WORKER_DIRECTORY}/input"
WORKER_OUTPUT_DIRECTORY = f"{WORKER_DIRECTORY}/output"
WORKER_BINARY_DIRECTORY = f"{WORKER_DIRECTORY}/binaries"

DEFAULT_INPUT_VIDEO = f"{MASTER_INPUT_DIRECTORY}/input.mkv"


# ==============================================================================
# SECTION 1b: BINARY DISTRIBUTION HELPERS
# ==============================================================================

# Mapping from (platform.system(), platform.machine()) → (os_folder, arch_folder)
# supported targets: linux (x64) and macos (aarch64)
BINARY_PLATFORM_MAP: dict[tuple[str, str], tuple[str, str]] = {
    ("Linux",   "x86_64"):  ("linux", "x64"),
    ("Darwin",  "arm64"):   ("macos", "arm64"),
}


def get_worker_platform() -> tuple[str, str]:
    """Return (os_folder, arch_folder) for supported worker platform targets."""
    sys_name = platform.system()
    machine = platform.machine()
    key = (sys_name, machine)
    if key in BINARY_PLATFORM_MAP:
        return BINARY_PLATFORM_MAP[key]
    os_folder = "macos" if sys_name == "Darwin" else sys_name.lower()
    arch_folder = "aarch64" if machine in ("arm64", "aarch64") else ("x64" if machine in ("x86_64", "AMD64") else machine.lower())
    return os_folder, arch_folder


def compute_md5(filepath: str | Path) -> str:
    """Compute the MD5 hex digest of a file using chunked reads (memory-efficient).

    MD5 is chosen because it is natively available in Python's hashlib on all
    platforms (unlike MD4, which was removed from OpenSSL), and hashing a typical
    binary (~50 MB) takes well under 100 ms — fast enough for a pre-task handshake.
    """
    h = hashlib.md5()
    with open(filepath, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def get_binary_path(
    binaries_directory: str,
    os_folder: str,
    arch_folder: str,
    binary_name: str,
) -> Path | None:
    """Return the Path to the binary for the given OS/arch, or None if not found.

    Supports layout:
      <binaries_directory>/<binary_name>/<os_folder>/<binary_name>
    as well as fallback legacy layout:
      <binaries_directory>/<os_folder>/<arch_folder>/<binary_name>
    """
    # Primary directory layout: binaries/<binary_name>/<os_folder>/<binary_name>
    primary = Path(binaries_directory) / binary_name / os_folder / binary_name
    if primary.is_file():
        return primary

    # Secondary check: if there is a directory or executable matching under primary parent
    primary_dir = Path(binaries_directory) / binary_name / os_folder
    if primary_dir.is_dir():
        # Check for executable/file in primary_dir (e.g., blender executable or app)
        for item in primary_dir.iterdir():
            if item.is_file() and not item.name.startswith("."):
                return item

    # Legacy fallback: binaries/<os_folder>/<arch_folder>/<binary_name>
    legacy = Path(binaries_directory) / os_folder / arch_folder / binary_name
    if legacy.is_file():
        return legacy

    return None


def resolve_worker_binary_cache_path(
    os_folder: str,
    arch_folder: str,
    binary_name: str,
) -> Path:
    """Return the expected local cache path for a binary on a worker node.

    Path: worker/binaries/<binary_name>/<os_folder>/<binary_name>
    The directory is created automatically if it doesn't exist.
    """
    cache_dir = Path(WORKER_BINARY_DIRECTORY) / binary_name / os_folder
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / binary_name


# ------ Binary handshake messaging functions ------

def send_binary_info_request(connection: socket.socket, binary_name: str) -> None:
    """Master asks a worker for its platform info and whether it has the binary."""
    send_json_message(connection, {
        "type": MESSAGE_TYPE_BINARY_INFO_REQUEST,
        "binary_name": binary_name,
    })


def receive_binary_info_request(connection: socket.socket) -> dict:
    """Worker receives the master's binary_info_request message."""
    msg = receive_json_message(connection)
    if msg.get("type") != MESSAGE_TYPE_BINARY_INFO_REQUEST:
        raise ValueError(f"Expected binary_info_request message, got: {msg!r}")
    return msg


def send_binary_info(
    connection: socket.socket,
    os_name: str,
    arch: str,
    binary_name: str,
    has_binary: bool,
    md5: str | None,
) -> None:
    """Worker replies with its platform and binary status (md5 is None if absent)."""
    send_json_message(connection, {
        "type": MESSAGE_TYPE_BINARY_INFO,
        "os_name": os_name,
        "arch": arch,
        "binary_name": binary_name,
        "has_binary": has_binary,
        "md5": md5 or "",
    })


def receive_binary_info(connection: socket.socket) -> dict:
    """Master receives the worker's binary_info message."""
    msg = receive_json_message(connection)
    if msg.get("type") != MESSAGE_TYPE_BINARY_INFO:
        raise ValueError(f"Expected binary_info message, got: {msg!r}")
    return msg


def send_binary_ready(connection: socket.socket) -> None:
    """Master tells the worker that binary setup is complete (present or just sent)."""
    send_json_message(connection, {"type": MESSAGE_TYPE_BINARY_READY})


def receive_binary_ready(connection: socket.socket) -> None:
    """Worker waits for the master's binary_ready signal before proceeding."""
    msg = receive_json_message(connection)
    if msg.get("type") != MESSAGE_TYPE_BINARY_READY:
        raise ValueError(f"Expected binary_ready message, got: {msg!r}")


def send_worker_telemetry(connection: socket.socket, telemetry: dict) -> None:
    send_json_message(connection, {"type": MESSAGE_TYPE_WORKER_TELEMETRY, **telemetry})


def receive_worker_telemetry(connection: socket.socket) -> dict:
    msg = receive_json_message(connection)
    validate_message(msg)
    if msg["type"] != MESSAGE_TYPE_WORKER_TELEMETRY:
        raise ValueError(f"Expected worker_telemetry, got {msg['type']}")
    return msg


def generate_pin() -> str:
    import random
    bad_pins = {"0000", "1111", "1234", "4321", "9876", "0123"}
    while True:
        pin = f"{random.randint(0, 9999):04d}"
        if pin not in bad_pins:
            return pin


def send_pin_challenge(connection: socket.socket, master_name: str) -> None:
    send_json_message(connection, {"type": MESSAGE_TYPE_PIN_CHALLENGE, "master_name": master_name})


def receive_pin_challenge(connection: socket.socket) -> dict:
    msg = receive_json_message(connection)
    validate_message(msg)
    if msg["type"] != MESSAGE_TYPE_PIN_CHALLENGE:
        raise ValueError(f"Expected pin_challenge, got {msg['type']}")
    return msg


def send_pin_response(connection: socket.socket, pin: str) -> None:
    send_json_message(connection, {"type": MESSAGE_TYPE_PIN_RESPONSE, "pin": pin})


def receive_pin_response(connection: socket.socket) -> dict:
    msg = receive_json_message(connection)
    validate_message(msg)
    if msg["type"] != MESSAGE_TYPE_PIN_RESPONSE:
        raise ValueError(f"Expected pin_response, got {msg['type']}")
    return msg


def send_pin_accepted(connection: socket.socket) -> None:
    send_json_message(connection, {"type": MESSAGE_TYPE_PIN_ACCEPTED})


def send_pin_rejected(connection: socket.socket, reason: str, attempts_left: int) -> None:
    send_json_message(connection, {"type": MESSAGE_TYPE_PIN_REJECTED, "reason": reason, "attempts_left": attempts_left})




# ==============================================================================
# SECTION 2: CONFIGURATION LOADER
# ==============================================================================

@dataclass(frozen=True)
class Config:
    split_command: str
    execute_command: str
    merge_command: str
    max_nodes: int
    min_devices: int = 2
    on_high_usage: str = "kill"
    long_runnable: bool = False
    cpu_threshold_percent: float = 20.0
    cpu_threshold_seconds: float = 20.0
    nice_initial: int = 5
    nice_step: int = 5
    nice_max: int = 19
    nice_check_interval_seconds: float = 60.0
    max_throttle_duration_seconds: float = 600.0
    unthrottle_threshold_seconds: float = 20.0
    # Binary distribution settings
    binary_name: str = ""
    binaries_directory: str = "binaries"
    require_binary: bool = False
    resource_type: str = "auto"
    min_vram_mb: int = 0
    # OS-specific command overrides for bare-metal execution
    split_command_windows: str = ""
    split_command_macos: str = ""
    split_command_linux: str = ""
    execute_command_windows: str = ""
    execute_command_macos: str = ""
    execute_command_linux: str = ""
    merge_command_windows: str = ""
    merge_command_macos: str = ""
    merge_command_linux: str = ""

    def get_execute_command(self, os_type: str | None = None) -> str:
        """Return platform-specific execute command, falling back to execute_command."""
        if not os_type:
            os_type = platform.system().lower()
        else:
            os_type = os_type.lower()
        if ("win" in os_type) and self.execute_command_windows:
            return self.execute_command_windows
        elif ("darwin" in os_type or "mac" in os_type) and self.execute_command_macos:
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
        if ("win" in os_type) and self.split_command_windows:
            return self.split_command_windows
        elif ("darwin" in os_type or "mac" in os_type) and self.split_command_macos:
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
        if ("win" in os_type) and self.merge_command_windows:
            return self.merge_command_windows
        elif ("darwin" in os_type or "mac" in os_type) and self.merge_command_macos:
            return self.merge_command_macos
        elif ("linux" in os_type) and self.merge_command_linux:
            return self.merge_command_linux
        return self.merge_command



_LOG_LOCK = threading.Lock()

def printlog(st):
    with _LOG_LOCK:
        with open("logs.txt", "a+") as f:
            f.write(st + "\n")


def get_available_configs(base_dir: str | Path = ".") -> list[str]:
    """Discover available .ini configuration files in config/ directory and root."""
    base_path = Path(base_dir).resolve()
    configs = []

    # Check config/ directory first
    config_dir = base_path / "config"
    if config_dir.is_dir():
        for file in sorted(config_dir.glob("*.ini")):
            try:
                rel_path = str(file.relative_to(base_path))
            except ValueError:
                rel_path = file.name
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
        raise ValueError("Config file is missing required fields: "+ ", ".join(missing_keys))

    max_nodes_raw = section[CONFIG_KEY_MAX_NODES].strip()
    try:
        max_nodes = int(max_nodes_raw)
    except ValueError as exc:
        raise ValueError(f"max_nodes must be a positive integer, got {max_nodes_raw!r}") from exc

    if max_nodes < 1:
        raise ValueError(f"max_nodes must be a positive integer, got {max_nodes}")

    min_devices = section.getint("min_devices", fallback=max_nodes)

    on_high_usage = section.get("on_high_usage", "kill").strip().lower()
    if on_high_usage not in ("kill", "throttle"):
        on_high_usage = "kill"

    return Config(
        split_command=section[CONFIG_KEY_SPLIT_COMMAND].strip(),
        execute_command=section[CONFIG_KEY_EXECUTE_COMMAND].strip(),
        merge_command=section[CONFIG_KEY_MERGE_COMMAND].strip(),
        max_nodes=max_nodes,
        min_devices=min_devices,
        on_high_usage=on_high_usage,
        long_runnable=section.getboolean("long_runnable", fallback=False),
        cpu_threshold_percent=section.getfloat("cpu_threshold_percent", fallback=20.0),
        cpu_threshold_seconds=section.getfloat("cpu_threshold_seconds", fallback=20.0),
        nice_initial=section.getint("nice_initial", fallback=5),
        nice_step=section.getint("nice_step", fallback=5),
        nice_max=section.getint("nice_max", fallback=19),
        nice_check_interval_seconds=section.getfloat("nice_check_interval_seconds", fallback=60.0),
        max_throttle_duration_seconds=section.getfloat("max_throttle_duration_seconds", fallback=600.0),
        unthrottle_threshold_seconds=section.getfloat("unthrottle_threshold_seconds", fallback=20.0),
        binary_name=section.get("binary_name", "").strip(),
        binaries_directory=section.get("binaries_directory", "binaries").strip(),
        require_binary=section.getboolean("require_binary", fallback=False),
        resource_type=section.get("resource_type", "auto").strip().lower(),
        min_vram_mb=section.getint("min_vram_mb", fallback=0),
        split_command_windows=section.get("split_command_windows", "").strip(),
        split_command_macos=section.get("split_command_macos", "").strip(),
        split_command_linux=section.get("split_command_linux", "").strip(),
        execute_command_windows=section.get("execute_command_windows", "").strip(),
        execute_command_macos=section.get("execute_command_macos", "").strip(),
        execute_command_linux=section.get("execute_command_linux", "").strip(),
        merge_command_windows=section.get("merge_command_windows", "").strip(),
        merge_command_macos=section.get("merge_command_macos", "").strip(),
        merge_command_linux=section.get("merge_command_linux", "").strip(),
    )




# ==============================================================================
# SECTION 3: DEVICE ID & JSON WRITER
# ==============================================================================

def assign_device_id(position: int) -> str:
    """Assign a device ID in IP-list order (node1, node2, ...)."""
    return f"{DEVICE_ID_PREFIX}{position}"


def build_active_device_mapping(active_ip_addresses: list[str]) -> dict[str, str]:
    """Map each active node's IP address to a device ID; spares are excluded."""
    return {
        ip_address: assign_device_id(index)
        for index, ip_address in enumerate(active_ip_addresses, start=1)
    }


def write_devices_json(
    active_ip_addresses: list[str],
    devices_json_path: str = DEVICES_JSON_FILE,
) -> dict[str, str]:
    """Write devices.json for active nodes only, before splitting starts."""
    device_mapping = build_active_device_mapping(active_ip_addresses)
    path = Path(devices_json_path)
    path.write_text(json.dumps(device_mapping, indent=2) + "\n")
    return device_mapping


def swap_device_ip(
    old_ip: str,
    new_ip: str,
    devices_json_path: str = DEVICES_JSON_FILE,
) -> dict[str, str]:
    """Replace a killed node's IP with the spare node's IP, keeping the device ID."""
    path = Path(devices_json_path)
    if not path.is_file():
        raise FileNotFoundError(f"{devices_json_path} does not exist")

    mapping: dict[str, str] = json.loads(path.read_text())
    if old_ip not in mapping:
        for k in list(mapping.keys()):
            if k == old_ip or k.split(":")[0] == old_ip.split(":")[0]:
                old_ip = k
                break
    if old_ip not in mapping:
        raise KeyError(f"IP address {old_ip} not found in {devices_json_path}")

    device_id = mapping.pop(old_ip)
    mapping[new_ip] = device_id
    path.write_text(json.dumps(mapping, indent=2) + "\n")
    return mapping


# ==============================================================================
# SECTION 4: NETWORK HELPERS & MDNS AUTO-DISCOVERY
# ==============================================================================

MDNS_ADDR = "224.0.0.251"
MDNS_PORT = 5353
MDNS_SERVICE_MASTER = "_distcompute._tcp.local"
MDNS_MASTER_FQDN = "distcompute-master.local"
MDNS_SERVICE_WORKER = "_distworker._tcp.local"
MDNS_WORKER_FQDN = "distcompute-worker.local"
MDNS_KEYWORD = "distcompute"
MDNS_KEYWORD_MASTER = "distcompute-master"
MDNS_KEYWORD_WORKER = "distcompute-worker"


def get_local_ip() -> str:
    """Determine the local machine's primary outbound LAN IP."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"


def encode_dns_label(name: str) -> bytes:
    """Encode a DNS name into label wire format (no compression)."""
    out = b""
    for part in name.rstrip(".").split("."):
        enc = part.encode("utf-8")
        out += bytes([len(enc)]) + enc
    out += b"\x00"
    return out


def build_a_record(fqdn: str, ipv4: str, ttl: int = 4500) -> bytes:
    """Return a DNS A-record answer RR in wire format."""
    name = encode_dns_label(fqdn)
    rdata = socket.inet_aton(ipv4)
    rr = struct.pack("!HHIH", 1, 0x8001, ttl, 4)  # type A, class IN+flush
    return name + rr + rdata


def build_ptr_record(service: str, instance: str, ttl: int = 4500) -> bytes:
    """Return a DNS PTR-record answer RR in wire format."""
    name = encode_dns_label(service)
    rdata = encode_dns_label(instance)
    rr = struct.pack("!HHIH", 12, 0x0001, ttl, len(rdata))  # type PTR
    return name + rr + rdata


def build_mdns_announcement(
    local_ip: str, fqdn: str = MDNS_MASTER_FQDN, service: str = MDNS_SERVICE_MASTER
) -> bytes:
    """Assemble a full mDNS (DNS-SD) response packet."""
    a_rec = build_a_record(fqdn, local_ip)
    ptr_rec = build_ptr_record(service, f"{fqdn.split('.')[0]}.{service}")

    header = struct.pack(
        "!HHHHHH",
        0x0000,  # transaction id (always 0 for mDNS)
        0x8400,  # flags: response + authoritative
        0,       # questions
        2,       # answer RRs
        0,       # authority RRs
        0,       # additional RRs
    )
    return header + ptr_rec + a_rec


def parse_dns_name(data: bytes, offset: int) -> tuple[str, int]:
    """Parse a DNS wire-format name starting at offset."""
    labels = []
    visited = set()

    while True:
        if offset >= len(data):
            break
        length = data[offset]

        if length == 0:
            offset += 1
            break
        elif (length & 0xC0) == 0xC0:
            if offset + 1 >= len(data):
                break
            ptr = ((length & 0x3F) << 8) | data[offset + 1]
            offset += 2
            if ptr in visited:
                break
            visited.add(ptr)
            label, _ = parse_dns_name(data, ptr)
            labels.append(label)
            break
        else:
            offset += 1
            end = offset + length
            labels.append(data[offset:end].decode("utf-8", errors="replace"))
            offset = end

    return ".".join(labels), offset


def extract_a_record_ip(data: bytes, keyword: str = MDNS_KEYWORD_MASTER) -> str | None:
    """Walk through DNS answer RRs looking for an A record whose name matches keyword."""
    try:
        if len(data) < 12:
            return None
        if keyword.encode("utf-8") not in data:
            return None

        qdcount = struct.unpack_from("!H", data, 4)[0]
        ancount = struct.unpack_from("!H", data, 6)[0]

        offset = 12
        for _ in range(qdcount):
            _, offset = parse_dns_name(data, offset)
            offset += 4  # QTYPE + QCLASS

        for _ in range(ancount):
            name, offset = parse_dns_name(data, offset)
            if offset + 10 > len(data):
                break
            rtype, rclass, ttl, rdlen = struct.unpack_from("!HHIH", data, offset)
            offset += 10
            rdata = data[offset : offset + rdlen]
            offset += rdlen

            if rtype == 1 and rdlen == 4:  # A record
                ip = socket.inet_ntoa(rdata)
                if keyword.lower() in name.lower():
                    return ip
    except Exception:
        pass
    return None


def make_mdns_sender_socket() -> socket.socket:
    """Create a UDP socket for sending mDNS multicast announcements."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except AttributeError:
        pass
    try:
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 255)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
    except OSError:
        pass
    return sock


def make_mdns_listener_socket() -> socket.socket:
    """Create a UDP socket listening on the mDNS multicast group (224.0.0.251:5353)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except AttributeError:
        pass
    try:
        sock.bind(("", MDNS_PORT))
    except OSError:
        sock.bind(("0.0.0.0", MDNS_PORT))

    try:
        mreq = struct.pack("4sL", socket.inet_aton(MDNS_ADDR), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    except OSError:
        try:
            local_ip = get_local_ip()
            mreq = struct.pack("4s4s", socket.inet_aton(MDNS_ADDR), socket.inet_aton(local_ip))
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        except OSError:
            pass
    return sock


class MdnsAnnouncer:
    """Background thread announcing node presence over mDNS multicast."""

    def __init__(
        self,
        local_ip: str | None = None,
        fqdn: str = MDNS_MASTER_FQDN,
        service: str = MDNS_SERVICE_MASTER,
        interval_seconds: float = 2.0,
    ) -> None:
        self.local_ip = local_ip or get_local_ip()
        self.fqdn = fqdn
        self.service = service
        self.interval_seconds = interval_seconds
        self.running = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self.running = True
        self._thread = threading.Thread(target=self._announce_loop, daemon=True)
        self._thread.start()

    def _announce_loop(self) -> None:
        packet = build_mdns_announcement(self.local_ip, self.fqdn, self.service)
        try:
            with make_mdns_sender_socket() as sock:
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                except Exception:
                    pass
                while self.running:
                    try:
                        sock.sendto(packet, (MDNS_ADDR, MDNS_PORT))
                    except Exception:
                        pass
                    try:
                        sock.sendto(packet, ("255.255.255.255", MDNS_PORT))
                    except Exception:
                        pass
                    try:
                        sock.sendto(packet, ("127.0.0.1", MDNS_PORT))
                    except Exception:
                        pass
                    time.sleep(self.interval_seconds)
        except Exception:
            pass

    def stop(self) -> None:
        self.running = False


def discover_master_ip(timeout_seconds: float = 60.0) -> str:
    """Listen on mDNS multicast group for Master announcement; return Master IP or raise TimeoutError after timeout."""
    start_time = time.time()
    deadline = start_time + timeout_seconds

    with make_mdns_listener_socket() as sock:
        sock.settimeout(1.0)
        print(f"Searching for Master via mDNS on {MDNS_ADDR}:{MDNS_PORT} (timeout: {int(timeout_seconds)}s)...")
        while time.time() < deadline:
            try:
                data, addr = sock.recvfrom(4096)
                master_ip = extract_a_record_ip(data, MDNS_KEYWORD_MASTER)
                if master_ip:
                    print(f"\n[mDNS DISCOVERY] Master found at {master_ip}!")
                    return master_ip
            except socket.timeout:
                continue
            except Exception:
                time.sleep(0.5)

    raise TimeoutError(
        f"No Master device found via mDNS within {int(timeout_seconds)} seconds!"
    )



def get_local_ip_for_peer(peer_ip_address: str) -> str:
    """Return the local IP address used to reach a peer on the LAN."""
    peer_ip = peer_ip_address.split(":")[0]
    connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        connection.connect((peer_ip, 9))
        return connection.getsockname()[0]
    finally:
        connection.close()



# ==============================================================================
# SECTION 5: CONTROL MESSAGES & TCP PROTOCOL
# ==============================================================================

class MasterShutdownError(Exception):
    """Raised when master sends a shutdown message to worker nodes."""

    pass


_SOCKET_BUFFERS: dict[socket.socket, str] = {}


def send_json_message(connection: socket.socket, message: dict) -> None:
    """Send one JSON control message terminated by a newline."""
    payload = json.dumps(message) + "\n"
    connection.sendall(payload.encode("utf-8"))


def receive_json_message(connection: socket.socket, timeout: float | None = None) -> dict:
    """Receive one JSON control message from the connection, buffering extra lines."""
    buf = _SOCKET_BUFFERS.get(connection, "")
    while "\n" not in buf:
        if timeout is not None:
            r, _, _ = select.select([connection], [], [], timeout)
            if not r:
                raise TimeoutError("Timeout waiting for JSON message")
        chunk = connection.recv(4096)
        if not chunk:
            _SOCKET_BUFFERS.pop(connection, None)
            raise ConnectionError("Control connection closed before message was received")
        buf += chunk.decode("utf-8")

    line, remainder = buf.split("\n", 1)
    _SOCKET_BUFFERS[connection] = remainder
    message = json.loads(line)
    validate_message(message)
    return message


def has_buffered_message(connection: socket.socket) -> bool:
    """Return True if an unread newline-terminated message is buffered for this socket."""
    return "\n" in _SOCKET_BUFFERS.get(connection, "")


def send_shutdown_message(connection: socket.socket) -> None:
    """Master sends a shutdown message to worker nodes to signal clean termination."""
    send_json_message(connection, {"type": MESSAGE_TYPE_SHUTDOWN})


def send_ready_message(
    connection: socket.socket, master_ip_address: str, file_transfer_port: int, execute_command: str | None = None
) -> None:
    """Master sends a ready message with this node's file-transfer daemon port."""
    msg = {
        "type": MESSAGE_TYPE_READY,
        "master_ip_address": master_ip_address,
        "file_transfer_port": file_transfer_port,
    }
    if execute_command:
        msg["execute_command"] = execute_command
    send_json_message(connection, msg)


def receive_ready_message(connection: socket.socket, timeout: float | None = None) -> dict:
    """Worker receives the ready message from the master."""
    message = receive_json_message(connection, timeout=timeout)
    if message.get("type") == MESSAGE_TYPE_SHUTDOWN:
        raise MasterShutdownError("Master sent shutdown signal")
    if message.get("type") != MESSAGE_TYPE_READY:
        raise ValueError(f"Expected ready message, got: {message!r}")
    return message


def send_file_received_message(connection: socket.socket) -> None:
    """Worker sends file_received after fetching its allocated part file."""
    send_json_message(connection, {"type": MESSAGE_TYPE_FILE_RECEIVED})


def receive_file_received_message(connection: socket.socket) -> None:
    """Master waits for file_received after sending a node's part file."""
    message = receive_json_message(connection)
    if message.get("type") != MESSAGE_TYPE_FILE_RECEIVED:
        raise ValueError(f"Expected file_received message, got: {message!r}")


def send_finished_message(
    connection: socket.socket, execution_time_seconds: float, part_filename: str
) -> None:
    """Worker sends finished message to master on completing execution."""
    send_json_message(
        connection,
        {
            "type": MESSAGE_TYPE_FINISHED,
            "execution_time": execution_time_seconds,
            "part_filename": part_filename,
        },
    )


def receive_finished_message(connection: socket.socket) -> dict:
    """Master receives finished message from worker."""
    message = receive_json_message(connection)
    if message.get("type") != MESSAGE_TYPE_FINISHED:
        raise ValueError(f"Expected finished message, got: {message!r}")
    return message


def send_failed_message(connection: socket.socket, reason: str) -> None:
    """Worker sends failed message to master when task fails or is killed."""
    send_json_message(
        connection,
        {
            "type": MESSAGE_TYPE_FAILED,
            "reason": reason,
        },
    )


def receive_failed_message(connection: socket.socket) -> dict:
    """Master receives failed message from worker."""
    message = receive_json_message(connection)
    if message.get("type") != MESSAGE_TYPE_FAILED:
        raise ValueError(f"Expected failed message, got: {message!r}")
    return message


# ==============================================================================
# SECTION 6: WORKER HTTP CLIENT FILE REQUESTS
# ==============================================================================

def request_file_list(master_ip_address: str, file_transfer_port: int) -> list[str]:
    """Worker calls GET /listfiles to see what's allocated to it."""
    url = f"http://{master_ip_address}:{file_transfer_port}/listfiles"
    with urllib.request.urlopen(url, timeout=10.0) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload["files"]


def request_file(
    master_ip_address: str,
    file_transfer_port: int,
    filename: str,
    destination_directory: str = ".",
) -> str:
    """Worker calls GET /file/<filename> to fetch its allocated part file."""
    url = f"http://{master_ip_address}:{file_transfer_port}/file/{filename}"
    destination_path = Path(destination_directory) / filename
    try:
        with urllib.request.urlopen(url, timeout=10.0) as response:
            destination_path.write_bytes(response.read())
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Failed to fetch {filename}: HTTP {exc.code}") from exc
    return str(destination_path)


def upload_result_file(
    master_ip_address: str,
    file_transfer_port: int,
    source_filepath: str,
) -> None:
    """Worker calls POST /file/<filename> to send its output file back to master's daemon."""
    path = Path(source_filepath)
    if not path.is_file():
        raise FileNotFoundError(f"Result file to upload does not exist: {source_filepath}")

    filename = path.name
    file_bytes = path.read_bytes()
    url = f"http://{master_ip_address}:{file_transfer_port}/file/{filename}"

    request = urllib.request.Request(
        url,
        data=file_bytes,
        headers={
            "Content-Type": "application/octet-stream",
            "Content-Length": str(len(file_bytes)),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10.0) as response:
            if response.status != 200:
                raise RuntimeError(f"Failed to upload {filename}: HTTP {response.status}")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Failed to upload {filename}: HTTP {exc.code}") from exc


# ==============================================================================
# SECTION 7: HTTP FILE-TRANSFER DAEMON & PROCESS MANAGEMENT
# ==============================================================================

DAEMON_READY_TIMEOUT_SECONDS = 10.0
DAEMON_LISTENING_PREFIX = "LISTENING "


_active_daemons_registry: set["FileTransferDaemon"] = set()


def _cleanup_all_daemons() -> None:
    for daemon in list(_active_daemons_registry):
        try:
            daemon.stop()
        except Exception:
            pass


atexit.register(_cleanup_all_daemons)


class FileTransferDaemon:
    """Serve one node's allocated file(s) over HTTP on an ephemeral port."""

    def __init__(
        self,
        allocated_filenames: list[str],
        serve_directory: str,
        output_directory: str | None = None,
    ) -> None:
        self.allocated_filenames = allocated_filenames
        self.serve_directory = serve_directory
        self.output_directory = output_directory
        self.port: int | None = None
        self._process: subprocess.Popen[str] | None = None

    def start(self) -> None:
        """Start this node's file-transfer daemon in a separate process."""
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--serve-directory",
            self.serve_directory,
        ]
        if self.output_directory:
            command.extend(["--output-directory", self.output_directory])
        command.extend(["--files", *self.allocated_filenames])

        self._process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            text=True,
        )

        _active_daemons_registry.add(self)

        self.port = read_daemon_port(self._process)
        wait_for_daemon_port(self._process, self.port)

    def stop(self) -> None:
        """Stop this node's file-transfer daemon process immediately."""
        _active_daemons_registry.discard(self)
        if self._process is not None and self._process.poll() is None:
            try:
                self._process.kill()
                self._process.wait(timeout=0.2)
            except Exception:
                pass



def _make_handler_class(
    allocated_filenames: list[str],
    serve_directory: Path,
    output_directory: Path | None = None,
) -> type[BaseHTTPRequestHandler]:
    """Build a request handler bound to one node's allocated files."""
    files_for_node = allocated_filenames
    directory_for_node = serve_directory
    output_dir_for_node = output_directory or serve_directory

    class FileTransferRequestHandler(BaseHTTPRequestHandler):
        """Handle GET /listfiles, GET /file/<filename>, and POST /file/<filename> for one node."""

        allocated_filenames = files_for_node
        serve_directory = directory_for_node
        output_directory = output_dir_for_node

        def log_message(self, format: str, *args) -> None:
            return

        def do_GET(self) -> None:
            if self.path == "/listfiles":
                self._send_listfiles()
                return
            if self.path.startswith("/file/"):
                self._send_file()
                return
            self.send_error(404, "Not Found")

        def do_POST(self) -> None:
            if self.path.startswith("/file/") or self.path.startswith("/upload/"):
                self._receive_file()
                return
            self.send_error(404, "Not Found")

        def _send_listfiles(self) -> None:
            """GET /listfiles lists the file(s) allocated to this specific node."""
            payload = json.dumps({"files": self.allocated_filenames}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _send_file(self) -> None:
            """GET /file/<filename> sends the requested file's bytes back to the worker."""
            filename = unquote(self.path.removeprefix("/file/").lstrip("/"))
            if filename not in self.allocated_filenames:
                self.send_error(404, "Not Found")
                return

            file_path = self.serve_directory / filename
            if not file_path.is_file():
                self.send_error(404, "Not Found")
                return

            file_bytes = file_path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(file_bytes)))
            self.end_headers()
            self.wfile.write(file_bytes)

        def _receive_file(self) -> None:
            """POST /file/<filename> receives uploaded result file bytes from worker."""
            filename = unquote(
                self.path.removeprefix("/file/")
                .removeprefix("/upload/")
                .lstrip("/")
            )
            # VULN-04: Sanitize filename to prevent path traversal
            filename = os.path.basename(filename)
            if not filename or filename.startswith('.'):
                self.send_error(400, "Bad Request - invalid filename")
                return
            content_length = int(self.headers.get("Content-Length", 0))
            if content_length <= 0:
                self.send_error(400, "Bad Request - missing Content-Length")
                return

            file_bytes = self.rfile.read(content_length)
            self.output_directory.mkdir(parents=True, exist_ok=True)
            output_file_path = self.output_directory / filename
            output_file_path.write_bytes(file_bytes)

            payload = json.dumps({"status": "received", "filename": filename}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    return FileTransferRequestHandler


def read_daemon_port(
    process: subprocess.Popen[str],
    timeout_seconds: float = DAEMON_READY_TIMEOUT_SECONDS,
) -> int:
    """Read the ephemeral port chosen by a file-transfer daemon subprocess."""
    if process.stdout is None:
        raise RuntimeError("File-transfer daemon stdout is not available")

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                "File-transfer daemon exited before reporting its listening port "
                f"with code {process.returncode}"
            )
        line = process.stdout.readline()
        if not line:
            time.sleep(0.1)
            continue
        if line.startswith(DAEMON_LISTENING_PREFIX):
            return int(line.removeprefix(DAEMON_LISTENING_PREFIX).strip())
        raise RuntimeError(
            f"Unexpected output from file-transfer daemon: {line.strip()!r}"
        )
    raise RuntimeError(
        "File-transfer daemon did not report its listening port within "
        f"{timeout_seconds}s"
    )


def wait_for_daemon_port(
    process: subprocess.Popen[str],
    port: int,
    timeout_seconds: float = DAEMON_READY_TIMEOUT_SECONDS,
) -> None:
    """Wait until the file-transfer daemon is accepting connections on its port."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"File-transfer daemon on port {port} exited with code {process.returncode}"
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(
        f"File-transfer daemon on port {port} did not become ready within {timeout_seconds}s"
    )


def _parent_watchdog(parent_pid: int, server: HTTPServer) -> None:
    """Watchdog thread that shuts down the server if parent process dies."""
    while True:
        time.sleep(1.0)
        current_ppid = os.getppid()
        if parent_pid != 1 and current_ppid != parent_pid:
            server.shutdown()
            break


def run_file_transfer_daemon(
    allocated_filenames: list[str],
    serve_directory: str,
    output_directory: str | None = None,
) -> None:
    """Run one file-transfer daemon process for a single active node."""
    output_path = Path(output_directory).resolve() if output_directory else None
    handler_class = _make_handler_class(
        allocated_filenames, Path(serve_directory).resolve(), output_path
    )
    server = HTTPServer(("0.0.0.0", 0), handler_class)
    port = server.server_address[1]

    parent_pid = os.getppid()
    watchdog = threading.Thread(
        target=_parent_watchdog, args=(parent_pid, server), daemon=True
    )
    watchdog.start()

    sys.stdout.write(f"{DAEMON_LISTENING_PREFIX}{port}\n")
    sys.stdout.flush()
    try:
        server.serve_forever()
    finally:
        server.server_close()


def start_file_transfer_daemon(
    allocated_filenames: list[str],
    serve_directory: str,
    output_directory: str | None = None,
) -> FileTransferDaemon:
    """Start a separate file-transfer daemon instance for one active node."""
    daemon = FileTransferDaemon(allocated_filenames, serve_directory, output_directory)
    daemon.start()
    return daemon


def parse_daemon_arguments() -> argparse.Namespace:
    """Parse CLI arguments for a standalone file-transfer daemon process."""
    parser = argparse.ArgumentParser(
        description="Run one per-node HTTP file-transfer daemon instance."
    )
    parser.add_argument("--serve-directory", required=True)
    parser.add_argument("--output-directory", default=None)
    parser.add_argument("--files", nargs="+", required=True)
    return parser.parse_args()


# ==============================================================================
# SECTION 8: WORKER EXECUTION & PROGRESS MONITORING
# ==============================================================================

def build_execute_command(
    execute_command_template: str,
    input_file: str,
    output_directory: str = WORKER_OUTPUT_DIRECTORY,
    output_file: str | None = None,
) -> str:
    """Build the execute_command string by substituting input/output placeholders."""
    input_path = Path(input_file)
    if output_file is None:
        output_file = input_path.name

    resolved_command = execute_command_template.replace("{input}", str(input_path))
    resolved_command = resolved_command.replace("{output_directory}", output_directory)
    resolved_command = resolved_command.replace("{output}", output_file)
    resolved_command = resolved_command.replace(
        "{input_directory}", str(input_path.parent)
    )
    return resolved_command


def format_elapsed_time(seconds: float) -> str:
    """Format elapsed seconds as MM:SS string."""
    minutes = int(seconds) // 60
    secs = int(seconds) % 60
    return f"{minutes:02d}:{secs:02d}"


def parse_ffmpeg_time_seconds(time_str: str) -> float:
    """Parse HH:MM:SS.ss timestamp string into total seconds."""
    parts = time_str.split(":")
    if len(parts) == 3:
        return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
    elif len(parts) == 2:
        return float(parts[0]) * 60 + float(parts[1])
    return float(parts[0])


def get_video_duration_ffprobe(filepath: str) -> float | None:
    """Try reading video duration in seconds using ffprobe."""
    try:
        res = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                filepath,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode == 0 and res.stdout.strip():
            return float(res.stdout.strip())
    except Exception:
        pass
    return None


def force_stop_process(process: subprocess.Popen) -> None:
    """Force stop a running execution process (SIGTERM then SIGKILL)."""
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=1.0)


def check_socket_connection_lost(connection: socket.socket) -> bool:
    """Check if the control socket connection to the master node has been lost/closed."""
    if _SOCKET_BUFFERS.get(connection, ""):
        return False
    try:
        rlist, _, _ = select.select([connection], [], [], 0.0)
        if rlist:
            data = connection.recv(1024, socket.MSG_PEEK)
            if not data:
                return True
    except (OSError, ConnectionError):
        return True
    return False


def _stderr_reader_thread(process: subprocess.Popen, state: dict, input_file: str) -> None:
    """Background thread to suppress raw ffmpeg stderr noise and parse progress/ETA."""
    total_dur = get_video_duration_ffprobe(input_file)
    if total_dur:
        state["total_duration"] = total_dur

    start_wall = time.time()
    if process.stderr is None:
        return

    for line in iter(process.stderr.readline, ""):
        if not line:
            break
        state["last_lines"].append(line.strip())
        if len(state["last_lines"]) > 10:
            state["last_lines"].pop(0)

        if "Duration:" in line and state.get("total_duration") is None:
            match = re.search(r"Duration:\s*(\d+:\d+:\d+\.\d+|\d+:\d+:\d+)", line)
            if match:
                state["total_duration"] = parse_ffmpeg_time_seconds(match.group(1))

        if "time=" in line:
            match = re.search(r"time=\s*(\d+:\d+:\d+\.\d+|\d+:\d+:\d+)", line)
            if match:
                curr_sec = parse_ffmpeg_time_seconds(match.group(1))
                state["current_seconds"] = curr_sec
                dur = state.get("total_duration")
                if dur and dur > 0:
                    pct = min(100.0, max(0.0, (curr_sec / dur) * 100.0))
                    state["pct"] = pct
                    elapsed_wall = time.time() - start_wall
                    if pct > 0:
                        est_total = elapsed_wall / (pct / 100.0)
                        eta_sec = max(0.0, est_total - elapsed_wall)
                        state["eta"] = format_elapsed_time(eta_sec)

    process.stderr.close()


class ProcessCpuTracker:
    """Track system CPU usage and subprocess CPU usage over time."""

    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.last_time = time.time()
        self.num_cpus = os.cpu_count() or 1
        self._last_sys_ticks = self._read_sys_ticks()
        self._last_proc_ticks = self._read_proc_ticks()

    def _read_sys_ticks(self) -> tuple[float, float] | None:
        """Return (total_jiffies, idle_jiffies) from /proc/stat if available."""
        try:
            with open("/proc/stat", "r") as f:
                line = f.readline()
            if line.startswith("cpu "):
                parts = [float(x) for x in line.split()[1:]]
                total = sum(parts)
                idle = parts[3] + (parts[4] if len(parts) > 4 else 0.0)
                return total, idle
        except Exception:
            pass
        return None

    def _read_proc_ticks(self) -> float | None:
        """Return total process jiffies from /proc/<pid>/stat if available."""
        try:
            with open(f"/proc/{self.pid}/stat", "r") as f:
                content = f.read()
            rparen_idx = content.rfind(")")
            if rparen_idx != -1:
                fields = content[rparen_idx + 1:].split()
                utime = float(fields[11])
                stime = float(fields[12])
                cutime = float(fields[13])
                cstime = float(fields[14])
                return utime + stime + cutime + cstime
        except Exception:
            pass
        return None

    def get_cpu_usage(self) -> tuple[float, float, float]:
        """Return (system_cpu_pct, process_cpu_pct, background_cpu_pct)."""
        now = time.time()
        dt = now - self.last_time
        self.last_time = now

        sys_ticks = self._read_sys_ticks()
        proc_ticks = self._read_proc_ticks()

        system_cpu_pct = 0.0
        process_cpu_pct = 0.0

        if sys_ticks is not None and self._last_sys_ticks is not None:
            tot_diff = sys_ticks[0] - self._last_sys_ticks[0]
            idle_diff = sys_ticks[1] - self._last_sys_ticks[1]
            if tot_diff > 0:
                system_cpu_pct = max(0.0, min(100.0, 100.0 * (1.0 - idle_diff / tot_diff)))

            if proc_ticks is not None and self._last_proc_ticks is not None:
                proc_diff = proc_ticks - self._last_proc_ticks
                if tot_diff > 0:
                    process_cpu_pct = max(0.0, min(100.0, 100.0 * (proc_diff / tot_diff)))
            self._last_sys_ticks = sys_ticks
            self._last_proc_ticks = proc_ticks
        else:
            try:
                import psutil
                system_cpu_pct = psutil.cpu_percent()
                p = psutil.Process(self.pid)
                process_cpu_pct = p.cpu_percent() / self.num_cpus
            except Exception:
                system_cpu_pct = 0.0
                process_cpu_pct = 0.0

        background_cpu_pct = max(0.0, system_cpu_pct - process_cpu_pct)
        return system_cpu_pct, process_cpu_pct, background_cpu_pct


def set_process_nice(pid: int, nice_value: int) -> bool:
    """Set nice priority score for a process PID."""
    try:
        # On macOS, os.setpriority() may fail silently when raising niceness.
        # Force fallback to psutil for macOS to ensure it maps correctly.
        import sys
        if sys.platform == "darwin":
            raise OSError("Force psutil fallback on macOS")
        os.setpriority(os.PRIO_PROCESS, pid, nice_value)
        return True
    except Exception:
        try:
            import psutil
            psutil.Process(pid).nice(nice_value)
            return True
        except Exception:
            return False


def run_execute_command(
    execute_command_template: str,
    input_file: str,
    output_directory: str = WORKER_OUTPUT_DIRECTORY,
    output_file: str | None = None,
    connection: socket.socket | None = None,
    simulate_failure_after: float | None = None,
    progress_callback: Callable[[float], None] | None = None,
    config: Config | None = None,
) -> float:
    """Run execute_command on worker, monitor CPU usage, support stepped nice throttling / process termination, and report progress."""
    Path(output_directory).mkdir(parents=True, exist_ok=True)
    resolved_command = build_execute_command(
        execute_command_template, input_file, output_directory, output_file
    )
    command_args = shlex.split(resolved_command)

    filename = Path(input_file).name
    print(f"{PROGRESS_EXECUTING} ({filename}) - starting: {resolved_command}")
    start_time = time.time()

    process = subprocess.Popen(
        command_args,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    state = {
        "pct": None,
        "eta": None,
        "current_seconds": 0.0,
        "total_duration": None,
        "last_lines": [],
    }

    reader_thread = threading.Thread(
        target=_stderr_reader_thread,
        args=(process, state, input_file),
        daemon=True,
    )
    reader_thread.start()

    tracker = ProcessCpuTracker(process.pid)
    high_usage_start: float | None = None
    low_usage_start: float | None = None
    is_throttled = False
    current_nice = 0
    throttled_start_time: float | None = None
    last_nice_check_time: float | None = None
    last_bg_usage = 0.0
    last_cpu_check_time = 0.0

    fd = None
    old_settings = None
    if sys.stdin.isatty():
        try:
            import termios
            import tty

            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            tty.setcbreak(fd)
        except Exception:
            fd = None
            old_settings = None

    try:
        while True:
            return_code = process.poll()
            now = time.time()
            elapsed_seconds = now - start_time
            formatted_time = format_elapsed_time(elapsed_seconds)

            # Resource monitoring check every second
            if config is not None and now - last_cpu_check_time >= 1.0:
                last_cpu_check_time = now
                sys_pct, proc_pct, bg_pct = tracker.get_cpu_usage()

                if bg_pct > config.cpu_threshold_percent:
                    low_usage_start = None
                    if high_usage_start is None:
                        high_usage_start = now
                    elif now - high_usage_start >= config.cpu_threshold_seconds:
                        if config.on_high_usage == "kill":
                            sys.stdout.write("\n")
                            print(
                                f"\n[RESOURCE ALERT] Background CPU usage ({bg_pct:.1f}%) exceeded threshold ({config.cpu_threshold_percent}%) for >{config.cpu_threshold_seconds}s! Killing task process..."
                            )
                            force_stop_process(process)
                            raise RuntimeError(
                                f"Task process killed: background CPU usage ({bg_pct:.1f}%) exceeded {config.cpu_threshold_percent}% for >{config.cpu_threshold_seconds}s"
                            )
                        elif config.on_high_usage == "throttle":
                            if not is_throttled:
                                is_throttled = True
                                current_nice = config.nice_initial
                                set_process_nice(process.pid, current_nice)
                                throttled_start_time = now
                                last_nice_check_time = now
                                last_bg_usage = bg_pct
                                sys.stdout.write("\n")
                                print(
                                    f"\n[RESOURCE ALERT] Background CPU usage ({bg_pct:.1f}%) high for >{config.cpu_threshold_seconds}s. Throttled PID {process.pid} (nice={current_nice})."
                                )
                            else:
                                if (
                                    last_nice_check_time is not None
                                    and now - last_nice_check_time
                                    >= config.nice_check_interval_seconds
                                ):
                                    last_nice_check_time = now
                                    if bg_pct > last_bg_usage:
                                        next_nice = min(
                                            config.nice_max,
                                            current_nice + config.nice_step,
                                        )
                                        if next_nice != current_nice:
                                            current_nice = next_nice
                                            set_process_nice(
                                                process.pid, current_nice
                                            )
                                            sys.stdout.write("\n")
                                            print(
                                                f"\n[RESOURCE ALERT] Background CPU increased ({last_bg_usage:.1f}% -> {bg_pct:.1f}%). Escalated nice to {current_nice}."
                                            )
                                    last_bg_usage = bg_pct

                                if (
                                    throttled_start_time is not None
                                    and now - throttled_start_time
                                    >= config.max_throttle_duration_seconds
                                ):
                                    if not config.long_runnable:
                                        sys.stdout.write("\n")
                                        print(
                                            f"\n[RESOURCE ALERT] Task throttled for >{config.max_throttle_duration_seconds}s and long_runnable is False! Killing task process..."
                                        )
                                        force_stop_process(process)
                                        raise RuntimeError(
                                            f"Task process killed: throttled for >{config.max_throttle_duration_seconds}s and long_runnable is False"
                                        )
                else:
                    high_usage_start = None
                    if is_throttled:
                        if low_usage_start is None:
                            low_usage_start = now
                        elif (
                            now - low_usage_start
                            >= config.unthrottle_threshold_seconds
                        ):
                            is_throttled = False
                            current_nice = 0
                            set_process_nice(process.pid, 0)
                            throttled_start_time = None
                            last_nice_check_time = None
                            low_usage_start = None
                            sys.stdout.write("\n")
                            print(
                                f"\n[RESOURCE RECOVERY] Background CPU usage ({bg_pct:.1f}%) returned to normal. Restored nice to 0."
                            )

            pct = state.get("pct")
            eta = state.get("eta")

            status_prefix = (
                f"{PROGRESS_EXECUTING} ({filename}) [throttled nice={current_nice}]"
                if is_throttled
                else f"{PROGRESS_EXECUTING} ({filename})"
            )

            if pct is not None and eta is not None:
                display_str = f"\r{status_prefix} - {pct:.1f}% [ETA: {eta}]   "
            elif pct is not None:
                display_str = f"\r{status_prefix} - {pct:.1f}%   "
            else:
                display_str = f"\r{status_prefix} - running time: {formatted_time}   "

            sys.stdout.write(display_str)
            sys.stdout.flush()

            if progress_callback:
                progress_callback(elapsed_seconds)

            if return_code is not None:
                sys.stdout.write("\n")
                if return_code != 0:
                    err_details = "\n".join(state["last_lines"])
                    raise RuntimeError(
                        f"execute_command failed with exit code {return_code}:\n{err_details}"
                    )
                print(
                    f"{PROGRESS_EXECUTING} complete for {filename} in {formatted_time}"
                )
                return elapsed_seconds

            if connection is not None:
                if has_buffered_message(connection):
                    msg = receive_json_message(connection)
                    if msg.get("type") == MESSAGE_TYPE_SHUTDOWN:
                        sys.stdout.write("\n")
                        print("\nMaster sent shutdown signal! Force-stopping running process...")
                        force_stop_process(process)
                        raise MasterShutdownError("Master sent shutdown signal during task execution")
                else:
                    try:
                        rlist, _, _ = select.select([connection], [], [], 0.0)
                        if rlist:
                            peek = connection.recv(1024, socket.MSG_PEEK)
                            if not peek:
                                sys.stdout.write("\n")
                                print("\nConnection to server lost! Force-stopping running process...")
                                force_stop_process(process)
                                raise ConnectionError("Server connection lost during task execution")
                            else:
                                msg = receive_json_message(connection)
                                if msg.get("type") == MESSAGE_TYPE_SHUTDOWN:
                                    sys.stdout.write("\n")
                                    print("\nMaster sent shutdown signal! Force-stopping running process...")
                                    force_stop_process(process)
                                    raise MasterShutdownError("Master sent shutdown signal during task execution")
                    except (OSError, ConnectionError):
                        sys.stdout.write("\n")
                        print("\nConnection to server lost! Force-stopping running process...")
                        force_stop_process(process)
                        raise ConnectionError("Server connection lost during task execution")

            if (
                simulate_failure_after is not None
                and elapsed_seconds >= simulate_failure_after
            ):
                sys.stdout.write("\n")
                print("\nSimulated failure triggered! Force-stopping process...")
                force_stop_process(process)
                raise KeyboardInterrupt(
                    f"Simulated failure triggered after {simulate_failure_after}s"
                )


            if fd is not None:
                rlist, _, _ = select.select([sys.stdin], [], [], 0.0)
                if rlist:
                    char = sys.stdin.read(1)
                    if char.lower() == "k":
                        sys.stdout.write("\n")
                        print("\n'k' key pressed! Force-killing task process and simulating node drop...")
                        force_stop_process(process)
                        raise KeyboardInterrupt("Task force-killed by user pressing 'k'")

            time.sleep(0.2)
    except Exception:
        force_stop_process(process)
        raise
    finally:
        if fd is not None and old_settings is not None:
            try:
                import termios

                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            except Exception:
                pass


# ==============================================================================
# SECTION 9: MASTER SPLIT VIDEO LOGIC
# ==============================================================================

def resolve_input_video(
    split_command: str = "",
    input_video: Optional[str] = None,
    parts_directory: str = MASTER_INPUT_DIRECTORY,
) -> str:
    """Resolve the actual input video path.
    If input_video exists on disk, returns it.
    Otherwise, attempts to infer it from split_command or by searching parts_directory.
    """
    if input_video and input_video != DEFAULT_INPUT_VIDEO and Path(input_video).is_file():
        return input_video

    if Path(DEFAULT_INPUT_VIDEO).is_file():
        return DEFAULT_INPUT_VIDEO

    # Extract input file path from split_command (-i <path>)
    if split_command:
        match = re.search(r'-i\s+([^\s]+)', split_command)
        if match:
            candidate = match.group(1).replace("{input_directory}", parts_directory)
            if Path(candidate).is_file():
                return candidate

    parts_path = Path(parts_directory)
    if parts_path.is_dir():
        for candidate in sorted(parts_path.glob("input.*")):
            if candidate.is_file():
                return str(candidate)
        for candidate in sorted(parts_path.iterdir()):
            if candidate.is_file() and not candidate.name.startswith("part") and candidate.name != "filelist.txt":
                return str(candidate)

    return input_video or DEFAULT_INPUT_VIDEO


def get_part_extension(
    split_command: str = "",
    parts_directory: str = MASTER_INPUT_DIRECTORY,
) -> str:
    """Infer part file extension (.webm, .mkv, .mp4) from split_command or existing part files."""
    if split_command:
        match = re.search(r'part(?:%d|\d+)?(\.[a-zA-Z0-9]+)', split_command)
        if match:
            return match.group(1)

    parts_path = Path(parts_directory)
    if parts_path.is_dir():
        for item in parts_path.iterdir():
            if item.is_file() and item.name.startswith("part"):
                return item.suffix

    return PART_FILENAME_SUFFIX


def part_filename_for_node(
    node_index: int,
    extension: Optional[str] = None,
    split_command: str = "",
    parts_directory: str = MASTER_INPUT_DIRECTORY,
) -> str:
    """Return the split output name for a node (part1.mkv, part2.webm, ...)."""
    if extension is None:
        extension = get_part_extension(split_command, parts_directory)
    return f"{PART_FILENAME_PREFIX}{node_index}{extension}"


def get_video_duration_seconds(input_video: str) -> float:
    """Read the input video duration in seconds using ffprobe."""
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            input_video,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"ffprobe failed for {input_video}: {result.stderr.strip()}"
        )
    return float(result.stdout.strip())


def build_split_command(
    split_command: str,
    active_node_count: int,
    input_video: Optional[str] = None,
    parts_directory: str = MASTER_INPUT_DIRECTORY,
) -> str:
    """Fill split_command placeholders so ffmpeg produces exactly one part per active node."""
    actual_input = resolve_input_video(split_command, input_video, parts_directory)
    if "{segment_times}" in split_command:
        duration_seconds = get_video_duration_seconds(actual_input)
        split_points = [
            duration_seconds * node_index / active_node_count
            for node_index in range(1, active_node_count)
        ]
        segment_times = ",".join(f"{split_point:.3f}" for split_point in split_points)
        resolved_command = split_command.replace("{segment_times}", segment_times)
    else:
        resolved_command = split_command

    resolved_command = resolved_command.replace("{input_directory}", parts_directory)
    return resolved_command


def run_split_command(
    split_command: str,
    active_node_count: int,
    input_video: Optional[str] = None,
    parts_directory: str = MASTER_INPUT_DIRECTORY,
) -> None:
    """On start, master runs split_command from config against the input video,
    writing exactly active_node_count part files into parts_directory (master/input/)."""
    print(PROGRESS_SPLITTING_FILE)
    Path(parts_directory).mkdir(parents=True, exist_ok=True)
    actual_input = resolve_input_video(split_command, input_video, parts_directory)

    if active_node_count == 1:
        ext = get_part_extension(split_command, parts_directory)
        destination = Path(parts_directory) / part_filename_for_node(1, extension=ext)
        shutil.copyfile(actual_input, destination)
        return

    resolved_command = build_split_command(
        split_command, active_node_count, actual_input, parts_directory
    )
    command_parts = shlex.split(resolved_command)
    result = subprocess.run(
        command_parts,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"split_command failed with exit code {result.returncode}:\n{result.stderr.strip()}"
        )


def verify_part_files(
    active_node_count: int,
    parts_directory: str = MASTER_INPUT_DIRECTORY,
    split_command: str = "",
) -> list[str]:
    """Verify part1 through partN exist for each active node."""
    parts_path = Path(parts_directory)
    ext = get_part_extension(split_command, parts_directory)
    part_filenames = [
        part_filename_for_node(node_index, extension=ext)
        for node_index in range(1, active_node_count + 1)
    ]
    missing_parts = [
        filename
        for filename in part_filenames
        if not (parts_path / filename).is_file()
    ]
    if missing_parts:
        raise FileNotFoundError(
            "Split output missing required part files: "
            + ", ".join(missing_parts)
        )
    return [str(parts_path / filename) for filename in part_filenames]


# ==============================================================================
# SECTION 10: MASTER MERGE VIDEO LOGIC
# ==============================================================================

def generate_filelist_text(
    active_node_count: int,
    output_directory: str = MASTER_OUTPUT_DIRECTORY,
    split_command: str = "",
) -> str:
    """Generate the contents of filelist.txt for ffmpeg concat demuxer using absolute paths."""
    dir_path = Path(output_directory).resolve()
    lines = [
        f"file '{dir_path / part_filename_for_node(index, split_command=split_command, parts_directory=output_directory)}'"
        for index in range(1, active_node_count + 1)
    ]
    return "\n".join(lines) + "\n"


def write_filelist_txt(
    active_node_count: int,
    output_directory: str = MASTER_OUTPUT_DIRECTORY,
    split_command: str = "",
) -> str:
    """Write filelist.txt into output_directory AND root working directory for universal compatibility."""
    dir_path = Path(output_directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    content = generate_filelist_text(active_node_count, output_directory, split_command=split_command)

    filelist_in_output = dir_path / "filelist.txt"
    filelist_in_output.write_text(content)

    filelist_in_root = Path("filelist.txt")
    filelist_in_root.write_text(content)

    return str(filelist_in_output)


def build_merge_command(
    merge_command_template: str,
    output_directory: str = MASTER_OUTPUT_DIRECTORY,
) -> str:
    """Substitute placeholders in merge_command template."""
    resolved = merge_command_template.replace("{output_directory}", output_directory)
    resolved = resolved.replace("{filelist}", f"{output_directory}/filelist.txt")
    return resolved


def resolve_output_file(
    merge_command_template: str,
    output_directory: str = MASTER_OUTPUT_DIRECTORY,
) -> Path:
    """Resolve the merged output file path by checking the merge command or output directory."""
    resolved_command = build_merge_command(merge_command_template, output_directory)
    out_dir_path = Path(output_directory).resolve()

    try:
        command_parts = shlex.split(resolved_command)
        for part in reversed(command_parts):
            try:
                p = Path(part).resolve()
                if (out_dir_path in p.parents or p.parent == out_dir_path) and p.name != "filelist.txt" and not p.name.startswith("part"):
                    if p.is_file():
                        return p
            except Exception:
                pass
    except Exception:
        pass

    out_dir = Path(output_directory)
    if out_dir.is_dir():
        for candidate in out_dir.glob("output.*"):
            if candidate.is_file():
                return candidate
        for candidate in out_dir.iterdir():
            if candidate.is_file() and candidate.name != "filelist.txt" and not candidate.name.startswith("part"):
                return candidate

    return Path(output_directory) / "output.mkv"


def run_merge_command(
    merge_command_template: str,
    active_node_count: int,
    output_directory: str = MASTER_OUTPUT_DIRECTORY,
    split_command: str = "",
) -> str:
    """Once all worker nodes finish, combine output parts into one final file."""
    print(PROGRESS_MERGING_FILES)
    write_filelist_txt(active_node_count, output_directory, split_command=split_command)
    resolved_command = build_merge_command(merge_command_template, output_directory)
    command_parts = shlex.split(resolved_command)

    result = subprocess.run(
        command_parts,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"merge_command failed with exit code {result.returncode}:\n{result.stderr.strip()}"
        )

    final_output_path = resolve_output_file(merge_command_template, output_directory)
    if not final_output_path.is_file():
        raise FileNotFoundError(
            f"Merged output file not found at expected location: {final_output_path}"
        )

    print(f"{PROGRESS_FINISHED} (master: {final_output_path.name})")
    return str(final_output_path)


def cleanup_master_temporary_files(
    parts_directory: str = MASTER_INPUT_DIRECTORY,
    output_directory: str = MASTER_OUTPUT_DIRECTORY,
    input_video: Optional[str] = None,
) -> None:
    """Clean up temporary split and intermediate part files on master node."""
    input_path = Path(input_video).resolve() if input_video else None

    # Clean temporary split part files in parts_directory
    parts_dir = Path(parts_directory)
    if parts_dir.is_dir():
        for file_path in parts_dir.iterdir():
            if file_path.is_file():
                try:
                    resolved = file_path.resolve()
                    if input_path and resolved == input_path:
                        continue
                    if file_path.name.startswith(PART_FILENAME_PREFIX) or file_path.name.startswith("part"):
                        file_path.unlink()
                        printlog(f"Cleaned up temporary master part file: {file_path}")
                except Exception as exc:
                    printlog(f"Failed to delete {file_path}: {exc}")

    # Clean intermediate received part files in output_directory
    out_dir = Path(output_directory)
    if out_dir.is_dir():
        for file_path in out_dir.iterdir():
            if file_path.is_file():
                try:
                    if file_path.name.startswith(PART_FILENAME_PREFIX) or file_path.name.startswith("part"):
                        file_path.unlink()
                        printlog(f"Cleaned up temporary master output part file: {file_path}")
                except Exception as exc:
                    printlog(f"Failed to delete {file_path}: {exc}")

    # Clean temporary filelist.txt files if present
    for fl in (Path("filelist.txt"), Path(output_directory) / "filelist.txt"):
        if fl.is_file():
            try:
                fl.unlink()
            except Exception:
                pass


def cleanup_worker_temporary_files(
    download_directory: str = WORKER_INPUT_DIRECTORY,
    output_directory: str = WORKER_OUTPUT_DIRECTORY,
) -> None:
    """Clean up downloaded and processed part files on worker node."""
    for dir_path_str in (download_directory, output_directory):
        dir_path = Path(dir_path_str)
        if dir_path.is_dir():
            for file_path in dir_path.iterdir():
                if file_path.is_file():
                    try:
                        file_path.unlink()
                        printlog(f"Cleaned up temporary worker file: {file_path}")
                    except Exception as exc:
                        printlog(f"Failed to delete {file_path}: {exc}")


# ==============================================================================
# SECTION 11: MASTER CLI STATUS DASHBOARD
# ==============================================================================

class StatusDashboard:
    """Live CLI status view tracking master and per-node states as specified in idea.txt."""

    def __init__(self, master_state: str = "initializing") -> None:
        self.lock = threading.Lock()
        self.master_state = master_state
        self.splitting_flag = False
        self.merging_flag = False
        self.node_states: Dict[str, Dict[str, Any]] = {}
        self.messages: list[dict] = []
        self._last_render_time = time.time()
        self.available_configs: list[str] = []
        self.selected_config_index: int = 0

    def set_available_configs(self, configs: list[str], selected: str | None = None) -> None:
        """Set available config paths and initial selection."""
        with self.lock:
            self.available_configs = list(configs)
            if selected and selected in self.available_configs:
                self.selected_config_index = self.available_configs.index(selected)
            else:
                self.selected_config_index = 0
            self._render_unlocked()

    def select_next_config(self) -> str:
        """Cycle to the next available config file."""
        with self.lock:
            if self.available_configs:
                self.selected_config_index = (self.selected_config_index + 1) % len(self.available_configs)
            self._render_unlocked()
            if self.available_configs and 0 <= self.selected_config_index < len(self.available_configs):
                return self.available_configs[self.selected_config_index]
            return ""

    def select_config_by_index(self, index: int) -> str:
        """Select a config file by its 0-based index."""
        with self.lock:
            if self.available_configs and 0 <= index < len(self.available_configs):
                self.selected_config_index = index
            self._render_unlocked()
            if self.available_configs and 0 <= self.selected_config_index < len(self.available_configs):
                return self.available_configs[self.selected_config_index]
            return ""

    def get_selected_config(self) -> str:
        """Return the currently selected config file path."""
        with self.lock:
            if self.available_configs and 0 <= self.selected_config_index < len(self.available_configs):
                return self.available_configs[self.selected_config_index]
            return "config.ini"
            
    def reset_nodes(self) -> None:
        """Clear the current node states."""
        with self.lock:
            self.node_states.clear()
            self._render_unlocked()

    def add_message(self, text: str, timeout_seconds: int = 10) -> None:
        """Add an event/alert message with a countdown period to the dashboard (idea.txt)."""
        with self.lock:
            self.messages.append({"text": text, "time_left": float(timeout_seconds)})
            self._render_unlocked()

    def set_master_state(self, state: str) -> None:
        """Update overall master state (e.g. splitting file, merging files, finished)."""
        with self.lock:
            self.master_state = state
            if state == PROGRESS_SPLITTING_FILE:
                self.splitting_flag = True
            elif state == PROGRESS_MERGING_FILES:
                self.merging_flag = True
            self._render_unlocked()

    def update_node_flags(
        self,
        node_ip: str,
        receiving: bool = False,
        executing: bool = False,
        sending: bool = False,
    ) -> None:
        """Update 3-phase node state flags (idea.txt)."""
        with self.lock:
            if node_ip in self.node_states:
                self.node_states[node_ip]["receiving"] = receiving
                self.node_states[node_ip]["executing"] = executing
                self.node_states[node_ip]["sending"] = sending
                self._render_unlocked()

    def update_node(
        self,
        node_ip: str,
        node_id: str,
        filename: str,
        state: str,
        elapsed_seconds: float | None = None,
        pct: float | None = None,
        eta: str | None = None,
        receiving: bool | None = None,
        executing: bool | None = None,
        sending: bool | None = None,
        connected: bool | None = None,
        hostname: str | None = None,
        username: str | None = None,
    ) -> None:
        """Update individual worker node state and flags."""
        with self.lock:
            existing = self.node_states.get(node_ip, {})
            rcv = receiving if receiving is not None else existing.get("receiving", False)
            exc = executing if executing is not None else existing.get("executing", False)
            snd = sending if sending is not None else existing.get("sending", False)
            conn = connected if connected is not None else existing.get("connected", True)

            self.node_states[node_ip] = {
                "node_id": node_id,
                "filename": filename,
                "state": state,
                "elapsed": elapsed_seconds,
                "pct": pct,
                "eta": eta,
                "receiving": rcv,
                "executing": exc,
                "sending": snd,
                "connected": conn,
                "hostname": hostname if hostname is not None else existing.get("hostname"),
                "username": username if username is not None else existing.get("username"),
            }
            self._render_unlocked()

    def set_node_connected(
        self, node_ip: str, connected: bool, state: str | None = None
    ) -> None:
        """Update connection status of a node."""
        with self.lock:
            if node_ip in self.node_states:
                self.node_states[node_ip]["connected"] = connected
                if not connected:
                    self.node_states[node_ip]["state"] = state or "disconnected"
                    self.node_states[node_ip]["receiving"] = False
                    self.node_states[node_ip]["executing"] = False
                    self.node_states[node_ip]["sending"] = False
                elif state:
                    self.node_states[node_ip]["state"] = state
                self._render_unlocked()

    def remove_node(self, node_ip: str) -> None:
        """Remove a node from the active dashboard view."""
        with self.lock:
            self.node_states.pop(node_ip, None)
            self._render_unlocked()

    def render(self) -> None:
        with self.lock:
            self._render_unlocked()

    def _render_unlocked(self) -> None:
        now = time.time()
        dt = now - self._last_render_time
        self._last_render_time = now

        # Update message countdowns and clear expired messages (idea.txt)
        updated_messages = []
        for msg in self.messages:
            msg["time_left"] -= dt
            if msg["time_left"] > 0:
                updated_messages.append(msg)
        self.messages = updated_messages

        # Clear screen using ANSI escape codes (avoids resetting terminal cbreak mode)
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()
        """Render formatted CLI dashboard view."""
        header = f"=== MASTER DASHBOARD: [{self.master_state.upper()}] ==="
        divider = "=" * (len(header) + 12)

        lines = ["", divider, header, divider]

        if self.available_configs:
            lines.append("CONFIG FILE SELECTION:")
            for idx, cfg in enumerate(self.available_configs):
                marker = "► [SELECTED]" if idx == self.selected_config_index else " "
                lines.append(f"  [{idx + 1}] {cfg:<30} {marker}")
            lines.append("-" * (len(header) + 12))

        if self.master_state != "config selection":
            lines.append(
                f"{'NODE IP (USER@HOST)':<32} {'DEVICE':<10} {'CONNECTED':<11} {'PART FILE':<12} {'STATE':<20} {'FLAGS [R/E/S]':<14} {'PROGRESS / RUN TIME'}"
            )
            lines.append("-" * (len(header) + 12 + 16))

            if not self.node_states:
                lines.append("  (Waiting for worker connections...)")
            else:
                for ip, info in self.node_states.items():
                    state_str = info["state"]
                    elapsed = info["elapsed"]
                    pct = info["pct"]
                    eta = info["eta"]
                    conn_str = "YES" if info.get("connected", True) else "NO"
                    rcv = "R" if info.get("receiving") else "-"
                    exc = "E" if info.get("executing") else "-"
                    snd = "S" if info.get("sending") else "-"
                    flags_str = f"[{rcv}/{exc}/{snd}]"

                    prog_parts = []
                    if elapsed is not None:
                        m, s = int(elapsed) // 60, int(elapsed) % 60
                        prog_parts.append(f"{m:02d}:{s:02d}")
                    if pct is not None:
                        prog_parts.append(f"{pct:.1f}%")
                    if eta is not None:
                        prog_parts.append(f"[ETA: {eta}]")

                    prog_str = " ".join(prog_parts)
                    
                    display_ip = ip
                    if info.get("hostname") and info.get("username"):
                        display_ip = f"{ip}({info['username']}@{info['hostname']})"
                        
                    lines.append(
                        f"{display_ip:<32} {info['node_id']:<10} {conn_str:<11} {info['filename']:<12} {state_str:<20} {flags_str:<14} {prog_str}"
                    )


        if self.messages:
            lines.append(divider)
            lines.append("MESSAGES / ALERTS:")
            for m in self.messages:
                lines.append(f"  • {m['text']} ({int(m['time_left'])}s left)")

        lines.append(divider)
        printlog("\n".join(lines) + "\n")
        print("\n".join(lines))



# ==============================================================================
# SECTION X: HARDWARE BENCHMARKING & TELEMETRY
# ==============================================================================

def benchmark_single_core(duration_seconds: float = 0.5) -> float:
    import hashlib
    data = b"benchmark_payload_block_" * 43
    count = 0
    start = time.time()
    while time.time() - start < duration_seconds:
        hashlib.sha256(data).digest()
        count += 1
    elapsed = time.time() - start
    return count / elapsed if elapsed > 0 else 0.0

def benchmark_multi_core(duration_seconds: float = 1.0) -> float:
    num_cores = os.cpu_count() or 1
    per_core_duration = max(0.2, duration_seconds / 2)
    try:
        from multiprocessing import Pool
        with Pool(num_cores) as pool:
            scores = pool.starmap(benchmark_single_core, [(per_core_duration,)] * num_cores)
        return sum(scores)
    except Exception:
        single = benchmark_single_core(duration_seconds)
        return single * num_cores

def detect_gpu_info() -> dict:
    info = {"gpu_name": "", "vram_mb": 0, "hw_encoders": []}
    if platform.system() == "Linux":
        try:
            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                line = result.stdout.strip().split("\n")[0]
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 2:
                    info["gpu_name"] = parts[0]
                    info["vram_mb"] = int(float(parts[1]))
                    info["hw_encoders"].extend(["h264_nvenc", "hevc_nvenc"])
        except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
            pass
    if platform.system() == "Darwin":
        try:
            result = subprocess.run(
                ["system_profiler", "SPDisplaysDataType", "-json"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                data = json.loads(result.stdout)
                displays = data.get("SPDisplaysDataType", [])
                if displays:
                    gpu = displays[0]
                    info["gpu_name"] = gpu.get("sppci_model", "Apple Silicon GPU")
                    info["hw_encoders"].extend(["h264_videotoolbox", "hevc_videotoolbox"])
        except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError):
            pass
    try:
        result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            known_hw_encoders = [
                "h264_nvenc", "hevc_nvenc",
                "h264_videotoolbox", "hevc_videotoolbox",
                "h264_qsv", "hevc_qsv",
                "h264_vaapi", "hevc_vaapi",
            ]
            for enc in known_hw_encoders:
                if enc in result.stdout and enc not in info["hw_encoders"]:
                    info["hw_encoders"].append(enc)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return info

def detect_power_state() -> dict:
    info = {"is_plugged_in": True, "battery_pct": 100, "has_battery": False}
    try:
        import psutil
        battery = psutil.sensors_battery()
        if battery is not None:
            info["has_battery"] = True
            info["is_plugged_in"] = bool(battery.power_plugged)
            info["battery_pct"] = int(battery.percent)
    except Exception:
        pass
    return info

def _get_total_ram_mb() -> int:
    try:
        import psutil
        return psutil.virtual_memory().total // (1024 * 1024)
    except Exception:
        try:
            with open("/proc/meminfo", "r") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        return int(line.split()[1]) // 1024
        except Exception:
            pass
    return 0

def run_node_benchmark() -> dict:
    import getpass
    power = detect_power_state()
    gpu = detect_gpu_info()
    
    try:
        username = getpass.getuser()
    except Exception:
        username = "unknown"
        
    result = {
        "single_core_score": round(benchmark_single_core(0.5), 1),
        "multi_core_score": round(benchmark_multi_core(1.0), 1),
        "gpu_name": gpu["gpu_name"],
        "vram_mb": gpu["vram_mb"],
        "hw_encoders": gpu["hw_encoders"],
        "os_type": platform.system().lower(),
        "cpu_count": os.cpu_count() or 1,
        "total_ram_mb": _get_total_ram_mb(),
        "is_plugged_in": power.get("is_plugged_in", True),
        "battery_pct": power.get("battery_pct", 100),
        "has_battery": power.get("has_battery", False),
        "hostname": socket.gethostname(),
        "username": username,
    }
    
    print(f"  Single-core: {result['single_core_score']:.0f} ops/s")
    print(f"  Multi-core:  {result['multi_core_score']:.0f} ops/s ({result['cpu_count']} cores)")
    if result["gpu_name"]:
        print(f"  GPU: {result['gpu_name']} ({result['vram_mb']} MB VRAM)")
    print(f"  RAM: {result['total_ram_mb']} MB | Power: {'AC' if result['is_plugged_in'] else 'Battery'} ({result['battery_pct']}%)")
    
    return result


UDP_DISCOVERY_PORT = 5005
DISCOVERY_BEACON_TYPE = "DISCOVERY_BEACON"
MASTER_OFFER_TYPE = "MASTER_OFFER"

def start_udp_discovery_listener(master_ip: str, control_port: int) -> threading.Thread:
    """Listen for UDP broadcast beacons and reply with master info."""
    def listener():
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except Exception:
                pass
        sock.bind(("", UDP_DISCOVERY_PORT))
        while True:
            try:
                data, addr = sock.recvfrom(1024)
                if data.decode("utf-8").strip() == DISCOVERY_BEACON_TYPE:
                    reply = f"{MASTER_OFFER_TYPE}:{master_ip}:{control_port}"
                    sock.sendto(reply.encode("utf-8"), addr)
            except Exception:
                pass
    
    t = threading.Thread(target=listener, daemon=True)
    t.start()
    return t

def discover_master_udp(timeout_seconds: float = 30.0) -> tuple[str, int]:
    """Broadcast UDP beacons and wait for master offer."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(2.0)
    
    end_time = time.time() + timeout_seconds
    while time.time() < end_time:
        try:
            sock.sendto(DISCOVERY_BEACON_TYPE.encode("utf-8"), ("255.255.255.255", UDP_DISCOVERY_PORT))
            data, addr = sock.recvfrom(1024)
            msg = data.decode("utf-8").strip()
            if msg.startswith(MASTER_OFFER_TYPE):
                parts = msg.split(":")
                if len(parts) >= 3:
                    return parts[1], int(parts[2])
        except (socket.timeout, OSError):
            pass
        time.sleep(1.0)
    raise TimeoutError("UDP discovery timed out")

def rank_workers_by_affinity(telemetry_map: dict[str, dict], resource_type: str, min_vram_mb: int = 0) -> list[str]:
    valid = []
    for ip, tel in telemetry_map.items():
        if tel.get("has_battery") and tel.get("battery_pct", 100) < 20 and not tel.get("is_plugged_in"):
            continue
        if resource_type == "gpu" and tel.get("vram_mb", 0) < min_vram_mb:
            continue
        valid.append((ip, tel))
        
    if resource_type == "single_core":
        valid.sort(key=lambda x: x[1].get("single_core_score", 0), reverse=True)
    elif resource_type == "multi_core":
        valid.sort(key=lambda x: x[1].get("multi_core_score", 0), reverse=True)
    elif resource_type == "gpu":
        valid.sort(key=lambda x: x[1].get("vram_mb", 0), reverse=True)
    
    return [x[0] for x in valid]

# ==============================================================================
# SUBPROCESS ENTRY POINT FOR STANDALONE FILE-TRANSFER DAEMON
# ==============================================================================

if __name__ == "__main__":
    arguments = parse_daemon_arguments()
    run_file_transfer_daemon(
        arguments.files,
        arguments.serve_directory,
        arguments.output_directory,
    )
