"""Constants and schemas for the distributed compute system."""

from __future__ import annotations

# Networking Ports
CONTROL_PORT = 5000
FIXED_PORT = CONTROL_PORT
MDNS_PORT = 5353
UDP_DISCOVERY_PORT = 5005

# Networking Addresses & Identifiers
MDNS_ADDR = "224.0.0.251"
MDNS_SERVICE_MASTER = "_distcompute._tcp.local"
MDNS_MASTER_FQDN = "distcompute-master.local"
MDNS_SERVICE_WORKER = "_distworker._tcp.local"
MDNS_WORKER_FQDN = "distcompute-worker.local"
MDNS_KEYWORD = "distcompute"
MDNS_KEYWORD_MASTER = "distcompute-master"
MDNS_KEYWORD_WORKER = "distcompute-worker"

DISCOVERY_BEACON_TYPE = "DISCOVERY_BEACON"
MASTER_OFFER_TYPE = "MASTER_OFFER"

# Shared Secret & Security
SHARED_SECRET_ENV_VAR = "DIST_SHARED_SECRET"

# Devices & Identification
DEVICES_JSON_FILE = "devices.json"
DEVICE_ID_PREFIX = "node"

# Configuration Keys
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

# Progress Status Labels
PROGRESS_SPLITTING_FILE = "splitting file"
PROGRESS_SENDING_FILE = "sending file"
PROGRESS_EXECUTING = "executing"
PROGRESS_RECEIVING_FILES = "receiving files"
PROGRESS_FINISHED = "finished"
PROGRESS_MERGING_FILES = "merging files"

# Message Types
MESSAGE_TYPE_READY = "ready"
MESSAGE_TYPE_FILE_RECEIVED = "file_received"
MESSAGE_TYPE_FINISHED = "finished"
MESSAGE_TYPE_FAILED = "failed"
MESSAGE_TYPE_SHUTDOWN = "shutdown"
MESSAGE_TYPE_AUTH = "auth"
MESSAGE_TYPE_BINARY_INFO_REQUEST = "binary_info_request"
MESSAGE_TYPE_BINARY_INFO = "binary_info"
MESSAGE_TYPE_BINARY_READY = "binary_ready"
MESSAGE_TYPE_WORKER_TELEMETRY = "worker_telemetry"
MESSAGE_TYPE_PIN_CHALLENGE = "pin_challenge"
MESSAGE_TYPE_PIN_RESPONSE = "pin_response"
MESSAGE_TYPE_PIN_ACCEPTED = "pin_accepted"
MESSAGE_TYPE_PIN_REJECTED = "pin_rejected"

# Message schema validation: required keys for each message type
MESSAGE_REQUIRED_KEYS = {
    MESSAGE_TYPE_READY: ["type", "master_ip_address", "file_transfer_port"],
    MESSAGE_TYPE_FILE_RECEIVED: ["type"],
    MESSAGE_TYPE_FINISHED: ["type", "execution_time", "part_filename"],
    MESSAGE_TYPE_FAILED: ["type", "reason"],
    MESSAGE_TYPE_SHUTDOWN: ["type"],
    MESSAGE_TYPE_AUTH: ["type", "secret"],
    MESSAGE_TYPE_BINARY_INFO_REQUEST: ["type", "binary_name"],
    MESSAGE_TYPE_BINARY_INFO: [
        "type",
        "os_name",
        "arch",
        "binary_name",
        "has_binary",
        "md5",
    ],
    MESSAGE_TYPE_BINARY_READY: ["type"],
    MESSAGE_TYPE_WORKER_TELEMETRY: [
        "type",
        "single_core_score",
        "multi_core_score",
        "os_type",
        "cpu_count",
        "gpu_score",
        "ram_mb",
    ],
    MESSAGE_TYPE_PIN_CHALLENGE: ["type", "master_name"],
    MESSAGE_TYPE_PIN_RESPONSE: ["type", "pin"],
    MESSAGE_TYPE_PIN_ACCEPTED: ["type"],
    MESSAGE_TYPE_PIN_REJECTED: ["type", "reason", "attempts_left"],
}

# Directories & Files
PART_FILENAME_PREFIX = "part"
PART_FILENAME_SUFFIX = ".mkv"

MASTER_DIRECTORY = "master"
WORKER_DIRECTORY = "worker"

MASTER_INPUT_DIRECTORY = f"{MASTER_DIRECTORY}/input"
MASTER_OUTPUT_DIRECTORY = f"{MASTER_DIRECTORY}/output"
WORKER_INPUT_DIRECTORY = f"{WORKER_DIRECTORY}/input"
WORKER_OUTPUT_DIRECTORY = f"{WORKER_DIRECTORY}/output"
WORKER_BINARY_DIRECTORY = f"{WORKER_DIRECTORY}/binaries"

DEFAULT_INPUT_VIDEO = f"{MASTER_INPUT_DIRECTORY}/input.mkv"

# File Transfer Daemon Constants
DAEMON_READY_TIMEOUT_SECONDS = 10.0
DAEMON_LISTENING_PREFIX = "LISTENING "
