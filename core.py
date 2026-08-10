"""Core shared library module for distributed video processing.
Consolidates constants, config loading, device mapping, networking, control messaging,
HTTP file transfers, task execution monitoring, video splitting/merging, and UI dashboard.
"""

import argparse
import configparser
import json
import os
import re
import select
import shlex
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable, Dict
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

PART_FILENAME_PREFIX = "part"
PART_FILENAME_SUFFIX = ".mkv"

# Role-specific working directories under the project root.
MASTER_DIRECTORY = "master"
WORKER_DIRECTORY = "worker"

MASTER_INPUT_DIRECTORY = f"{MASTER_DIRECTORY}/input"
MASTER_OUTPUT_DIRECTORY = f"{MASTER_DIRECTORY}/output"
WORKER_INPUT_DIRECTORY = f"{WORKER_DIRECTORY}/input"
WORKER_OUTPUT_DIRECTORY = f"{WORKER_DIRECTORY}/output"

DEFAULT_INPUT_VIDEO = f"{MASTER_INPUT_DIRECTORY}/input.mkv"


# ==============================================================================
# SECTION 2: CONFIGURATION LOADER
# ==============================================================================

@dataclass(frozen=True)
class Config:
    split_command: str
    execute_command: str
    merge_command: str
    max_nodes: int


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

    return Config(
        split_command=section[CONFIG_KEY_SPLIT_COMMAND].strip(),
        execute_command=section[CONFIG_KEY_EXECUTE_COMMAND].strip(),
        merge_command=section[CONFIG_KEY_MERGE_COMMAND].strip(),
        max_nodes=max_nodes,
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
        raise KeyError(f"IP address {old_ip} not found in {devices_json_path}")

    device_id = mapping.pop(old_ip)
    mapping[new_ip] = device_id
    path.write_text(json.dumps(mapping, indent=2) + "\n")
    return mapping


# ==============================================================================
# SECTION 4: NETWORK HELPERS
# ==============================================================================

def get_local_ip_for_peer(peer_ip_address: str) -> str:
    """Return the local IP address used to reach a peer on the LAN."""
    connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        connection.connect((peer_ip_address, 9))
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


def receive_json_message(connection: socket.socket) -> dict:
    """Receive one JSON control message from the connection, buffering extra lines."""
    buf = _SOCKET_BUFFERS.get(connection, "")
    while "\n" not in buf:
        chunk = connection.recv(4096)
        if not chunk:
            _SOCKET_BUFFERS.pop(connection, None)
            raise ConnectionError("Control connection closed before message was received")
        buf += chunk.decode("utf-8")

    line, remainder = buf.split("\n", 1)
    _SOCKET_BUFFERS[connection] = remainder
    return json.loads(line)


def has_buffered_message(connection: socket.socket) -> bool:
    """Return True if an unread newline-terminated message is buffered for this socket."""
    return "\n" in _SOCKET_BUFFERS.get(connection, "")


def send_shutdown_message(connection: socket.socket) -> None:
    """Master sends a shutdown message to worker nodes to signal clean termination."""
    send_json_message(connection, {"type": MESSAGE_TYPE_SHUTDOWN})


def send_ready_message(
    connection: socket.socket, master_ip_address: str, file_transfer_port: int
) -> None:
    """Master sends a ready message with this node's file-transfer daemon port."""
    send_json_message(
        connection,
        {
            "type": MESSAGE_TYPE_READY,
            "master_ip_address": master_ip_address,
            "file_transfer_port": file_transfer_port,
        },
    )


def receive_ready_message(connection: socket.socket) -> dict:
    """Worker receives the ready message from the master."""
    message = receive_json_message(connection)
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

        self.port = read_daemon_port(self._process)
        wait_for_daemon_port(self._process, self.port)

    def stop(self) -> None:
        """Stop this node's file-transfer daemon process."""
        if self._process is not None and self._process.poll() is None:
            self._process.terminate()
            self._process.wait(timeout=5)


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
    sys.stdout.write(f"{DAEMON_LISTENING_PREFIX}{port}\n")
    sys.stdout.flush()
    server.serve_forever()


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


def run_execute_command(
    execute_command_template: str,
    input_file: str,
    output_directory: str = WORKER_OUTPUT_DIRECTORY,
    output_file: str | None = None,
    connection: socket.socket | None = None,
    simulate_failure_after: float | None = None,
    progress_callback: Callable[[float], None] | None = None,
) -> float:
    """Run execute_command on worker, show clean percentage & ETA progress, support 'k' kill and connection loss force-stop."""
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
            elapsed_seconds = time.time() - start_time
            formatted_time = format_elapsed_time(elapsed_seconds)

            pct = state.get("pct")
            eta = state.get("eta")

            if pct is not None and eta is not None:
                display_str = f"\r{PROGRESS_EXECUTING} ({filename}) - {pct:.1f}% [ETA: {eta}]   "
            elif pct is not None:
                display_str = f"\r{PROGRESS_EXECUTING} ({filename}) - {pct:.1f}%   "
            else:
                display_str = f"\r{PROGRESS_EXECUTING} ({filename}) - running time: {formatted_time}   "

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

            if connection is not None and check_socket_connection_lost(connection):
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

def part_filename_for_node(node_index: int) -> str:
    """Return the split output name for a node (part1.mkv, part2.mkv, ...)."""
    return f"{PART_FILENAME_PREFIX}{node_index}{PART_FILENAME_SUFFIX}"


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
    input_video: str = DEFAULT_INPUT_VIDEO,
    parts_directory: str = MASTER_INPUT_DIRECTORY,
) -> str:
    """Fill split_command placeholders so ffmpeg produces exactly one part per active node."""
    if "{segment_times}" in split_command:
        duration_seconds = get_video_duration_seconds(input_video)
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
    input_video: str = DEFAULT_INPUT_VIDEO,
    parts_directory: str = MASTER_INPUT_DIRECTORY,
) -> None:
    """On start, master runs split_command from config against the input video,
    writing exactly active_node_count part files into parts_directory (master/input/)."""
    print(PROGRESS_SPLITTING_FILE)
    Path(parts_directory).mkdir(parents=True, exist_ok=True)

    if active_node_count == 1:
        destination = Path(parts_directory) / part_filename_for_node(1)
        shutil.copyfile(input_video, destination)
        return

    resolved_command = build_split_command(
        split_command, active_node_count, input_video, parts_directory
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
    active_node_count: int, parts_directory: str = MASTER_INPUT_DIRECTORY
) -> list[str]:
    """Verify part1.mkv through partN.mkv exist for each active node."""
    parts_path = Path(parts_directory)
    part_filenames = [
        part_filename_for_node(node_index)
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
    active_node_count: int, output_directory: str = MASTER_OUTPUT_DIRECTORY
) -> str:
    """Generate the contents of filelist.txt for ffmpeg concat demuxer using absolute paths."""
    dir_path = Path(output_directory).resolve()
    lines = [
        f"file '{dir_path / f'{PART_FILENAME_PREFIX}{index}{PART_FILENAME_SUFFIX}'}'"
        for index in range(1, active_node_count + 1)
    ]
    return "\n".join(lines) + "\n"


def write_filelist_txt(
    active_node_count: int, output_directory: str = MASTER_OUTPUT_DIRECTORY
) -> str:
    """Write filelist.txt into output_directory AND root working directory for universal compatibility."""
    dir_path = Path(output_directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    content = generate_filelist_text(active_node_count, output_directory)

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


def run_merge_command(
    merge_command_template: str,
    active_node_count: int,
    output_directory: str = MASTER_OUTPUT_DIRECTORY,
) -> str:
    """Once all worker nodes finish, combine output parts into one final file."""
    print(PROGRESS_MERGING_FILES)
    write_filelist_txt(active_node_count, output_directory)
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

    final_output_path = Path(output_directory) / "output.mkv"
    if not final_output_path.is_file():
        raise FileNotFoundError(
            f"Merged output file not found at expected location: {final_output_path}"
        )

    print(f"{PROGRESS_FINISHED} (master: {final_output_path.name})")
    return str(final_output_path)


# ==============================================================================
# SECTION 11: MASTER CLI STATUS DASHBOARD
# ==============================================================================

class StatusDashboard:
    """Live CLI status view tracking master and per-node states."""

    def __init__(self, master_state: str = "initializing") -> None:
        self.master_state = master_state
        self.node_states: Dict[str, Dict[str, Any]] = {}

    def set_master_state(self, state: str) -> None:
        """Update overall master state (e.g. splitting file, merging files, finished)."""
        self.master_state = state
        self.render()

    def update_node(
        self,
        node_ip: str,
        node_id: str,
        filename: str,
        state: str,
        elapsed_seconds: float | None = None,
        pct: float | None = None,
        eta: str | None = None,
    ) -> None:
        """Update individual worker node state."""
        self.node_states[node_ip] = {
            "node_id": node_id,
            "filename": filename,
            "state": state,
            "elapsed": elapsed_seconds,
            "pct": pct,
            "eta": eta,
        }
        self.render()

    def remove_node(self, node_ip: str) -> None:
        """Remove a node from the active dashboard view."""
        self.node_states.pop(node_ip, None)
        self.render()

    def render(self) -> None:
        """Render formatted CLI dashboard view."""
        header = f"=== MASTER DASHBOARD: [{self.master_state.upper()}] ==="
        divider = "=" * len(header)

        lines = ["", divider, header, divider]
        lines.append(
            f"{'NODE IP':<16} {'DEVICE':<10} {'PART FILE':<12} {'STATE':<16} {'PROGRESS / RUN TIME'}"
        )
        lines.append("-" * len(header))

        if not self.node_states:
            lines.append("  (Waiting for worker connections...)")
        else:
            for ip, info in self.node_states.items():
                state_str = info["state"]
                elapsed = info["elapsed"]
                pct = info["pct"]
                eta = info["eta"]

                prog_parts = []
                if elapsed is not None:
                    m, s = int(elapsed) // 60, int(elapsed) % 60
                    prog_parts.append(f"{m:02d}:{s:02d}")
                if pct is not None:
                    prog_parts.append(f"{pct:.1f}%")
                if eta is not None:
                    prog_parts.append(f"[ETA: {eta}]")

                prog_str = " ".join(prog_parts)
                lines.append(
                    f"{ip:<16} {info['node_id']:<10} {info['filename']:<12} {state_str:<16} {prog_str}"
                )

        lines.append(divider)
        print("\n".join(lines))


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
