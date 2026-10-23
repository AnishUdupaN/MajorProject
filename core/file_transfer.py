"""HTTP FileTransferDaemon and worker upload/download helpers."""

from __future__ import annotations

import argparse
import atexit
import http.client
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.constants import DAEMON_LISTENING_PREFIX, DAEMON_READY_TIMEOUT_SECONDS

# ==============================================================================
# WORKER HTTP CLIENT FILE REQUESTS
# ==============================================================================


def request_file_list(
    master_ip_address: str,
    file_transfer_port: int,
    max_retries: int = 5,
    retry_delay_seconds: float = 0.5,
) -> list[str]:
    """Worker calls GET /listfiles to see what's allocated to it."""
    url = f"http://{master_ip_address}:{file_transfer_port}/listfiles"
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=15.0) as response:
                payload = json.loads(response.read().decode("utf-8"))
            return payload["files"]
        except (
            urllib.error.URLError,
            http.client.HTTPException,
            ConnectionError,
            OSError,
            TimeoutError,
            json.JSONDecodeError,
        ) as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(retry_delay_seconds * attempt)
            else:
                break
    raise RuntimeError(
        f"Failed to fetch file list from {master_ip_address}:{file_transfer_port} "
        f"after {max_retries} attempts: {last_error}"
    ) from last_error


def request_file(
    master_ip_address: str,
    file_transfer_port: int,
    filename: str,
    destination_directory: str = ".",
    max_retries: int = 5,
    retry_delay_seconds: float = 1.0,
) -> str:
    """Worker calls GET /file/<filename> to fetch its allocated part file with streaming and automatic retries."""
    url = f"http://{master_ip_address}:{file_transfer_port}/file/{filename}"
    destination_path = Path(destination_directory) / filename
    temp_path = Path(destination_directory) / f".tmp_{filename}"

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            temp_path.unlink(missing_ok=True)
            with urllib.request.urlopen(url, timeout=1200.0) as response:
                content_length_header = response.headers.get("Content-Length")
                expected_bytes = (
                    int(content_length_header) if content_length_header else None
                )
                total_read = 0
                with open(temp_path, "wb") as f:
                    while True:
                        chunk = response.read(64 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)
                        total_read += len(chunk)

                if expected_bytes is not None and total_read < expected_bytes:
                    raise http.client.IncompleteRead(
                        partial=total_read,
                        expected=expected_bytes - total_read,
                    )

            temp_path.replace(destination_path)
            return str(destination_path)
        except (
            urllib.error.URLError,
            http.client.HTTPException,
            http.client.IncompleteRead,
            ConnectionError,
            OSError,
            TimeoutError,
        ) as exc:
            temp_path.unlink(missing_ok=True)
            last_error = exc
            if attempt < max_retries:
                time.sleep(retry_delay_seconds * attempt)
            else:
                break

    raise RuntimeError(
        f"Failed to fetch {filename} from {master_ip_address}:{file_transfer_port} "
        f"after {max_retries} attempts: {last_error}"
    ) from last_error


def upload_result_file(
    master_ip_address: str,
    file_transfer_port: int,
    source_filepath: str,
    max_retries: int = 5,
    retry_delay_seconds: float = 1.0,
) -> None:
    """Worker calls POST /file/<filename> to send its output file back to master's daemon."""
    path = Path(source_filepath)
    if not path.is_file():
        raise FileNotFoundError(
            f"Result file to upload does not exist: {source_filepath}"
        )

    filename = path.name
    file_bytes = path.read_bytes()
    file_size = len(file_bytes)
    url = f"http://{master_ip_address}:{file_transfer_port}/file/{filename}"

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            request = urllib.request.Request(
                url,
                data=file_bytes,
                headers={
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(file_size),
                    "Connection": "close",
                },
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=1200.0) as response:
                if response.status == 200:
                    return
                raise RuntimeError(
                    f"Failed to upload {filename}: HTTP {response.status}"
                )
        except (
            urllib.error.URLError,
            http.client.HTTPException,
            ConnectionError,
            OSError,
            TimeoutError,
        ) as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(retry_delay_seconds * attempt)
            else:
                break

    raise RuntimeError(
        f"Failed to upload {filename} to {master_ip_address}:{file_transfer_port} "
        f"after {max_retries} attempts: {last_error}"
    ) from last_error


# ==============================================================================
# HTTP FILE-TRANSFER DAEMON & PROCESS MANAGEMENT
# ==============================================================================

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

        protocol_version = "HTTP/1.1"
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
            self.send_header("Connection", "close")
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

            file_size = file_path.stat().st_size
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(file_size))
            self.send_header("Connection", "close")
            self.end_headers()

            try:
                with open(file_path, "rb") as f:
                    shutil.copyfileobj(f, self.wfile, length=64 * 1024)
                self.wfile.flush()
            except (ConnectionError, BrokenPipeError, OSError):
                pass

        def _receive_file(self) -> None:
            """POST /file/<filename> receives uploaded result file bytes from worker."""
            filename = unquote(
                self.path.removeprefix("/file/")
                .removeprefix("/upload/")
                .lstrip("/")
            )
            filename = os.path.basename(filename)
            if not filename or filename.startswith("."):
                self.send_error(400, "Bad Request - invalid filename")
                return

            self.output_directory.mkdir(parents=True, exist_ok=True)
            output_file_path = self.output_directory / filename
            temp_file_path = self.output_directory / f".tmp_{filename}"

            content_length_header = self.headers.get("Content-Length")
            transfer_encoding = self.headers.get("Transfer-Encoding", "").lower()

            if content_length_header is not None:
                try:
                    content_length = int(content_length_header)
                except ValueError:
                    self.send_error(400, "Bad Request - invalid Content-Length")
                    return

                if content_length < 0:
                    self.send_error(400, "Bad Request - invalid Content-Length")
                    return

                remaining = content_length
                try:
                    with open(temp_file_path, "wb") as f:
                        while remaining > 0:
                            chunk_size = min(remaining, 64 * 1024)
                            chunk = self.rfile.read(chunk_size)
                            if not chunk:
                                break
                            f.write(chunk)
                            remaining -= len(chunk)

                    if remaining > 0:
                        temp_file_path.unlink(missing_ok=True)
                        self.send_error(400, "Bad Request - incomplete upload body")
                        return

                    temp_file_path.replace(output_file_path)
                except Exception as e:
                    temp_file_path.unlink(missing_ok=True)
                    self.send_error(500, f"Error saving file: {e}")
                    return
            elif "chunked" in transfer_encoding or self.headers.get("Connection", "").lower() == "close":
                try:
                    with open(temp_file_path, "wb") as f:
                        while True:
                            chunk = self.rfile.read(64 * 1024)
                            if not chunk:
                                break
                            f.write(chunk)
                    temp_file_path.replace(output_file_path)
                except Exception as e:
                    temp_file_path.unlink(missing_ok=True)
                    self.send_error(500, f"Error saving file: {e}")
                    return
            else:
                self.send_error(400, "Bad Request - missing Content-Length")
                return

            payload = json.dumps(
                {"status": "received", "filename": filename}
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Connection", "close")
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
    import socket

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


def _parent_watchdog(parent_pid: int, server: HTTPServer | ThreadingHTTPServer) -> None:
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
    server = ThreadingHTTPServer(("0.0.0.0", 0), handler_class)
    server.daemon_threads = True
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


def parse_daemon_arguments(
    raw_arguments: list[str] | None = None,
) -> argparse.Namespace:
    """Parse CLI arguments for a standalone file-transfer daemon process."""
    parser = argparse.ArgumentParser(
        description="Run one per-node HTTP file-transfer daemon instance."
    )
    parser.add_argument("--serve-directory", required=True)
    parser.add_argument("--output-directory", default=None)
    parser.add_argument("--files", nargs="+", required=True)
    return parser.parse_args(raw_arguments)


if __name__ == "__main__":
    arguments = parse_daemon_arguments()
    run_file_transfer_daemon(
        arguments.files,
        arguments.serve_directory,
        arguments.output_directory,
    )
