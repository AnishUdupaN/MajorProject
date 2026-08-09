"""Per-node HTTP file-transfer daemon with /listfiles and /file/<filename>."""

import argparse
import json
import socket
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import unquote

DAEMON_READY_TIMEOUT_SECONDS = 10.0


DAEMON_LISTENING_PREFIX = "LISTENING "


class FileTransferDaemon:
    """Serve one node's allocated file(s) over HTTP on an ephemeral port."""

    def __init__(
        self,
        allocated_filenames: list[str],
        serve_directory: str,
    ) -> None:
        self.allocated_filenames = allocated_filenames
        self.serve_directory = serve_directory
        self.port: int | None = None
        self._process: subprocess.Popen[str] | None = None

    def start(self) -> None:
        """Start this node's file-transfer daemon in a separate process."""
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--serve-directory",
            self.serve_directory,
            "--files",
            *self.allocated_filenames,
        ]
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
    allocated_filenames: list[str], serve_directory: Path
) -> type[BaseHTTPRequestHandler]:
    """Build a request handler bound to one node's allocated files."""
    files_for_node = allocated_filenames
    directory_for_node = serve_directory

    class FileTransferRequestHandler(BaseHTTPRequestHandler):
        """Handle GET /listfiles and GET /file/<filename> for one node."""

        allocated_filenames = files_for_node
        serve_directory = directory_for_node

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
) -> None:
    """Run one file-transfer daemon process for a single active node."""
    handler_class = _make_handler_class(
        allocated_filenames, Path(serve_directory).resolve()
    )
    server = HTTPServer(("0.0.0.0", 0), handler_class)
    port = server.server_address[1]
    sys.stdout.write(f"{DAEMON_LISTENING_PREFIX}{port}\n")
    sys.stdout.flush()
    server.serve_forever()


def start_file_transfer_daemon(
    allocated_filenames: list[str],
    serve_directory: str,
) -> FileTransferDaemon:
    """Start a separate file-transfer daemon instance for one active node."""
    daemon = FileTransferDaemon(allocated_filenames, serve_directory)
    daemon.start()
    return daemon


def parse_daemon_arguments() -> argparse.Namespace:
    """Parse CLI arguments for a standalone file-transfer daemon process."""
    parser = argparse.ArgumentParser(
        description="Run one per-node HTTP file-transfer daemon instance."
    )
    parser.add_argument("--serve-directory", required=True)
    parser.add_argument("--files", nargs="+", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_daemon_arguments()
    run_file_transfer_daemon(
        arguments.files,
        arguments.serve_directory,
    )
