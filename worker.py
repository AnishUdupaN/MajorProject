"""Worker node entry point: connect to master on port 5000 and idle until contacted."""

import argparse
import socket
import sys
from pathlib import Path

from constants import CONTROL_PORT, FIXED_PORT, WORKER_INPUT_DIRECTORY, WORKER_OUTPUT_DIRECTORY
from control_messages import receive_ready_message, send_file_received_message
from file_request import request_file, request_file_list


def parse_worker_arguments() -> argparse.Namespace:
    """Parse CLI arguments for worker startup with only the master IP address."""
    parser = argparse.ArgumentParser(
        description="Start a worker node for distributed video processing."
    )
    parser.add_argument(
        "--download-directory",
        default=WORKER_INPUT_DIRECTORY,
        help=f"Directory where downloaded part files are saved (default: {WORKER_INPUT_DIRECTORY})",
    )
    parser.add_argument(
        "master_ip_address",
        help="IP address of the master node",
    )
    return parser.parse_args()


def connect_to_master(master_ip_address: str, port: int) -> socket.socket:
    """Connect to the master node on the fixed port."""
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.connect((master_ip_address, port))
    return connection


def fetch_allocated_part_file(
    master_ip_address: str,
    file_transfer_port: int,
    download_directory: str,
) -> str:
    """On receiving ready, list and fetch this worker's allocated part file."""
    allocated_files = request_file_list(master_ip_address, file_transfer_port)
    if not allocated_files:
        raise RuntimeError("No files listed for this worker")

    downloaded_paths = [
        request_file(
            master_ip_address,
            file_transfer_port,
            filename,
            download_directory,
        )
        for filename in allocated_files
    ]
    return downloaded_paths[0]


def idle_until_contacted(
    connection: socket.socket,
    master_ip_address: str,
    download_directory: str,
) -> None:
    """Handle ready messages and fetch allocated files until the connection closes."""
    while True:
        ready_message = receive_ready_message(connection)
        file_transfer_port = ready_message["file_transfer_port"]

        downloaded_path = fetch_allocated_part_file(
            master_ip_address,
            file_transfer_port,
            download_directory,
        )
        print(f"Downloaded part file to {downloaded_path}")

        send_file_received_message(connection)


def run_worker() -> None:
    """Connect to master and handle ready/file-request messages."""
    arguments = parse_worker_arguments()
    master_ip_address = arguments.master_ip_address

    # worker/input holds downloaded parts; worker/output holds processed results.
    Path(arguments.download_directory).mkdir(parents=True, exist_ok=True)
    Path(WORKER_OUTPUT_DIRECTORY).mkdir(parents=True, exist_ok=True)

    print(f"Connecting to master at {master_ip_address}:{CONTROL_PORT}...")

    try:
        connection = connect_to_master(master_ip_address, FIXED_PORT)
    except OSError as exc:
        print(f"Connection error: {exc}", file=sys.stderr)
        sys.exit(1)

    print("Connected. Waiting for ready message from master...")

    try:
        idle_until_contacted(
            connection,
            master_ip_address,
            arguments.download_directory,
        )
    except (ConnectionError, KeyboardInterrupt):
        print("\nWorker shutting down.")
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"Worker error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        connection.close()


if __name__ == "__main__":
    run_worker()
