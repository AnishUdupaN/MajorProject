"""Worker node entry point: connect to master, download part file, execute task, and send finished signal."""

import argparse
import socket
import sys
from pathlib import Path

from config import load_config
from constants import (
    CONTROL_PORT,
    FIXED_PORT,
    PROGRESS_FINISHED,
    WORKER_INPUT_DIRECTORY,
    WORKER_OUTPUT_DIRECTORY,
)
from control_messages import (
    receive_ready_message,
    send_file_received_message,
    send_finished_message,
)
from execution import run_execute_command
from file_request import request_file, request_file_list


def parse_worker_arguments() -> argparse.Namespace:
    """Parse CLI arguments for worker startup with master IP address and optional flags."""
    parser = argparse.ArgumentParser(
        description="Start a worker node for distributed video processing."
    )
    parser.add_argument(
        "--config",
        default="config.ini",
        help="Path to the config file (default: config.ini)",
    )
    parser.add_argument(
        "--download-directory",
        default=WORKER_INPUT_DIRECTORY,
        help=f"Directory where downloaded part files are saved (default: {WORKER_INPUT_DIRECTORY})",
    )
    parser.add_argument(
        "--simulate-failure-after",
        type=float,
        default=None,
        help="Simulate node failure after specified seconds of execution (for automated testing)",
    )
    parser.add_argument(
        "--bind-ip",
        default=None,
        help="Optional local IP to bind outgoing connection from (useful for multi-node loopback testing)",
    )
    parser.add_argument(
        "master_ip_address",
        help="IP address of the master node",
    )
    return parser.parse_args()


def connect_to_master(
    master_ip_address: str, port: int, bind_ip: str | None = None
) -> socket.socket:
    """Connect to the master node on the fixed port."""
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if bind_ip:
        connection.bind((bind_ip, 0))
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


def run_worker_task(
    connection: socket.socket,
    master_ip_address: str,
    download_directory: str,
    execute_command_template: str,
    simulate_failure_after: float | None = None,
) -> None:
    """Handle ready message, fetch allocated part file, run execute_command, and send finished signal."""
    ready_message = receive_ready_message(connection)
    file_transfer_port = ready_message["file_transfer_port"]

    downloaded_path = fetch_allocated_part_file(
        master_ip_address,
        file_transfer_port,
        download_directory,
    )
    print(f"Downloaded part file to {downloaded_path}")

    send_file_received_message(connection)

    # Phase 3: Execute task on the downloaded part file.
    # Note: Pressing 'k' during execution will raise KeyboardInterrupt to simulate node failure/kill.
    elapsed_time = run_execute_command(
        execute_command_template,
        input_file=downloaded_path,
        output_directory=WORKER_OUTPUT_DIRECTORY,
        simulate_failure_after=simulate_failure_after,
    )

    part_filename = Path(downloaded_path).name
    send_finished_message(connection, elapsed_time, part_filename)
    print(f"{PROGRESS_FINISHED} for worker ({part_filename})")


def run_worker() -> None:
    """Connect to master, handle ready/file-request messages, and run task execution."""
    arguments = parse_worker_arguments()
    master_ip_address = arguments.master_ip_address

    try:
        config = load_config(arguments.config)
    except ValueError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        sys.exit(1)

    # worker/input holds downloaded parts; worker/output holds processed results.
    Path(arguments.download_directory).mkdir(parents=True, exist_ok=True)
    Path(WORKER_OUTPUT_DIRECTORY).mkdir(parents=True, exist_ok=True)

    print(f"Connecting to master at {master_ip_address}:{CONTROL_PORT}...")

    try:
        connection = connect_to_master(
            master_ip_address, FIXED_PORT, bind_ip=arguments.bind_ip
        )
    except OSError as exc:

        print(f"Connection error: {exc}", file=sys.stderr)
        sys.exit(1)

    print("Connected. Waiting for ready message from master...")
    print("Tip: While executing, press 'k' at any time to kill the task and simulate a node drop.")

    try:
        run_worker_task(
            connection,
            master_ip_address,
            arguments.download_directory,
            config.execute_command,
            simulate_failure_after=arguments.simulate_failure_after,
        )
    except (ConnectionError, KeyboardInterrupt) as exc:
        print(f"\nWorker task aborted / disconnected: {exc}")
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"Worker error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        connection.close()


if __name__ == "__main__":
    run_worker()
