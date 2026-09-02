"""Worker node entry point: connect to master, download part file, execute task, and send finished signal."""

import argparse
import socket
import sys
import time
from pathlib import Path


from core import (
    CONTROL_PORT,
    FIXED_PORT,
    MDNS_SERVICE_WORKER,
    MDNS_WORKER_FQDN,
    PROGRESS_FINISHED,
    WORKER_INPUT_DIRECTORY,
    WORKER_OUTPUT_DIRECTORY,
    Config,
    MasterShutdownError,
    MdnsAnnouncer,
    discover_master_ip,
    load_config,
    receive_ready_message,
    request_file,
    request_file_list,
    run_execute_command,
    get_shared_secret,
    send_auth_message,
    send_failed_message,
    send_file_received_message,
    send_finished_message,
    upload_result_file,
)


def parse_worker_arguments() -> argparse.Namespace:
    """Parse CLI arguments for worker startup with optional master IP address and flags."""
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
        "--reconnect-timeout",
        type=float,
        default=10.0,
        help="Maximum time in seconds to retry connecting to master before exiting safely (default: 10.0)",
    )
    parser.add_argument(
        "master_ip_address",
        nargs="?",
        default=None,
        help="Optional IP address of master node. If omitted, auto-discovers Master via mDNS.",
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
    config: Config | None = None,
) -> None:
    """Handle ready message, fetch allocated part file, run execute_command, send finished signal, and upload result."""
    ready_message = receive_ready_message(connection)
    file_transfer_port = ready_message["file_transfer_port"]

    downloaded_path = fetch_allocated_part_file(
        master_ip_address,
        file_transfer_port,
        download_directory,
    )
    print(f"Downloaded part file to {downloaded_path}")

    send_file_received_message(connection)

    # Phase 3 & 4: Execute task on the downloaded part file.
    # Force-stops process if connection is lost or user presses 'k'.
    try:
        elapsed_time = run_execute_command(
            execute_command_template,
            input_file=downloaded_path,
            output_directory=WORKER_OUTPUT_DIRECTORY,
            connection=connection,
            simulate_failure_after=simulate_failure_after,
            config=config,
        )
    except (KeyboardInterrupt, RuntimeError, ValueError) as exc:
        try:
            send_failed_message(connection, str(exc))
        except Exception:
            pass
        raise

    part_filename = Path(downloaded_path).name
    output_filepath = Path(WORKER_OUTPUT_DIRECTORY) / part_filename

    # Phase 4 Task 4.1: Upload completed output file back to Master's per-node file-transfer daemon FIRST
    if output_filepath.is_file():
        upload_result_file(master_ip_address, file_transfer_port, str(output_filepath))
        print(f"Uploaded result {part_filename} to master daemon on port {file_transfer_port}")

    # Send finished signal to Master over control port AFTER upload completes
    send_finished_message(connection, elapsed_time, part_filename)

    print(f"{PROGRESS_FINISHED} for worker ({part_filename})")




def run_worker() -> None:
    """Connect to master and persist in an idle loop, executing tasks as assigned."""
    arguments = parse_worker_arguments()
    master_ip_address = arguments.master_ip_address

    try:
        config = load_config(arguments.config)
    except ValueError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        sys.exit(1)

    shared_secret = get_shared_secret()

    # worker/input holds downloaded parts; worker/output holds processed results.
    Path(arguments.download_directory).mkdir(parents=True, exist_ok=True)
    Path(WORKER_OUTPUT_DIRECTORY).mkdir(parents=True, exist_ok=True)

    # Start worker mDNS announcer in background
    mdns_announcer = MdnsAnnouncer(
        fqdn=MDNS_WORKER_FQDN, service=MDNS_SERVICE_WORKER
    )
    mdns_announcer.start()

    if not master_ip_address:
        print("No master IP address specified. Auto-discovering Master via mDNS...")
        try:
            master_ip_address = discover_master_ip(timeout_seconds=60.0)
        except TimeoutError as exc:
            print(f"Discovery error: {exc}", file=sys.stderr)
            sys.exit(1)

    print(f"Connecting to Master at {master_ip_address}:{FIXED_PORT}...")
    print("Press 'k' to kill the task.")

    connection = None
    simulate_fail = arguments.simulate_failure_after
    reconnect_start_time: float | None = None
    max_reconnect_timeout = arguments.reconnect_timeout

    while True:
        if connection is None:
            if reconnect_start_time is None:
                reconnect_start_time = time.time()
            elif time.time() - reconnect_start_time >= max_reconnect_timeout:
                print(
                    f"\nMaster at {master_ip_address}:{FIXED_PORT} unreachable for {max_reconnect_timeout:.0f}s. Exiting safely.",
                    file=sys.stderr,
                )
                sys.exit(0)

            print(f"\nConnecting to master at {master_ip_address}:{FIXED_PORT}...")
            try:
                connection = connect_to_master(
                    master_ip_address, FIXED_PORT, bind_ip=arguments.bind_ip
                )
                print("Connected to master. Node state: IDLE. Waiting for task assignment...")
                # VULN-08: Send shared secret for authentication if configured
                if shared_secret:
                    send_auth_message(connection, shared_secret)
                reconnect_start_time = None
            except OSError as exc:
                print(f"Connection attempt failed ({exc}). Retrying in 2 seconds...", file=sys.stderr)
                time.sleep(2.0)
                continue

        try:
            # Wait for ready message or disconnection check
            current_sim_fail = simulate_fail
            simulate_fail = None  # One-shot simulation flag: consume immediately
            run_worker_task(
                connection,
                master_ip_address,
                arguments.download_directory,
                config.execute_command,
                simulate_failure_after=current_sim_fail,
                config=config,
            )
            print("Task completed. Node returning to IDLE state. Waiting for next task...")

        except MasterShutdownError:
            print("\nMaster completed processing and initiated shutdown. Exiting safely.")
            if connection:
                try:
                    connection.close()
                except Exception:
                    pass
            sys.exit(0)
        except KeyboardInterrupt as exc:
            print(f"\nTask aborted by user ('k' key / interrupt): {exc}")
            if connection:
                try:
                    connection.close()
                except Exception:
                    pass
            connection = None
            simulate_fail = None
            print("Node state: IDLE. Reconnecting to master...")
            time.sleep(0.5)
        except (ConnectionError, OSError) as exc:
            print(f"\nControl socket disconnected ({exc}). Reconnecting to master...", file=sys.stderr)
            if connection:
                try:
                    connection.close()
                except Exception:
                    pass
            connection = None
            time.sleep(1.0)
        except (RuntimeError, ValueError) as exc:
            print(f"\nTask error ({exc}). Reconnecting to master...", file=sys.stderr)
            if connection:
                try:
                    connection.close()
                except Exception:
                    pass
            connection = None
            time.sleep(1.0)



if __name__ == "__main__":
    run_worker()

