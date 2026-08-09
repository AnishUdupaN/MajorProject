"""Master node entry point: load config, validate node list, listen on port 5000."""

import argparse
import socket
import sys
from pathlib import Path

from config import load_config
from constants import (
    MASTER_INPUT_DIRECTORY,
    MASTER_OUTPUT_DIRECTORY,
    CONTROL_PORT,
    FIXED_PORT,
    PROGRESS_SENDING_FILE,
)
from control_messages import receive_file_received_message, send_ready_message
from devices import write_devices_json
from file_transfer_daemon import FileTransferDaemon, start_file_transfer_daemon
from network import get_local_ip_for_peer
from split import part_filename_for_node, run_split_command, verify_part_files


def parse_master_arguments() -> argparse.Namespace:
    """Parse CLI arguments for master startup with a list of worker IP addresses."""
    parser = argparse.ArgumentParser(
        description="Start the master node for distributed video processing."
    )
    parser.add_argument(
        "--config",
        default="config.ini",
        help="Path to the config file (default: config.ini)",
    )
    parser.add_argument(
        "--master-ip",
        default=None,
        help="IP address workers should use to reach this master (auto-detected if omitted)",
    )
    parser.add_argument(
        "--parts-directory",
        default=MASTER_INPUT_DIRECTORY,
        help=f"Directory where split part files are written and served (default: {MASTER_INPUT_DIRECTORY})",
    )
    parser.add_argument(
        "worker_ip_addresses",
        nargs="+",
        help="IP addresses of worker nodes (at least max_nodes + 1 required)",
    )
    return parser.parse_args()


def split_active_and_spare_addresses(
    worker_ip_addresses: list[str], max_nodes: int
) -> tuple[list[str], list[str]]:
    """First max_nodes addresses are active nodes; remaining addresses are spares."""
    active_nodes = worker_ip_addresses[:max_nodes]
    spare_nodes = worker_ip_addresses[max_nodes:]
    return active_nodes, spare_nodes


def validate_worker_address_count(
    worker_ip_addresses: list[str], max_nodes: int
) -> None:
    """Reject startup if fewer than max_nodes + 1 IP addresses are given."""
    required_count = max_nodes + 1
    if len(worker_ip_addresses) < required_count:
        raise ValueError(
            f"At least {required_count} worker IP addresses are required "
            f"(max_nodes={max_nodes} active + 1 spare), "
            f"but only {len(worker_ip_addresses)} were given"
        )


def open_listening_socket(port: int) -> socket.socket:
    """Open a listening socket on the fixed port."""
    listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listening_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listening_socket.bind(("0.0.0.0", port))
    listening_socket.listen()
    return listening_socket


def wait_for_active_worker_connections(
    listening_socket: socket.socket, active_nodes: list[str]
) -> dict[str, socket.socket]:
    """Accept connections until each active node IP has connected."""
    worker_connections: dict[str, socket.socket] = {}
    pending_nodes = set(active_nodes)

    print(f"Waiting for active workers: {', '.join(active_nodes)}")

    while pending_nodes:
        connection, address = listening_socket.accept()
        worker_ip_address = address[0]
        print(f"Worker connected from {worker_ip_address}:{address[1]}")

        if worker_ip_address not in active_nodes:
            print(
                f"Ignoring connection from non-active address {worker_ip_address}",
                file=sys.stderr,
            )
            connection.close()
            continue

        if worker_ip_address in worker_connections:
            print(
                f"Replacing existing connection from {worker_ip_address}",
                file=sys.stderr,
            )
            worker_connections[worker_ip_address].close()

        worker_connections[worker_ip_address] = connection
        pending_nodes.discard(worker_ip_address)

    return worker_connections


def distribute_part_files_to_active_nodes(
    active_nodes: list[str],
    worker_connections: dict[str, socket.socket],
    parts_directory: str,
    master_ip_address: str | None = None,
) -> list[FileTransferDaemon]:
    """Start one file-transfer daemon per node and distribute part files in parallel."""
    daemons: list[FileTransferDaemon] = []
    pending_transfers: list[tuple[str, str, socket.socket, str, int]] = []

    for node_index, worker_ip_address in enumerate(active_nodes, start=1):
        part_filename = part_filename_for_node(node_index)
        daemon = start_file_transfer_daemon(
            allocated_filenames=[part_filename],
            serve_directory=parts_directory,
        )
        daemons.append(daemon)
        if daemon.port is None:
            raise RuntimeError("File-transfer daemon did not report a listening port")

        connection = worker_connections[worker_ip_address]
        reachable_master_ip = master_ip_address or get_local_ip_for_peer(
            worker_ip_address
        )
        pending_transfers.append(
            (
                worker_ip_address,
                part_filename,
                connection,
                reachable_master_ip,
                daemon.port,
            )
        )
        print(
            f"File-transfer daemon for {worker_ip_address} listening on port {daemon.port}"
        )

    for worker_ip_address, part_filename, connection, reachable_master_ip, file_transfer_port in pending_transfers:
        print(f"{PROGRESS_SENDING_FILE} ({worker_ip_address}, {part_filename})")
        send_ready_message(connection, reachable_master_ip, file_transfer_port)

    for worker_ip_address, part_filename, connection, _, _ in pending_transfers:
        receive_file_received_message(connection)
        print(f"Received {part_filename} confirmation from {worker_ip_address}")

    return daemons


def run_master() -> None:
    """Load config, validate addresses, split video, and distribute part files."""
    arguments = parse_master_arguments()

    try:
        config = load_config(arguments.config)
    except ValueError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        validate_worker_address_count(
            arguments.worker_ip_addresses, config.max_nodes
        )
    except ValueError as exc:
        print(f"Startup error: {exc}", file=sys.stderr)
        sys.exit(1)

    active_nodes, spare_nodes = split_active_and_spare_addresses(
        arguments.worker_ip_addresses, config.max_nodes
    )

    device_mapping = write_devices_json(active_nodes)

    # master/input holds split parts; master/output holds merged results.
    Path(arguments.parts_directory).mkdir(parents=True, exist_ok=True)
    Path(MASTER_OUTPUT_DIRECTORY).mkdir(parents=True, exist_ok=True)

    print(f"Loaded config from {arguments.config}")
    print(f"Active nodes ({len(active_nodes)}): {', '.join(active_nodes)}")
    print(f"Spare nodes ({len(spare_nodes)}): {', '.join(spare_nodes)}")
    print(f"Wrote devices.json: {device_mapping}")
    print(f"Listening on control port {CONTROL_PORT}...")

    listening_socket = open_listening_socket(FIXED_PORT)
    daemons: list[FileTransferDaemon] = []

    try:
        worker_connections = wait_for_active_worker_connections(
            listening_socket, active_nodes
        )

        run_split_command(
            config.split_command,
            len(active_nodes),
            parts_directory=arguments.parts_directory,
        )
        verify_part_files(len(active_nodes), arguments.parts_directory)

        daemons = distribute_part_files_to_active_nodes(
            active_nodes,
            worker_connections,
            arguments.parts_directory,
            master_ip_address=arguments.master_ip,
        )

        print("Phase 2 complete: split files distributed to all active workers.")
        print("Master idling for later phases...")

        while True:
            connection, address = listening_socket.accept()
            print(f"Additional connection from {address[0]}:{address[1]} (ignored)")
            connection.close()
    except KeyboardInterrupt:
        print("\nMaster shutting down.")
    except (OSError, RuntimeError, ValueError, FileNotFoundError) as exc:
        print(f"Master error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        for daemon in daemons:
            daemon.stop()
        listening_socket.close()


if __name__ == "__main__":
    run_master()
