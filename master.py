"""Master node entry point: load config, validate node list, listen on port 5000."""

import argparse
import socket
import sys

from config import load_config
from constants import FIXED_PORT


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


def run_master() -> None:
    """Load config, validate addresses, and listen for worker connections."""
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

    print(f"Loaded config from {arguments.config}")
    print(f"Active nodes ({len(active_nodes)}): {', '.join(active_nodes)}")
    print(f"Spare nodes ({len(spare_nodes)}): {', '.join(spare_nodes)}")
    print(f"Listening on port {FIXED_PORT}...")

    listening_socket = open_listening_socket(FIXED_PORT)
    worker_connections: list[socket.socket] = []

    try:
        while True:
            connection, address = listening_socket.accept()
            print(f"Worker connected from {address[0]}:{address[1]}")
            worker_connections.append(connection)
    except KeyboardInterrupt:
        print("\nMaster shutting down.")
    finally:
        for connection in worker_connections:
            connection.close()
        listening_socket.close()


if __name__ == "__main__":
    run_master()
