"""Worker node entry point: connect to master on port 5000 and idle until contacted."""

import argparse
import socket
import sys

from constants import FIXED_PORT


def parse_worker_arguments() -> argparse.Namespace:
    """Parse CLI arguments for worker startup with only the master IP address."""
    parser = argparse.ArgumentParser(
        description="Start a worker node for distributed video processing."
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


def idle_until_contacted(connection: socket.socket) -> None:
    """Idle on the connection until the master contacts this worker."""
    while True:
        data = connection.recv(4096)
        if not data:
            break


def run_worker() -> None:
    """Connect to master and idle until contacted."""
    arguments = parse_worker_arguments()
    master_ip_address = arguments.master_ip_address

    print(f"Connecting to master at {master_ip_address}:{FIXED_PORT}...")

    try:
        connection = connect_to_master(master_ip_address, FIXED_PORT)
    except OSError as exc:
        print(f"Connection error: {exc}", file=sys.stderr)
        sys.exit(1)

    print("Connected. Idling until contacted by master...")

    try:
        idle_until_contacted(connection)
    except (ConnectionError, KeyboardInterrupt):
        print("\nWorker shutting down.")
    finally:
        connection.close()


if __name__ == "__main__":
    run_worker()
