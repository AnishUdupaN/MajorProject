"""Command-line argument parsing and input validation for the master node."""

from __future__ import annotations

import argparse

from core.constants import MASTER_INPUT_DIRECTORY


def parse_master_arguments() -> argparse.Namespace:
    """Parse CLI arguments for master startup with optional list of worker IP addresses."""
    parser = argparse.ArgumentParser(
        description="Start the master node for distributed processing."
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to initial config file (optional; config is selected interactively on dashboard)",
    )
    parser.add_argument(
        "--master-ip",
        default=None,
        help="IP address workers should use to reach this master (auto-detected if omitted)",
    )
    parser.add_argument(
        "--password",
        default=None,
        help="Static password for worker authentication (disables random PIN)",
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Automatically confirm prompts (e.g. running without spare nodes)",
    )
    parser.add_argument(
        "--retry-timeout",
        type=float,
        default=15.0,
        help="Timeout in seconds to wait for a disconnected worker to reconnect before failing over (default: 15.0)",
    )
    parser.add_argument(
        "--parts-directory",
        default=MASTER_INPUT_DIRECTORY,
        help=f"Directory where split part files are written and served (default: {MASTER_INPUT_DIRECTORY})",
    )
    parser.add_argument(
        "worker_ip_addresses",
        nargs="*",
        default=None,
        help="Optional IP addresses of worker nodes. If omitted, auto-discovers workers via mDNS.",
    )
    parser.add_argument(
        "--webserver",
        action="store_true",
        help="Start a web dashboard on port 8080",
    )
    return parser.parse_args()


def validate_worker_address_count(
    worker_ip_addresses: list[str] | None, min_nodes: int
) -> None:
    """Reject startup if manual worker IPs are given but fewer than min_nodes."""
    if worker_ip_addresses:
        if len(worker_ip_addresses) < min_nodes:
            raise ValueError(
                f"At least {min_nodes} worker IP addresses are required "
                f"(min_nodes={min_nodes}), "
                f"but only {len(worker_ip_addresses)} were given"
            )
