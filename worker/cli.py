"""Command-line argument parsing for worker nodes."""

from __future__ import annotations

import argparse

from core.constants import WORKER_INPUT_DIRECTORY


def parse_worker_arguments() -> argparse.Namespace:
    """Parse CLI arguments for worker startup with optional master IP address and flags."""
    parser = argparse.ArgumentParser(
        description="Start a worker node for distributed processing."
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to the config file (default: auto-discovered from config/ directory)",
    )
    parser.add_argument(
        "--username",
        default=None,
        help="Custom username to display on master dashboard (overrides system username)",
    )
    parser.add_argument(
        "--password",
        default=None,
        help="Static password for connection authentication",
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
        "--worker-id",
        default=None,
        help="Custom persistent worker ID for connection tracking",
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
