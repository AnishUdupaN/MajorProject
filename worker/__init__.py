"""Worker package for distributed compute nodes."""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure worker directory is in sys.path
_worker_dir = str(Path(__file__).resolve().parent)
if _worker_dir not in sys.path:
    sys.path.insert(0, _worker_dir)

from worker.binary_manager import (
    do_binary_handshake,
    ensure_executable,
    handle_binary_handshake_request,
)
from worker.cli import parse_worker_arguments
from worker.connection import (
    connect_to_master,
    discover_master_address,
    perform_handshake_authentication,
)
from worker.task_executor import run_worker_task
from worker.task_fetcher import fetch_allocated_part_file
from worker.worker import run_worker
