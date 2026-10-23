"""Master package for orchestrating distributed computation."""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure master directory is in sys.path
_master_dir = str(Path(__file__).resolve().parent)
if _master_dir not in sys.path:
    sys.path.insert(0, _master_dir)

from master.bench_and_sort import (
    rank_and_partition_workers,
    split_active_and_spare_addresses,
)
from master.binary_distributor import check_and_distribute_binaries
from master.cli import parse_master_arguments, validate_worker_address_count
from master.connection_pool import (
    WorkerConnectionPool,
    open_listening_socket,
    safe_select_sockets,
)
from master.failsafe import (
    handle_no_spare_nodes_recovery,
    reassign_task_to_spare_node,
)
from master.master import run_master
from master.monitor import monitor_worker_executions_and_collect_results
from master.task_distributor import (
    distribute_part_file_to_single_node,
    distribute_part_files_to_active_nodes,
)
