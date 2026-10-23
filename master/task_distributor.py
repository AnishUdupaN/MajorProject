"""Task and part-file distribution to active worker nodes via ephemeral HTTP daemons."""

from __future__ import annotations

import socket
from typing import TYPE_CHECKING

from core.constants import (
    MASTER_OUTPUT_DIRECTORY,
    PROGRESS_SENDING_FILE,
)
from core.dashboard import StatusDashboard, printlog
from core.discovery import get_local_ip_for_peer
from core.file_transfer import FileTransferDaemon, start_file_transfer_daemon
from core.messaging import (
    receive_file_received_message,
    send_ready_message,
)
from core.task_prep import part_filename_for_node

if TYPE_CHECKING:
    from core.config import Config
    from master.connection_pool import WorkerConnectionPool


def distribute_part_file_to_single_node(
    worker_ip_address: str,
    part_filename: str,
    connection: socket.socket,
    parts_directory: str,
    master_ip_address: str | None = None,
    execute_command: str | None = None,
) -> FileTransferDaemon:
    """Start file-transfer daemon and send ready message to a single worker node."""
    daemon = start_file_transfer_daemon(
        allocated_filenames=[part_filename],
        serve_directory=parts_directory,
        output_directory=MASTER_OUTPUT_DIRECTORY,
    )
    if daemon.port is None:
        raise RuntimeError("File-transfer daemon did not report a listening port")

    reachable_master_ip = master_ip_address or get_local_ip_for_peer(
        worker_ip_address
    )
    printlog(f"{PROGRESS_SENDING_FILE} ({worker_ip_address}, {part_filename})")
    try:
        send_ready_message(
            connection,
            reachable_master_ip,
            daemon.port,
            execute_command=execute_command,
        )
        receive_file_received_message(connection)
    except (ConnectionError, OSError) as exc:
        printlog(
            f"[WARNING] Failed to receive file confirmation from {worker_ip_address}: {exc}"
        )
    return daemon


def distribute_part_files_to_active_nodes(
    active_nodes: list[str],
    worker_pool: WorkerConnectionPool,
    parts_directory: str,
    master_ip_address: str | None = None,
    dashboard: StatusDashboard | None = None,
    config: Config | None = None,
) -> list[FileTransferDaemon]:
    """Start one file-transfer daemon per node and distribute part files in parallel."""
    daemons: list[FileTransferDaemon] = []
    pending_transfers: list[tuple[str, str, socket.socket, str, int, str | None]] = []

    for node_index, worker_ip_address in enumerate(active_nodes, start=1):
        part_filename = part_filename_for_node(node_index)
        daemon = start_file_transfer_daemon(
            allocated_filenames=[part_filename],
            serve_directory=parts_directory,
            output_directory=MASTER_OUTPUT_DIRECTORY,
        )
        daemons.append(daemon)
        if daemon.port is None:
            raise RuntimeError("File-transfer daemon did not report a listening port")

        connection = worker_pool.get_connection(worker_ip_address)
        reachable_master_ip = master_ip_address or get_local_ip_for_peer(
            worker_ip_address
        )
        worker_os = worker_pool.worker_telemetry.get(worker_ip_address, {}).get(
            "os_type"
        )
        execute_command = (
            config.get_execute_command(worker_os) if config else None
        )
        pending_transfers.append(
            (
                worker_ip_address,
                part_filename,
                connection,
                reachable_master_ip,
                daemon.port,
                execute_command,
            )
        )

    for (
        worker_ip_address,
        part_filename,
        connection,
        reachable_master_ip,
        file_transfer_port,
        execute_command,
    ) in pending_transfers:
        printlog(f"{PROGRESS_SENDING_FILE} ({worker_ip_address}, {part_filename})")
        if dashboard:
            dashboard.add_message(
                f"Sending {part_filename} to {worker_ip_address}",
                timeout_seconds=8,
            )
        try:
            send_ready_message(
                connection,
                reachable_master_ip,
                file_transfer_port,
                execute_command=execute_command,
            )
        except (ConnectionError, OSError) as exc:
            printlog(
                f"[WARNING] Failed to send ready message to {worker_ip_address}: {exc}"
            )

    for (
        worker_ip_address,
        part_filename,
        connection,
        _,
        _,
        _,
    ) in pending_transfers:
        try:
            receive_file_received_message(connection)
            if dashboard:
                dashboard.update_node_flags(
                    worker_ip_address,
                    receiving=False,
                    executing=True,
                    sending=False,
                )
        except (ConnectionError, OSError) as exc:
            printlog(
                f"[WARNING] Failed to receive file confirmation from {worker_ip_address}: {exc}"
            )

    return daemons
