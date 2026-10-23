"""Worker execution progress monitoring, failure detection, and output file collection."""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

from core.constants import (
    MASTER_OUTPUT_DIRECTORY,
    PROGRESS_EXECUTING,
    PROGRESS_FINISHED,
    PROGRESS_RECEIVING_FILES,
)
from core.dashboard import StatusDashboard, printlog
from core.file_transfer import FileTransferDaemon
from core.messaging import has_buffered_message, receive_json_message
from core.task_prep import part_filename_for_node
from master.binary_distributor import check_and_distribute_binaries
from master.connection_pool import safe_select_sockets
from master.failsafe import reassign_task_to_spare_node
from master.task_distributor import distribute_part_file_to_single_node

if TYPE_CHECKING:
    from core.config import Config
    from master.connection_pool import WorkerConnectionPool


def monitor_worker_executions_and_collect_results(
    active_nodes: list[str],
    worker_pool: WorkerConnectionPool,
    spare_nodes: list[str],
    parts_directory: str,
    master_ip_address: str | None = None,
    dashboard: StatusDashboard | None = None,
    config: Config | None = None,
    retry_timeout: float = 15.0,
) -> list[FileTransferDaemon]:
    """Track execution and result collection, supporting 1-attempt same-node retry before spare failover."""
    active_tasks: dict[str, dict] = {}
    node_retries: dict[str, int] = {}

    for node_index, worker_ip in enumerate(active_nodes, start=1):
        active_tasks[worker_ip] = {
            "node_index": node_index,
            "part_filename": part_filename_for_node(node_index),
            "connection": worker_pool.get_connection(worker_ip),
            "start_time": time.time(),
            "exec_finished": False,
            "file_received": False,
        }

    active_daemons: list[FileTransferDaemon] = []

    printlog("\n--- Starting Phase 3 Execution & Phase 4 Result Collection ---")

    while True:
        unfinished_ips = [
            ip for ip, task in active_tasks.items() if not task["file_received"]
        ]
        if not unfinished_ips:
            printlog("\nAll active worker outputs have been collected!")
            break

        for ip in unfinished_ips:
            task = active_tasks[ip]
            elapsed = time.time() - task["start_time"]
            node_id = f"node{task['node_index']}"
            if not task["exec_finished"]:
                minutes, secs = int(elapsed) // 60, int(elapsed) % 60
                printlog(
                    f"{PROGRESS_EXECUTING} ({ip}, {task['part_filename']}) - live running time: {minutes:02d}:{secs:02d}"
                )
                if dashboard:
                    dashboard.update_node(
                        ip,
                        node_id,
                        task["part_filename"],
                        PROGRESS_EXECUTING,
                        elapsed,
                        receiving=False,
                        executing=True,
                        sending=False,
                    )
            else:
                printlog(
                    f"{PROGRESS_RECEIVING_FILES} ({ip}, {task['part_filename']})"
                )
                if dashboard:
                    dashboard.update_node(
                        ip,
                        node_id,
                        task["part_filename"],
                        PROGRESS_RECEIVING_FILES,
                        receiving=True,
                        executing=False,
                        sending=False,
                    )

        # Check sockets for incoming finished/failed messages or disconnections
        socket_map = {
            task["connection"]: ip
            for ip, task in active_tasks.items()
            if not task["file_received"]
        }
        dead_socks = [
            s for s in socket_map if getattr(s, "fileno", lambda: -1)() < 0
        ]
        live_socks = [s for s in socket_map if s not in dead_socks]
        readable = safe_select_sockets(live_socks, 0.5)

        socks_to_check = [s for s in live_socks if has_buffered_message(s)]
        for s in readable:
            if s not in socks_to_check:
                socks_to_check.append(s)
        for s in dead_socks:
            if s not in socks_to_check:
                socks_to_check.append(s)

        for sock in socks_to_check:
            ip = socket_map[sock]
            task = active_tasks[ip]

            try:
                if (
                    sock in dead_socks
                    or getattr(sock, "fileno", lambda: -1)() < 0
                ):
                    raise ConnectionError("Worker control socket closed")
                msg = receive_json_message(sock)
                if msg.get("type") == "finished":
                    task["exec_finished"] = True
                    exec_time = msg.get("execution_time", 0.0)
                    printlog(
                        f"\n[EXECUTION COMPLETE] Node {ip} finished {task['part_filename']} in {exec_time:.2f}s"
                    )
                    printlog(
                        f"{PROGRESS_RECEIVING_FILES} ({ip}, {task['part_filename']})"
                    )
                    if dashboard:
                        dashboard.update_node(
                            ip,
                            f"node{task['node_index']}",
                            task["part_filename"],
                            PROGRESS_RECEIVING_FILES,
                            receiving=True,
                            executing=False,
                            sending=False,
                        )
                        dashboard.add_message(
                            f"Node {ip} finished {task['part_filename']} in {exec_time:.2f}s",
                            timeout_seconds=10,
                        )
                elif msg.get("type") == "failed":
                    raise ConnectionError(
                        f"Node reported failure: {msg.get('reason')}"
                    )
            except (ConnectionError, OSError, ValueError, KeyError) as exc:
                if not task["exec_finished"]:
                    printlog(
                        f"\n[FAILURE DETECTED] Node {ip} socket error/disconnect: {exc}"
                    )
                    worker_pool.remove_connection(ip)

                    node_index = task["node_index"]
                    part_filename = task["part_filename"]
                    del active_tasks[ip]
                    if dashboard:
                        dashboard.update_node(
                            ip,
                            f"node{node_index}",
                            "-",
                            "failed",
                            receiving=False,
                            executing=False,
                            sending=False,
                            connected=False,
                        )
                        dashboard.add_message(
                            f"ALERT: Node {ip} task failed ({exc})",
                            timeout_seconds=12,
                        )

                    retries = node_retries.get(ip, 0)
                    retry_succeeded = False
                    if retries < 1:
                        # 1 Retry attempt on the same node first
                        node_retries[ip] = retries + 1
                        printlog(
                            f"\n[RETRY 1/1] Task for node{node_index} failed on {ip}. Attempting retry 1/1 on same node {ip}..."
                        )

                        retry_conn = None
                        try:
                            if dashboard:
                                dashboard.add_message(
                                    f"Waiting for node {ip} to reconnect...",
                                    timeout_seconds=int(retry_timeout),
                                )
                            retry_conn = worker_pool.get_connection(
                                ip, timeout_seconds=retry_timeout
                            )

                        except TimeoutError:
                            printlog(
                                f"[RETRY FAILED] Node {ip} did not reconnect within timeout."
                            )

                        if retry_conn:
                            ready_nodes, _ = check_and_distribute_binaries(
                                [ip],
                                worker_pool,
                                config,
                                master_ip_address,
                                dashboard,
                            )
                            if ip not in ready_nodes:
                                printlog(
                                    f"[RETRY FAILED] Node {ip} failed binary handshake."
                                )
                            else:
                                _retry_os = worker_pool.worker_telemetry.get(
                                    ip, {}
                                ).get("os_type")
                                daemon = distribute_part_file_to_single_node(
                                    ip,
                                    part_filename,
                                    retry_conn,
                                    parts_directory,
                                    master_ip_address,
                                    execute_command=(
                                        config.get_execute_command(_retry_os)
                                        if config
                                        else None
                                    ),
                                )
                                active_daemons.append(daemon)
                                active_tasks[ip] = {
                                    "node_index": node_index,
                                    "part_filename": part_filename,
                                    "connection": retry_conn,
                                    "start_time": time.time(),
                                    "exec_finished": False,
                                    "file_received": False,
                                }
                                printlog(
                                    f"[RETRY 1/1] Resumed execution on same node {ip} ({part_filename})"
                                )
                                retry_succeeded = True
                                continue

                    if not retry_succeeded:
                        # Failover to spare node
                        spare_ip, spare_conn, daemon = (
                            reassign_task_to_spare_node(
                                ip,
                                node_index,
                                spare_nodes,
                                worker_pool,
                                parts_directory,
                                master_ip_address,
                                active_tasks=active_tasks,
                                dashboard=dashboard,
                                config=config,
                            )
                        )
                        active_daemons.append(daemon)
                        active_tasks[spare_ip] = {
                            "node_index": node_index,
                            "part_filename": part_filename,
                            "connection": spare_conn,
                            "start_time": time.time(),
                            "exec_finished": False,
                            "file_received": False,
                        }
                        printlog(
                            f"{PROGRESS_EXECUTING} resumed for spare worker {spare_ip} ({part_filename})"
                        )
                        continue

        # Check if received output files have arrived in master/output/
        for ip, task in list(active_tasks.items()):
            if task["exec_finished"] and not task["file_received"]:
                expected_output_file = (
                    Path(MASTER_OUTPUT_DIRECTORY) / task["part_filename"]
                )
                if expected_output_file.is_file():
                    task["file_received"] = True
                    printlog(
                        f"{PROGRESS_FINISHED} ({ip}, {task['part_filename']}) - result saved to {expected_output_file}"
                    )
                    if dashboard:
                        dashboard.update_node(
                            ip,
                            f"node{task['node_index']}",
                            task["part_filename"],
                            PROGRESS_FINISHED,
                            receiving=False,
                            executing=False,
                            sending=False,
                            connected=True,
                        )

        time.sleep(0.5)

    return active_daemons
