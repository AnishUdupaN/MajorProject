"""Fault tolerance, failover recovery, spare node reassignment, and crash recovery."""

from __future__ import annotations

import select
import socket
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

from core.benchmark import rank_workers_by_affinity
from core.constants import (
    MASTER_OUTPUT_DIRECTORY,
    PROGRESS_FINISHED,
    PROGRESS_RECEIVING_FILES,
)
from core.dashboard import StatusDashboard, printlog
from core.devices import swap_device_ip
from core.file_transfer import FileTransferDaemon
from core.messaging import receive_json_message
from core.task_prep import part_filename_for_node
from master.binary_distributor import check_and_distribute_binaries
from master.connection_pool import safe_select_sockets
from master.task_distributor import distribute_part_file_to_single_node

if TYPE_CHECKING:
    from core.config import Config
    from master.connection_pool import WorkerConnectionPool


def handle_no_spare_nodes_recovery(
    failed_ip: str,
    node_index: int,
    part_filename: str,
    spare_nodes: list[str],
    worker_pool: WorkerConnectionPool,
    active_tasks: dict[str, dict],
    parts_directory: str,
    master_ip_address: str | None = None,
    dashboard: StatusDashboard | None = None,
    config: Config | None = None,
) -> tuple[str, socket.socket, FileTransferDaemon]:
    """When no spare nodes are available, ask user to (k)ill task, (w)ait 1 minute for new nodes, (r)eassign to a healthy node when finished, or (b)oth."""
    printlog(
        f"\n[RECOVERY ALERT] Node {failed_ip} failed and no spare nodes are available for recovery."
    )

    while True:
        if dashboard:
            dashboard.add_message(
                f"ALERT: Node {failed_ip} failed! No spare nodes. Press 'k' to kill, 'w' to wait 2m, 'r' to reassign, 'b' for both.",
                timeout_seconds=60,
            )

        prompt_text = (
            f"\n==================================================================\n"
            f"  NO RECOVERY NODES AVAILABLE FOR NODE{node_index} ({failed_ip})  \n"
            f"==================================================================\n"
            "One of the active devices has failed and no spare nodes are connected.\n"
            "Options:\n"
            "  [k] Kill task: Safely terminate running tasks on healthy nodes and exit.\n"
            "  [w] Wait 2 minutes: Keep healthy nodes running and wait for a new node to connect.\n"
            "  [r] Reassign when finished: Wait until a healthy node finishes its task and reassign this task to it.\n"
            "  [b] Both: Listen for new nodes for 2 minutes; if none found, fallback to reassigning to first finished node.\n"
            "Enter choice (k/w/r/b): "
        )
        if dashboard:
            dashboard.set_prompt(prompt_text)
        else:
            sys.stdout.write(prompt_text)
            sys.stdout.flush()

        choice = ""
        if dashboard and getattr(dashboard, "auto_confirm", False):
            choice = "r"
            print(
                "\n[AUTO-REASSIGN] Auto-confirm (-y) enabled: selecting 'r' (reassign when finished)."
            )
            printlog(
                "\n[AUTO-REASSIGN] Auto-confirm (-y) enabled: selecting 'r' (reassign when finished)."
            )

        fd = None
        old_settings = None
        if not choice and sys.stdin.isatty():
            try:
                import termios
                import tty

                fd = sys.stdin.fileno()
                if fd < 0:
                    fd = None
                else:
                    old_settings = termios.tcgetattr(fd)
                    tty.setcbreak(fd)
            except Exception:
                fd = None
                old_settings = None

        try:
            while not choice:
                if dashboard and getattr(dashboard, "webserver_mode", False):
                    dashboard.pending_input_event.clear()
                    dashboard.pending_input_event.wait(0.5)
                    ans = dashboard.pending_input_answer
                    if ans in ("k", "w", "r", "b"):
                        choice = ans
                        dashboard.pending_input_answer = None
                        break
                if fd is not None and fd >= 0:
                    try:
                        rlist, _, _ = select.select([sys.stdin], [], [], 0.5)
                    except (ValueError, OSError):
                        rlist = []
                    if rlist:
                        char = sys.stdin.read(1).lower()
                        if char in ("k", "w", "r", "b"):
                            choice = char
                            print(char)
                            break
                else:
                    try:
                        line = sys.stdin.readline()
                        if not line:
                            choice = "r"
                            break
                        line = line.strip().lower()
                        if line:
                            if line.startswith("k"):
                                choice = "k"
                            elif line.startswith("w"):
                                choice = "w"
                            elif line.startswith("r"):
                                choice = "r"
                            elif line.startswith("b"):
                                choice = "b"
                            break
                    except Exception:
                        choice = "r"
                        break
                time.sleep(0.1)
        finally:
            if fd is not None and old_settings is not None:
                try:
                    import termios

                    termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                except Exception:
                    pass

        if dashboard:
            dashboard.set_prompt(None)

        if choice == "k":
            print(
                "\nUser selected to KILL the task. Shutting down all healthy worker nodes..."
            )
            printlog(
                f"\nUser selected 'k' (kill task) after failure of node {failed_ip}."
            )
            raise RuntimeError(
                "Task execution killed by user due to node failure and lack of recovery nodes."
            )

        elif choice == "w":
            print(
                "\nUser selected to WAIT. Waiting up to 2 minutes for a new node to connect..."
            )
            printlog(
                f"\nUser selected 'w' (wait 2m) after failure of node {failed_ip}."
            )
            wait_start = time.time()
            wait_timeout = 120.0
            found_spare_ip = None

            while time.time() - wait_start < wait_timeout:
                elapsed_wait = int(time.time() - wait_start)
                remaining = int(wait_timeout - elapsed_wait)

                idle_ips = set()
                with worker_pool.lock:
                    for k in list(worker_pool.connections.keys()):
                        if (
                            k not in active_tasks
                            and k not in worker_pool.killed_ips
                            and k != failed_ip
                        ):
                            idle_ips.add(k)
                if idle_ips:
                    resource_type = config.resource_type if config else "auto"
                    idle_telemetry = {
                        k: worker_pool.worker_telemetry.get(k, {})
                        for k in idle_ips
                    }
                    ranked_ips = rank_workers_by_affinity(
                        idle_telemetry, resource_type
                    )
                    if ranked_ips:
                        found_spare_ip = ranked_ips[0]

                if found_spare_ip:
                    print(
                        f"\n[NEW NODE CONNECTED] Discovered new node {found_spare_ip}!"
                    )
                    printlog(
                        f"[FAILOVER] New node {found_spare_ip} connected during wait period."
                    )
                    break

                if dashboard:
                    dashboard.add_message(
                        f"Waiting for new nodes... ({remaining}s remaining). Press 'k' to cancel & kill.",
                        timeout_seconds=2,
                    )

                if sys.stdin.isatty():
                    try:
                        rlist, _, _ = select.select([sys.stdin], [], [], 0.0)
                    except (ValueError, OSError):
                        rlist = []
                    if rlist:
                        char = sys.stdin.read(1).lower()
                        if char == "k":
                            print(
                                "\nUser pressed 'k' during wait. Shutting down task..."
                            )
                            raise RuntimeError(
                                "Task execution killed by user during node wait period."
                            )

                for active_ip, task in list(active_tasks.items()):
                    if task["exec_finished"] and not task["file_received"]:
                        expected_output_file = (
                            Path(MASTER_OUTPUT_DIRECTORY)
                            / task["part_filename"]
                        )
                        if expected_output_file.is_file():
                            task["file_received"] = True
                            printlog(
                                f"{PROGRESS_FINISHED} ({active_ip}, {task['part_filename']}) - result saved to {expected_output_file}"
                            )

                time.sleep(0.5)

            if found_spare_ip:
                updated_mapping = swap_device_ip(failed_ip, found_spare_ip)
                printlog(f"[FAILOVER] Updated devices.json: {updated_mapping}")

                spare_connection = worker_pool.get_connection(found_spare_ip)
                printlog(
                    f"[FAILOVER] Reusing existing split file {part_filename} for spare node {found_spare_ip}"
                )
                _spare_os = worker_pool.worker_telemetry.get(
                    found_spare_ip, {}
                ).get("os_type")
                daemon = distribute_part_file_to_single_node(
                    found_spare_ip,
                    part_filename,
                    spare_connection,
                    parts_directory,
                    master_ip_address,
                    execute_command=(
                        config.get_execute_command(_spare_os) if config else None
                    ),
                )
                return found_spare_ip, spare_connection, daemon
            else:
                print("\n[TIMEOUT] 60 seconds elapsed and no new nodes connected.")
                printlog("[RECOVERY TIMEOUT] 60s elapsed without new nodes.")

        elif choice == "r":
            print(
                "\nUser selected to REASSIGN. Waiting for a healthy node to complete its current task..."
            )
            printlog(
                f"\nUser selected 'r' (reassign when finished) after failure of node {failed_ip}."
            )

            reassigned_ip = None
            reassigned_conn = None

            while True:
                healthy_candidates = [
                    ip for ip in active_tasks if ip != failed_ip
                ]
                if not healthy_candidates:
                    print(
                        "\n[ERROR] No healthy running nodes available to take over the task."
                    )
                    printlog("[REASSIGN ERROR] No healthy nodes available.")
                    break

                for healthy_ip in healthy_candidates:
                    t = active_tasks[healthy_ip]
                    if t.get("exec_finished") and t.get("file_received"):
                        reassigned_ip = healthy_ip
                        break

                if reassigned_ip:
                    break

                if dashboard:
                    dashboard.add_message(
                        f"Waiting for a healthy node to finish so it can take over {part_filename}... Press 'k' to cancel & kill.",
                        timeout_seconds=2,
                    )

                if sys.stdin.isatty():
                    try:
                        rlist, _, _ = select.select([sys.stdin], [], [], 0.0)
                    except (ValueError, OSError):
                        rlist = []
                    if rlist:
                        char = sys.stdin.read(1).lower()
                        if char == "k":
                            print(
                                "\nUser pressed 'k' during wait. Shutting down task..."
                            )
                            raise RuntimeError(
                                "Task execution killed by user during node wait period."
                            )

                socket_map = {
                    t["connection"]: a_ip
                    for a_ip, t in active_tasks.items()
                    if a_ip != failed_ip and not t.get("exec_finished")
                }
                if socket_map:
                    readable = safe_select_sockets(list(socket_map.keys()), 0.5)
                    for sock in readable:
                        a_ip = socket_map[sock]
                        t = active_tasks[a_ip]
                        try:
                            msg = receive_json_message(sock)
                            if msg.get("type") == "finished":
                                t["exec_finished"] = True
                                exec_time = msg.get("execution_time", 0.0)
                                printlog(
                                    f"[EXECUTION COMPLETE] Node {a_ip} finished {t['part_filename']} in {exec_time:.2f}s"
                                )
                                if dashboard:
                                    dashboard.update_node(
                                        a_ip,
                                        f"node{t['node_index']}",
                                        t["part_filename"],
                                        PROGRESS_RECEIVING_FILES,
                                        receiving=True,
                                        executing=False,
                                        sending=False,
                                    )
                                    dashboard.add_message(
                                        f"Node {a_ip} finished {t['part_filename']} in {exec_time:.2f}s",
                                        timeout_seconds=10,
                                    )
                            elif msg.get("type") == "failed":
                                printlog(
                                    f"[NODE FAILURE] Healthy candidate {a_ip} failed during wait."
                                )
                        except Exception as exc:
                            printlog(
                                f"[SOCKET ERROR] Node {a_ip} error during wait: {exc}"
                            )

                for active_ip, task in list(active_tasks.items()):
                    if task.get("exec_finished") and not task.get(
                        "file_received"
                    ):
                        expected_output_file = (
                            Path(MASTER_OUTPUT_DIRECTORY)
                            / task["part_filename"]
                        )
                        if expected_output_file.is_file():
                            task["file_received"] = True
                            printlog(
                                f"{PROGRESS_FINISHED} ({active_ip}, {task['part_filename']}) - result saved to {expected_output_file}"
                            )
                            if dashboard:
                                dashboard.update_node(
                                    active_ip,
                                    f"node{task['node_index']}",
                                    task["part_filename"],
                                    PROGRESS_FINISHED,
                                    receiving=False,
                                    executing=False,
                                    sending=False,
                                    connected=True,
                                )

                time.sleep(0.2)

            if reassigned_ip:
                print(
                    f"\n[TASK REASSIGNMENT] Node {reassigned_ip} finished its task and is taking over {part_filename}!"
                )
                printlog(
                    f"[REASSIGNMENT] Reassigning {part_filename} (node{node_index}) to healthy node {reassigned_ip}."
                )

                try:
                    updated_mapping = swap_device_ip(failed_ip, reassigned_ip)
                    printlog(f"[FAILOVER] Updated devices.json: {updated_mapping}")
                except Exception:
                    pass

                reassigned_conn = worker_pool.get_connection(reassigned_ip)
                if dashboard:
                    dashboard.update_node(
                        reassigned_ip,
                        f"node{node_index}",
                        part_filename,
                        "reassigned",
                        receiving=False,
                        executing=False,
                        sending=False,
                        connected=True,
                    )

                _reassigned_os = worker_pool.worker_telemetry.get(
                    reassigned_ip, {}
                ).get("os_type")
                daemon = distribute_part_file_to_single_node(
                    reassigned_ip,
                    part_filename,
                    reassigned_conn,
                    parts_directory,
                    master_ip_address,
                    execute_command=(
                        config.get_execute_command(_reassigned_os)
                        if config
                        else None
                    ),
                )
                return reassigned_ip, reassigned_conn, daemon

        elif choice == "b":
            print(
                "\nUser selected BOTH. Listening up to 2 minutes for new nodes or waiting for healthy node to finish..."
            )
            printlog(f"\nUser selected 'b' (both) after failure of node {failed_ip}.")

            wait_start = time.time()
            wait_timeout = 120.0
            found_spare_ip = None
            reassigned_ip = None

            while time.time() - wait_start < wait_timeout:
                elapsed_wait = int(time.time() - wait_start)
                remaining = int(wait_timeout - elapsed_wait)

                idle_ips = set()
                with worker_pool.lock:
                    for k in list(worker_pool.connections.keys()):
                        if (
                            k not in active_tasks
                            and k not in worker_pool.killed_ips
                            and k != failed_ip
                        ):
                            idle_ips.add(k)
                if idle_ips:
                    resource_type = config.resource_type if config else "auto"
                    idle_telemetry = {
                        k: worker_pool.worker_telemetry.get(k, {})
                        for k in idle_ips
                    }
                    ranked_ips = rank_workers_by_affinity(
                        idle_telemetry, resource_type
                    )
                    if ranked_ips:
                        found_spare_ip = ranked_ips[0]

                if found_spare_ip:
                    print(
                        f"\n[NEW NODE CONNECTED] Discovered new node {found_spare_ip}!"
                    )
                    printlog(
                        f"[FAILOVER] New node {found_spare_ip} connected during both-wait period."
                    )
                    break

                healthy_candidates = [
                    ip for ip in active_tasks if ip != failed_ip
                ]
                for healthy_ip in healthy_candidates:
                    t = active_tasks[healthy_ip]
                    if t.get("exec_finished") and t.get("file_received"):
                        reassigned_ip = healthy_ip
                        break

                if reassigned_ip:
                    print(
                        f"\n[HEALTHY NODE FINISHED] Healthy node {reassigned_ip} finished its task!"
                    )
                    printlog(
                        f"[FAILOVER] Healthy node {reassigned_ip} finished during both-wait period."
                    )
                    break

                if dashboard:
                    dashboard.add_message(
                        f"Listening for new nodes or healthy node completion ({remaining}s remaining)... Press 'k' to kill.",
                        timeout_seconds=2,
                    )

                if sys.stdin.isatty():
                    try:
                        rlist, _, _ = select.select([sys.stdin], [], [], 0.0)
                    except (ValueError, OSError):
                        rlist = []
                    if rlist:
                        char = sys.stdin.read(1).lower()
                        if char == "k":
                            print(
                                "\nUser pressed 'k' during wait. Shutting down task..."
                            )
                            raise RuntimeError(
                                "Task execution killed by user during node wait period."
                            )

                socket_map = {
                    t["connection"]: a_ip
                    for a_ip, t in active_tasks.items()
                    if a_ip != failed_ip and not t.get("exec_finished")
                }
                if socket_map:
                    readable = safe_select_sockets(list(socket_map.keys()), 0.5)
                    for sock in readable:
                        a_ip = socket_map[sock]
                        t = active_tasks[a_ip]
                        try:
                            msg = receive_json_message(sock)
                            if msg.get("type") == "finished":
                                t["exec_finished"] = True
                                exec_time = msg.get("execution_time", 0.0)
                                printlog(
                                    f"[EXECUTION COMPLETE] Node {a_ip} finished {t['part_filename']} in {exec_time:.2f}s"
                                )
                                if dashboard:
                                    dashboard.update_node(
                                        a_ip,
                                        f"node{t['node_index']}",
                                        t["part_filename"],
                                        PROGRESS_RECEIVING_FILES,
                                        receiving=True,
                                        executing=False,
                                        sending=False,
                                    )
                        except Exception:
                            pass

                for active_ip, task in list(active_tasks.items()):
                    if task.get("exec_finished") and not task.get(
                        "file_received"
                    ):
                        expected_output_file = (
                            Path(MASTER_OUTPUT_DIRECTORY)
                            / task["part_filename"]
                        )
                        if expected_output_file.is_file():
                            task["file_received"] = True
                            printlog(
                                f"{PROGRESS_FINISHED} ({active_ip}, {task['part_filename']}) - result saved to {expected_output_file}"
                            )
                            if dashboard:
                                dashboard.update_node(
                                    active_ip,
                                    f"node{task['node_index']}",
                                    task["part_filename"],
                                    PROGRESS_FINISHED,
                                    receiving=False,
                                    executing=False,
                                    sending=False,
                                    connected=True,
                                )

                time.sleep(0.2)

            if not found_spare_ip and not reassigned_ip:
                print(
                    "\n[TIMEOUT] 60s elapsed with no new nodes. Falling back to waiting for healthy node to finish..."
                )
                printlog(
                    "[RECOVERY FALLBACK] Falling back to waiting for healthy node to finish."
                )

                while True:
                    healthy_candidates = [
                        ip for ip in active_tasks if ip != failed_ip
                    ]
                    if not healthy_candidates:
                        print(
                            "\n[ERROR] No healthy running nodes available to take over the task."
                        )
                        break

                    for healthy_ip in healthy_candidates:
                        t = active_tasks[healthy_ip]
                        if t.get("exec_finished") and t.get("file_received"):
                            reassigned_ip = healthy_ip
                            break

                    if reassigned_ip:
                        break

                    if dashboard:
                        dashboard.add_message(
                            f"Fallback: Waiting for healthy node to finish {part_filename}... Press 'k' to kill.",
                            timeout_seconds=2,
                        )

                    if sys.stdin.isatty():
                        try:
                            rlist, _, _ = select.select([sys.stdin], [], [], 0.0)
                        except (ValueError, OSError):
                            rlist = []
                        if rlist:
                            char = sys.stdin.read(1).lower()
                            if char == "k":
                                print(
                                    "\nUser pressed 'k' during wait. Shutting down task..."
                                )
                                raise RuntimeError(
                                    "Task execution killed by user during node wait period."
                                )

                    socket_map = {
                        t["connection"]: a_ip
                        for a_ip, t in active_tasks.items()
                        if a_ip != failed_ip and not t.get("exec_finished")
                    }
                    if socket_map:
                        readable = safe_select_sockets(list(socket_map.keys()), 0.5)
                        for sock in readable:
                            a_ip = socket_map[sock]
                            t = active_tasks[a_ip]
                            try:
                                msg = receive_json_message(sock)
                                if msg.get("type") == "finished":
                                    t["exec_finished"] = True
                            except Exception:
                                pass

                    for active_ip, task in list(active_tasks.items()):
                        if task.get("exec_finished") and not task.get(
                            "file_received"
                        ):
                            expected_output_file = (
                                Path(MASTER_OUTPUT_DIRECTORY)
                                / task["part_filename"]
                            )
                            if expected_output_file.is_file():
                                task["file_received"] = True
                                printlog(
                                    f"{PROGRESS_FINISHED} ({active_ip}, {task['part_filename']}) - result saved to {expected_output_file}"
                                )
                                if dashboard:
                                    dashboard.update_node(
                                        active_ip,
                                        f"node{task['node_index']}",
                                        task["part_filename"],
                                        PROGRESS_FINISHED,
                                        receiving=False,
                                        executing=False,
                                        sending=False,
                                        connected=True,
                                    )

                    time.sleep(0.2)

            if found_spare_ip:
                updated_mapping = swap_device_ip(failed_ip, found_spare_ip)
                printlog(f"[FAILOVER] Updated devices.json: {updated_mapping}")
                spare_connection = worker_pool.get_connection(found_spare_ip)
                _spare_os = worker_pool.worker_telemetry.get(
                    found_spare_ip, {}
                ).get("os_type")
                daemon = distribute_part_file_to_single_node(
                    found_spare_ip,
                    part_filename,
                    spare_connection,
                    parts_directory,
                    master_ip_address,
                    execute_command=(
                        config.get_execute_command(_spare_os) if config else None
                    ),
                )
                return found_spare_ip, spare_connection, daemon
            elif reassigned_ip:
                print(
                    f"\n[TASK REASSIGNMENT] Node {reassigned_ip} finished its task and is taking over {part_filename}!"
                )
                printlog(
                    f"[REASSIGNMENT] Reassigning {part_filename} (node{node_index}) to healthy node {reassigned_ip}."
                )
                try:
                    updated_mapping = swap_device_ip(failed_ip, reassigned_ip)
                except Exception:
                    pass
                reassigned_conn = worker_pool.get_connection(reassigned_ip)
                if dashboard:
                    dashboard.update_node(
                        reassigned_ip,
                        f"node{node_index}",
                        part_filename,
                        "reassigned",
                        receiving=False,
                        executing=False,
                        sending=False,
                        connected=True,
                    )
                _reassigned_os = worker_pool.worker_telemetry.get(
                    reassigned_ip, {}
                ).get("os_type")
                daemon = distribute_part_file_to_single_node(
                    reassigned_ip,
                    part_filename,
                    reassigned_conn,
                    parts_directory,
                    master_ip_address,
                    execute_command=(
                        config.get_execute_command(_reassigned_os)
                        if config
                        else None
                    ),
                )
                return reassigned_ip, reassigned_conn, daemon


def reassign_task_to_spare_node(
    failed_ip: str,
    node_index: int,
    spare_nodes: list[str],
    worker_pool: WorkerConnectionPool,
    parts_directory: str,
    master_ip_address: str | None = None,
    active_tasks: dict[str, dict] | None = None,
    dashboard: StatusDashboard | None = None,
    config: Config | None = None,
) -> tuple[str, socket.socket, FileTransferDaemon]:
    """Swap IP in devices.json and reassign task to the next spare node or prompt user if none available."""
    idle_ips = set()
    if active_tasks is not None:
        with worker_pool.lock:
            for ip in list(worker_pool.connections.keys()):
                if (
                    ip != failed_ip
                    and ip not in active_tasks
                    and ip not in worker_pool.killed_ips
                ):
                    idle_ips.add(ip)

    for ip in list(spare_nodes):
        if (
            ip != failed_ip
            and (active_tasks is None or ip not in active_tasks)
            and ip not in worker_pool.killed_ips
        ):
            idle_ips.add(ip)

    spare_ip = None
    if idle_ips:
        resource_type = config.resource_type if config else "auto"
        idle_telemetry = {
            ip: worker_pool.worker_telemetry.get(ip, {}) for ip in idle_ips
        }
        ranked_ips = rank_workers_by_affinity(idle_telemetry, resource_type)
        if ranked_ips:
            spare_ip = ranked_ips[0]
            if spare_ip in spare_nodes:
                spare_nodes.remove(spare_ip)

    if spare_ip:
        printlog(
            f"\n Node {failed_ip} failed. Reassigning task to node{node_index}, {spare_ip}"
        )
        worker_pool.mark_killed(failed_ip)
        updated_mapping = swap_device_ip(failed_ip, spare_ip)
        printlog(f"[FAILOVER] Updated devices.json: {updated_mapping}")

        if dashboard:
            dashboard.update_node(
                failed_ip,
                f"node{node_index} (killed)",
                "-",
                "killed",
                receiving=False,
                executing=False,
                sending=False,
                connected=False,
            )

        spare_connection = worker_pool.get_connection(spare_ip)

        # Binary check for spare node
        ready_nodes, _ = check_and_distribute_binaries(
            [spare_ip], worker_pool, config, master_ip_address, dashboard
        )
        if spare_ip not in ready_nodes:
            printlog(
                f"[FAILOVER] Spare node {spare_ip} failed binary handshake."
            )
            return handle_no_spare_nodes_recovery(
                failed_ip,
                node_index,
                part_filename_for_node(node_index),
                spare_nodes,
                worker_pool,
                active_tasks if active_tasks is not None else {},
                parts_directory,
                master_ip_address,
                dashboard,
                config=config,
            )

        part_filename = part_filename_for_node(node_index)

        if dashboard:
            dashboard.update_node(
                spare_ip,
                f"node{node_index}",
                part_filename,
                "assigned",
                receiving=False,
                executing=False,
                sending=False,
                connected=True,
            )

        printlog(
            f"[FAILOVER] Reusing existing split file {part_filename} for spare node {spare_ip}"
        )
        _spare_os = worker_pool.worker_telemetry.get(spare_ip, {}).get("os_type")
        daemon = distribute_part_file_to_single_node(
            spare_ip,
            part_filename,
            spare_connection,
            parts_directory,
            master_ip_address,
            execute_command=(
                config.get_execute_command(_spare_os) if config else None
            ),
        )
        return spare_ip, spare_connection, daemon
    else:
        return handle_no_spare_nodes_recovery(
            failed_ip,
            node_index,
            part_filename_for_node(node_index),
            spare_nodes,
            worker_pool,
            active_tasks if active_tasks is not None else {},
            parts_directory,
            master_ip_address,
            dashboard,
            config=config,
        )
