"""Master node entry point: load config, validate addresses, split video, distribute parts, track execution with same-node retries & dynamic pooling, collect results, and merge."""

import argparse
import select
import socket
import sys
import threading
import time
from pathlib import Path

from config import load_config
from constants import (
    CONTROL_PORT,
    FIXED_PORT,
    MASTER_INPUT_DIRECTORY,
    MASTER_OUTPUT_DIRECTORY,
    PROGRESS_EXECUTING,
    PROGRESS_FINISHED,
    PROGRESS_RECEIVING_FILES,
    PROGRESS_SENDING_FILE,
)
from control_messages import (
    has_buffered_message,
    receive_file_received_message,
    receive_json_message,
    send_ready_message,
    send_shutdown_message,
)

from devices import swap_device_ip, write_devices_json
from file_transfer_daemon import FileTransferDaemon, start_file_transfer_daemon
from merge import run_merge_command
from network import get_local_ip_for_peer
from split import part_filename_for_node, run_split_command, verify_part_files
from ui import StatusDashboard


class WorkerConnectionPool:
    """Thread-safe pool accepting worker connections dynamically at any time."""

    def __init__(
        self,
        allowed_ips: list[str],
        listening_socket: socket.socket,
        dashboard: StatusDashboard | None = None,
    ) -> None:
        self.allowed_ips = set(allowed_ips)
        self.listening_socket = listening_socket
        self.dashboard = dashboard
        self.connections: dict[str, socket.socket] = {}
        self.lock = threading.Lock()
        self.running = True
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def _accept_loop(self) -> None:
        while self.running:
            try:
                r, _, _ = select.select([self.listening_socket], [], [], 0.5)
                if not r:
                    continue
                conn, addr = self.listening_socket.accept()
                ip = addr[0]
                if ip not in self.allowed_ips:
                    conn.close()
                    continue
                with self.lock:
                    old_conn = self.connections.get(ip)
                    if old_conn is not None and old_conn != conn:
                        try:
                            rlist, _, _ = select.select([old_conn], [], [], 0.0)
                            if rlist:
                                peek = old_conn.recv(1, socket.MSG_PEEK)
                                if not peek:
                                    old_conn.close()
                        except Exception:
                            pass
                    self.connections[ip] = conn
                    print(f"\n[POOL] Worker {ip}:{addr[1]} connected and kept in IDLE pool.")
                    if self.dashboard:
                        existing = self.dashboard.node_states.get(ip)
                        if not existing or existing.get("node_id") == "worker":
                            self.dashboard.update_node(ip, "worker", "-", "idle")
            except Exception:
                pass



    def get_connection(self, ip: str, timeout_seconds: float = 30.0) -> socket.socket:
        """Fetch connection for ip from pool, waiting if not yet connected."""
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            with self.lock:
                conn = self.connections.get(ip)
                if conn is not None:
                    return conn
            time.sleep(0.2)
        raise TimeoutError(f"Worker {ip} did not connect within {timeout_seconds}s")

    def remove_connection(self, ip: str) -> None:
        """Remove a dead or closed socket connection for ip from the pool."""
        with self.lock:
            conn = self.connections.pop(ip, None)
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def shutdown_all_workers(self) -> None:
        """Broadcast shutdown message to all connected workers in the pool."""
        with self.lock:
            for ip, conn in list(self.connections.items()):
                try:
                    send_shutdown_message(conn)
                except Exception:
                    pass

    def stop(self) -> None:
        self.running = False



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


def distribute_part_file_to_single_node(
    worker_ip_address: str,
    part_filename: str,
    connection: socket.socket,
    parts_directory: str,
    master_ip_address: str | None = None,
) -> FileTransferDaemon:
    """Start file-transfer daemon and send ready message to a single worker node."""
    daemon = start_file_transfer_daemon(
        allocated_filenames=[part_filename],
        serve_directory=parts_directory,
        output_directory=MASTER_OUTPUT_DIRECTORY,
    )
    if daemon.port is None:
        raise RuntimeError("File-transfer daemon did not report a listening port")

    reachable_master_ip = master_ip_address or get_local_ip_for_peer(worker_ip_address)
    print(f"{PROGRESS_SENDING_FILE} ({worker_ip_address}, {part_filename})")
    send_ready_message(connection, reachable_master_ip, daemon.port)
    receive_file_received_message(connection)
    print(f"Received {part_filename} confirmation from {worker_ip_address}")
    return daemon


def distribute_part_files_to_active_nodes(
    active_nodes: list[str],
    worker_pool: WorkerConnectionPool,
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
            output_directory=MASTER_OUTPUT_DIRECTORY,
        )
        daemons.append(daemon)
        if daemon.port is None:
            raise RuntimeError("File-transfer daemon did not report a listening port")

        connection = worker_pool.get_connection(worker_ip_address)
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

    for (
        worker_ip_address,
        part_filename,
        connection,
        reachable_master_ip,
        file_transfer_port,
    ) in pending_transfers:
        print(f"{PROGRESS_SENDING_FILE} ({worker_ip_address}, {part_filename})")
        send_ready_message(connection, reachable_master_ip, file_transfer_port)

    for worker_ip_address, part_filename, connection, _, _ in pending_transfers:
        receive_file_received_message(connection)
        print(f"Received {part_filename} confirmation from {worker_ip_address}")

    return daemons


def reassign_task_to_spare_node(
    failed_ip: str,
    node_index: int,
    spare_nodes: list[str],
    worker_pool: WorkerConnectionPool,
    parts_directory: str,
    master_ip_address: str | None = None,
) -> tuple[str, socket.socket, FileTransferDaemon]:
    """Swap IP in devices.json and reassign task to the next spare node using existing split file."""
    if not spare_nodes:
        raise RuntimeError(
            f"Node {failed_ip} failed and no spare nodes are available for reassignment!"
        )

    spare_ip = spare_nodes.pop(0)
    print(
        f"\n[FAILOVER] Node {failed_ip} failed 2 times. Reassigning task for node{node_index} to spare node {spare_ip}..."
    )

    updated_mapping = swap_device_ip(failed_ip, spare_ip)
    print(f"[FAILOVER] Updated devices.json: {updated_mapping}")

    spare_connection = worker_pool.get_connection(spare_ip)
    part_filename = part_filename_for_node(node_index)

    print(f"[FAILOVER] Reusing existing split file {part_filename} for spare node {spare_ip}")
    daemon = distribute_part_file_to_single_node(
        spare_ip,
        part_filename,
        spare_connection,
        parts_directory,
        master_ip_address,
    )
    return spare_ip, spare_connection, daemon


def monitor_worker_executions_and_collect_results(
    active_nodes: list[str],
    worker_pool: WorkerConnectionPool,
    spare_nodes: list[str],
    parts_directory: str,
    master_ip_address: str | None = None,
    dashboard: StatusDashboard | None = None,
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

    print("\n--- Starting Phase 3 Execution & Phase 4 Result Collection ---")

    while True:
        unfinished_ips = [ip for ip, task in active_tasks.items() if not task["file_received"]]
        if not unfinished_ips:
            print("\nAll active worker outputs have been collected!")
            break

        for ip in unfinished_ips:
            task = active_tasks[ip]
            elapsed = time.time() - task["start_time"]
            node_id = f"node{task['node_index']}"
            if not task["exec_finished"]:
                minutes, secs = int(elapsed) // 60, int(elapsed) % 60
                print(
                    f"{PROGRESS_EXECUTING} ({ip}, {task['part_filename']}) - live running time: {minutes:02d}:{secs:02d}"
                )
                if dashboard:
                    dashboard.update_node(
                        ip, node_id, task["part_filename"], PROGRESS_EXECUTING, elapsed
                    )
            else:
                print(f"{PROGRESS_RECEIVING_FILES} ({ip}, {task['part_filename']})")
                if dashboard:
                    dashboard.update_node(
                        ip, node_id, task["part_filename"], PROGRESS_RECEIVING_FILES
                    )

        # Check sockets for incoming finished/failed messages or disconnections
        socket_map = {
            task["connection"]: ip
            for ip, task in active_tasks.items()
            if not task["file_received"]
        }
        readable, _, _ = select.select(list(socket_map.keys()), [], [], 0.5)

        socks_to_check = [s for s in socket_map if has_buffered_message(s)]
        for s in readable:
            if s not in socks_to_check:
                socks_to_check.append(s)

        for sock in socks_to_check:
            ip = socket_map[sock]
            task = active_tasks[ip]


            try:
                msg = receive_json_message(sock)
                if msg.get("type") == "finished":
                    task["exec_finished"] = True
                    exec_time = msg.get("execution_time", 0.0)
                    print(
                        f"\n[EXECUTION COMPLETE] Node {ip} finished {task['part_filename']} in {exec_time:.2f}s"
                    )
                    print(f"{PROGRESS_RECEIVING_FILES} ({ip}, {task['part_filename']})")
                    if dashboard:
                        dashboard.update_node(
                            ip, f"node{task['node_index']}", task["part_filename"], PROGRESS_RECEIVING_FILES
                        )
                elif msg.get("type") == "failed":
                    raise ConnectionError(f"Node reported failure: {msg.get('reason')}")
            except (ConnectionError, OSError, ValueError, KeyError) as exc:
                if not task["exec_finished"]:
                    print(f"\n[FAILURE DETECTED] Node {ip} socket error/disconnect: {exc}")
                    worker_pool.remove_connection(ip)

                    node_index = task["node_index"]
                    part_filename = task["part_filename"]
                    del active_tasks[ip]
                    if dashboard:
                        dashboard.update_node(ip, f"node{node_index}", "-", "idle")



                    retries = node_retries.get(ip, 0)
                    if retries < 1:
                        # 1 Retry attempt on the same node first
                        node_retries[ip] = retries + 1
                        print(
                            f"\n[RETRY 1/1] Task for node{node_index} failed on {ip}. Attempting retry 1/1 on same node {ip}..."
                        )

                        retry_conn = None
                        try:
                            retry_conn = worker_pool.get_connection(ip, timeout_seconds=8.0)
                        except TimeoutError:
                            print(f"[RETRY FAILED] Node {ip} did not reconnect within timeout.")

                        if retry_conn:
                            daemon = distribute_part_file_to_single_node(
                                ip,
                                part_filename,
                                retry_conn,
                                parts_directory,
                                master_ip_address,
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
                            print(f"[RETRY 1/1] Resumed execution on same node {ip} ({part_filename})")
                            continue

                    # If retry attempt failed or retries >= 1, trigger failover to spare node
                    spare_ip, spare_conn, daemon = reassign_task_to_spare_node(
                        ip,
                        node_index,
                        spare_nodes,
                        worker_pool,
                        parts_directory,
                        master_ip_address,
                    )
                    active_daemons.append(daemon)
                    active_tasks[spare_ip] = {
                        "node_index": node_index,
                        "part_filename": part_filename_for_node(node_index),
                        "connection": spare_conn,
                        "start_time": time.time(),
                        "exec_finished": False,
                        "file_received": False,
                    }
                    print(
                        f"{PROGRESS_EXECUTING} resumed for spare worker {spare_ip} ({part_filename_for_node(node_index)})"
                    )

        # Check if received output files have arrived in master/output/
        for ip, task in list(active_tasks.items()):
            if task["exec_finished"] and not task["file_received"]:
                expected_output_file = Path(MASTER_OUTPUT_DIRECTORY) / task["part_filename"]
                if expected_output_file.is_file():
                    task["file_received"] = True
                    print(
                        f"{PROGRESS_FINISHED} ({ip}, {task['part_filename']}) - result saved to {expected_output_file}"
                    )
                    if dashboard:
                        dashboard.update_node(
                            ip, f"node{task['node_index']}", task["part_filename"], PROGRESS_FINISHED
                        )

        time.sleep(0.5)

    return active_daemons


def run_master() -> None:
    """Load config, validate addresses, split video, distribute parts, track execution, collect results, and merge."""
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

    dashboard = StatusDashboard(master_state="initializing")

    print(f"Loaded config from {arguments.config}")
    print(f"Active nodes ({len(active_nodes)}): {', '.join(active_nodes)}")
    print(f"Spare nodes ({len(spare_nodes)}): {', '.join(spare_nodes)}")
    print(f"Wrote devices.json: {device_mapping}")
    print(f"Listening on control port {CONTROL_PORT}...")

    listening_socket = open_listening_socket(FIXED_PORT)
    worker_pool = WorkerConnectionPool(
        arguments.worker_ip_addresses, listening_socket, dashboard=dashboard
    )
    daemons: list[FileTransferDaemon] = []


    try:
        print(f"Waiting for active worker connections ({', '.join(active_nodes)})...")
        for ip in active_nodes:
            worker_pool.get_connection(ip)
        print("All active workers connected!")

        dashboard.set_master_state("splitting file")
        run_split_command(
            config.split_command,
            len(active_nodes),
            parts_directory=arguments.parts_directory,
        )
        verify_part_files(len(active_nodes), arguments.parts_directory)

        for idx, ip in enumerate(active_nodes, start=1):
            dashboard.update_node(ip, f"node{idx}", part_filename_for_node(idx), "sending file")

        initial_daemons = distribute_part_files_to_active_nodes(
            active_nodes,
            worker_pool,
            arguments.parts_directory,
            master_ip_address=arguments.master_ip,
        )
        daemons.extend(initial_daemons)

        print("\nPhase 2 complete: split files distributed to all active workers.")

        # Phase 3 & Phase 4 (Task 4.1): Monitor execution, handle 1-attempt retry & failover
        dashboard.set_master_state("executing")
        failover_daemons = monitor_worker_executions_and_collect_results(
            active_nodes,
            worker_pool,
            spare_nodes,
            arguments.parts_directory,
            master_ip_address=arguments.master_ip,
            dashboard=dashboard,
        )
        daemons.extend(failover_daemons)

        print("\nPhase 4 Result Collection complete: all part files received.")

        # Phase 4 (Task 4.2): Merge output part files into single final video
        dashboard.set_master_state("merging files")
        run_merge_command(
            config.merge_command,
            len(active_nodes),
            output_directory=MASTER_OUTPUT_DIRECTORY,
        )
        dashboard.set_master_state("finished")
        print("\nAll tasks and merging completed successfully.")

    except KeyboardInterrupt:
        print("\nMaster shutting down.")
    except (OSError, RuntimeError, ValueError, FileNotFoundError) as exc:
        print(f"Master error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        if 'worker_pool' in locals() and worker_pool:
            try:
                worker_pool.shutdown_all_workers()
            except Exception:
                pass
            worker_pool.stop()
        for daemon in daemons:
            try:
                daemon.stop()
            except Exception:
                pass
        if 'listening_socket' in locals() and listening_socket:
            try:
                listening_socket.close()
            except Exception:
                pass


if __name__ == "__main__":
    run_master()
