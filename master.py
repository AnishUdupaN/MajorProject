"""Master node entry point: load config, validate addresses, split video, distribute parts, track execution with same-node retries & dynamic pooling, collect results, and merge."""

import argparse
import select
import socket
import sys
import threading
import time
from pathlib import Path

from core import (
    CONTROL_PORT,
    FIXED_PORT,
    MASTER_INPUT_DIRECTORY,
    MASTER_OUTPUT_DIRECTORY,
    MDNS_MASTER_FQDN,
    MDNS_SERVICE_MASTER,
    PROGRESS_EXECUTING,
    PROGRESS_FINISHED,
    PROGRESS_RECEIVING_FILES,
    PROGRESS_SENDING_FILE,
    Config,
    FileTransferDaemon,
    MdnsAnnouncer,
    StatusDashboard,
    get_local_ip_for_peer,
    has_buffered_message,
    load_config,
    part_filename_for_node,
    receive_file_received_message,
    receive_json_message,
    run_merge_command,
    run_split_command,
    send_ready_message,
    send_shutdown_message,
    start_file_transfer_daemon,
    swap_device_ip,
    verify_part_files,
    write_devices_json,
    printlog,
    get_shared_secret,
    verify_auth_message
)

# clean the old log file
f=open("logs.txt","w")
f.write("")
f.close()



class WorkerConnectionPool:
    """Thread-safe pool accepting worker connections dynamically at any time."""

    def __init__(
        self,
        allowed_ips: list[str] | None,
        listening_socket: socket.socket,
        dashboard: StatusDashboard | None = None,
    ) -> None:
        self.allowed_ips = set(allowed_ips) if allowed_ips else None
        self.listening_socket = listening_socket
        self.dashboard = dashboard
        self.connections: dict[str, socket.socket] = {}
        self.lock = threading.Lock()
        self.running = True
        self._shared_secret = get_shared_secret()
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
                if self.allowed_ips and ip not in self.allowed_ips:
                    conn.close()
                    continue
                # VULN-07: Verify shared secret if configured
                if self._shared_secret:
                    if not verify_auth_message(conn, self._shared_secret):
                        printlog(f"Worker {ip}:{addr[1]} failed authentication. Rejecting.")
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
                    printlog(f"\nWorker {ip}:{addr[1]} connected, kept IDLE.")
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
    """Parse CLI arguments for master startup with optional list of worker IP addresses."""
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
        "--input-video",
        default=None,
        help="Path to input video file (default: auto-detected from split_command or master/input/)",
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
    return parser.parse_args()


def split_active_and_spare_addresses(
    worker_ip_addresses: list[str], max_nodes: int
) -> tuple[list[str], list[str]]:
    """First max_nodes addresses are active nodes; remaining addresses are spares."""
    active_nodes = worker_ip_addresses[:max_nodes]
    spare_nodes = worker_ip_addresses[max_nodes:]
    return active_nodes, spare_nodes


def validate_worker_address_count(
    worker_ip_addresses: list[str] | None, min_devices: int
) -> None:
    """Reject startup if manual worker IPs are given but fewer than min_devices."""
    if worker_ip_addresses:
        if len(worker_ip_addresses) < min_devices:
            raise ValueError(
                f"At least {min_devices} worker IP addresses are required "
                f"(min_devices={min_devices}), "
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
    printlog(f"{PROGRESS_SENDING_FILE} ({worker_ip_address}, {part_filename})")
    send_ready_message(connection, reachable_master_ip, daemon.port)
    receive_file_received_message(connection)
    return daemon


def distribute_part_files_to_active_nodes(
    active_nodes: list[str],
    worker_pool: WorkerConnectionPool,
    parts_directory: str,
    master_ip_address: str | None = None,
    dashboard: StatusDashboard | None = None,
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
        printlog(f"{PROGRESS_SENDING_FILE} ({worker_ip_address}, {part_filename})")
        if dashboard:
            dashboard.add_message(
                f"Sending {part_filename} to {worker_ip_address}", timeout_seconds=8
            )
        send_ready_message(connection, reachable_master_ip, file_transfer_port)

    for worker_ip_address, part_filename, connection, _, _ in pending_transfers:
        receive_file_received_message(connection)
        if dashboard:
            dashboard.update_node_flags(
                worker_ip_address, receiving=False, executing=True, sending=False
            )

    return daemons



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
) -> tuple[str, socket.socket, FileTransferDaemon]:
    """When no spare nodes are available, ask user to (k)ill task or (w)ait 1 minute for new nodes to connect."""
    printlog(f"\n[RECOVERY ALERT] Node {failed_ip} failed and no spare nodes are available for recovery.")

    while True:
        if dashboard:
            dashboard.add_message(
                f"ALERT: Node {failed_ip} failed! No spare nodes. Press 'k' to kill, 'w' to wait 1m.",
                timeout_seconds=60,
            )

        print(f"\n==================================================================")
        print(f"  NO RECOVERY NODES AVAILABLE FOR NODE{node_index} ({failed_ip})  ")
        print(f"==================================================================")
        print("One of the active devices has failed and no spare nodes are connected.")
        print("Options:")
        print("  [k] Kill task: Safely terminate running tasks on healthy nodes and exit.")
        print("  [w] Wait 1 minute: Keep healthy nodes running and wait for a new node to connect.")
        sys.stdout.write("Enter choice (k/w): ")
        sys.stdout.flush()

        choice = ""
        fd = None
        old_settings = None
        if sys.stdin.isatty():
            try:
                import termios
                import tty
                fd = sys.stdin.fileno()
                old_settings = termios.tcgetattr(fd)
                tty.setcbreak(fd)
            except Exception:
                fd = None
                old_settings = None

        try:
            while True:
                if fd is not None:
                    rlist, _, _ = select.select([sys.stdin], [], [], 0.5)
                    if rlist:
                        char = sys.stdin.read(1).lower()
                        if char in ("k", "w"):
                            choice = char
                            print(char)
                            break
                else:
                    try:
                        line = sys.stdin.readline().strip().lower()
                        if line:
                            if line.startswith("k"):
                                choice = "k"
                            elif line.startswith("w"):
                                choice = "w"
                            break
                    except Exception:
                        choice = "k"
                        break
                time.sleep(0.1)
        finally:
            if fd is not None and old_settings is not None:
                try:
                    import termios
                    termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                except Exception:
                    pass

        if choice == "k":
            print("\nUser selected to KILL the task. Shutting down all healthy worker nodes...")
            printlog(f"\nUser selected 'k' (kill task) after failure of node {failed_ip}.")
            raise RuntimeError("Task execution killed by user due to node failure and lack of recovery nodes.")

        elif choice == "w":
            print("\nUser selected to WAIT. Waiting up to 60 seconds for a new node to connect...")
            printlog(f"\nUser selected 'w' (wait 60s) after failure of node {failed_ip}.")
            wait_start = time.time()
            wait_timeout = 60.0
            found_spare_ip = None

            while time.time() - wait_start < wait_timeout:
                elapsed_wait = int(time.time() - wait_start)
                remaining = int(wait_timeout - elapsed_wait)

                with worker_pool.lock:
                    for ip in list(worker_pool.connections.keys()):
                        if ip not in active_tasks and ip != failed_ip:
                            found_spare_ip = ip
                            break

                if found_spare_ip:
                    print(f"\n[NEW NODE CONNECTED] Discovered new node {found_spare_ip}!")
                    printlog(f"[FAILOVER] New node {found_spare_ip} connected during wait period.")
                    break

                if dashboard:
                    dashboard.add_message(
                        f"Waiting for new nodes... ({remaining}s remaining). Press 'k' to cancel & kill.",
                        timeout_seconds=2,
                    )

                if sys.stdin.isatty():
                    rlist, _, _ = select.select([sys.stdin], [], [], 0.0)
                    if rlist:
                        char = sys.stdin.read(1).lower()
                        if char == "k":
                            print("\nUser pressed 'k' during wait. Shutting down task...")
                            raise RuntimeError("Task execution killed by user during node wait period.")

                for active_ip, task in list(active_tasks.items()):
                    if task["exec_finished"] and not task["file_received"]:
                        expected_output_file = Path(MASTER_OUTPUT_DIRECTORY) / task["part_filename"]
                        if expected_output_file.is_file():
                            task["file_received"] = True
                            printlog(f"{PROGRESS_FINISHED} ({active_ip}, {task['part_filename']}) - result saved to {expected_output_file}")

                time.sleep(0.5)

            if found_spare_ip:
                updated_mapping = swap_device_ip(failed_ip, found_spare_ip)
                printlog(f"[FAILOVER] Updated devices.json: {updated_mapping}")

                spare_connection = worker_pool.get_connection(found_spare_ip)
                printlog(f"[FAILOVER] Reusing existing split file {part_filename} for spare node {found_spare_ip}")
                daemon = distribute_part_file_to_single_node(
                    found_spare_ip,
                    part_filename,
                    spare_connection,
                    parts_directory,
                    master_ip_address,
                )
                return found_spare_ip, spare_connection, daemon
            else:
                print("\n[TIMEOUT] 60 seconds elapsed and no new nodes connected.")
                printlog("[RECOVERY TIMEOUT] 60s elapsed without new nodes.")


def reassign_task_to_spare_node(
    failed_ip: str,
    node_index: int,
    spare_nodes: list[str],
    worker_pool: WorkerConnectionPool,
    parts_directory: str,
    master_ip_address: str | None = None,
    active_tasks: dict[str, dict] | None = None,
    dashboard: StatusDashboard | None = None,
) -> tuple[str, socket.socket, FileTransferDaemon]:
    """Swap IP in devices.json and reassign task to the next spare node or prompt user if no spare is available."""
    spare_ip = None
    if spare_nodes:
        spare_ip = spare_nodes.pop(0)
    elif active_tasks is not None:
        with worker_pool.lock:
            for ip in list(worker_pool.connections.keys()):
                if ip not in active_tasks and ip != failed_ip:
                    spare_ip = ip
                    break

    if spare_ip:
        printlog(f"\n Node {failed_ip} failed. Reassigning task to node{node_index}, {spare_ip}")
        updated_mapping = swap_device_ip(failed_ip, spare_ip)
        printlog(f"[FAILOVER] Updated devices.json: {updated_mapping}")

        spare_connection = worker_pool.get_connection(spare_ip)
        part_filename = part_filename_for_node(node_index)

        printlog(f"[FAILOVER] Reusing existing split file {part_filename} for spare node {spare_ip}")
        daemon = distribute_part_file_to_single_node(
            spare_ip,
            part_filename,
            spare_connection,
            parts_directory,
            master_ip_address,
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
        )


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

    printlog("\n--- Starting Phase 3 Execution & Phase 4 Result Collection ---")

    while True:
        unfinished_ips = [ip for ip, task in active_tasks.items() if not task["file_received"]]
        if not unfinished_ips:
            printlog("\nAll active worker outputs have been collected!")
            break

        for ip in unfinished_ips:
            task = active_tasks[ip]
            elapsed = time.time() - task["start_time"]
            node_id = f"node{task['node_index']}"
            if not task["exec_finished"]:
                minutes, secs = int(elapsed) // 60, int(elapsed) % 60
                printlog(f"{PROGRESS_EXECUTING} ({ip}, {task['part_filename']}) - live running time: {minutes:02d}:{secs:02d}")
                if dashboard:
                    dashboard.update_node(
                        ip, node_id, task["part_filename"], PROGRESS_EXECUTING, elapsed,
                        receiving=False, executing=True, sending=False
                    )
            else:
                printlog(f"{PROGRESS_RECEIVING_FILES} ({ip}, {task['part_filename']})")
                if dashboard:
                    dashboard.update_node(
                        ip, node_id, task["part_filename"], PROGRESS_RECEIVING_FILES,
                        receiving=True, executing=False, sending=False
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
                    printlog(
                        f"\n[EXECUTION COMPLETE] Node {ip} finished {task['part_filename']} in {exec_time:.2f}s"
                    )
                    printlog(f"{PROGRESS_RECEIVING_FILES} ({ip}, {task['part_filename']})")
                    if dashboard:
                        dashboard.update_node(
                            ip, f"node{task['node_index']}", task["part_filename"], PROGRESS_RECEIVING_FILES,
                            receiving=True, executing=False, sending=False
                        )
                        dashboard.add_message(
                            f"Node {ip} finished {task['part_filename']} in {exec_time:.2f}s", timeout_seconds=10
                        )
                elif msg.get("type") == "failed":
                    raise ConnectionError(f"Node reported failure: {msg.get('reason')}")
            except (ConnectionError, OSError, ValueError, KeyError) as exc:
                if not task["exec_finished"]:
                    printlog(f"\n[FAILURE DETECTED] Node {ip} socket error/disconnect: {exc}")
                    worker_pool.remove_connection(ip)

                    node_index = task["node_index"]
                    part_filename = task["part_filename"]
                    del active_tasks[ip]
                    if dashboard:
                        dashboard.update_node(ip, f"node{node_index}", "-", "idle", receiving=False, executing=False, sending=False)
                        dashboard.add_message(f"ALERT: Node {ip} task failed ({exc})", timeout_seconds=12)

                    retries = node_retries.get(ip, 0)
                    if retries < 1:
                        # 1 Retry attempt on the same node first
                        node_retries[ip] = retries + 1
                        printlog(
                            f"\n[RETRY 1/1] Task for node{node_index} failed on {ip}. Attempting retry 1/1 on same node {ip}..."
                        )

                        retry_conn = None
                        try:
                            retry_conn = worker_pool.get_connection(ip, timeout_seconds=8.0)

                        except TimeoutError:
                            printlog(f"[RETRY FAILED] Node {ip} did not reconnect within timeout.")

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
                            printlog(f"[RETRY 1/1] Resumed execution on same node {ip} ({part_filename})")
                            continue

                    # If retry attempt failed or retries >= 1, trigger failover to spare node
                    spare_ip, spare_conn, daemon = reassign_task_to_spare_node(
                        ip,
                        node_index,
                        spare_nodes,
                        worker_pool,
                        parts_directory,
                        master_ip_address,
                        active_tasks=active_tasks,
                        dashboard=dashboard,
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
                    printlog(
                        f"{PROGRESS_EXECUTING} resumed for spare worker {spare_ip} ({part_filename_for_node(node_index)})"
                    )

        # Check if received output files have arrived in master/output/
        for ip, task in list(active_tasks.items()):
            if task["exec_finished"] and not task["file_received"]:
                expected_output_file = Path(MASTER_OUTPUT_DIRECTORY) / task["part_filename"]
                if expected_output_file.is_file():
                    task["file_received"] = True
                    printlog(
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
            arguments.worker_ip_addresses, config.min_devices
        )
    except ValueError as exc:
        print(f"Startup error: {exc}", file=sys.stderr)
        sys.exit(1)

    # master/input holds split parts; master/output holds merged results.
    Path(arguments.parts_directory).mkdir(parents=True, exist_ok=True)
    Path(MASTER_OUTPUT_DIRECTORY).mkdir(parents=True, exist_ok=True)

    # Start Master mDNS announcer in background
    mdns_announcer = MdnsAnnouncer(
        fqdn=MDNS_MASTER_FQDN, service=MDNS_SERVICE_MASTER
    )
    mdns_announcer.start()

    dashboard = StatusDashboard(master_state="initializing")

    print(f"Loaded config from {arguments.config}")
    print(f"Listening on control port {CONTROL_PORT}...")

    listening_socket = open_listening_socket(FIXED_PORT)
    worker_pool = WorkerConnectionPool(
        arguments.worker_ip_addresses, listening_socket, dashboard=dashboard
    )
    daemons: list[FileTransferDaemon] = []

    if arguments.worker_ip_addresses:
        active_nodes, spare_nodes = split_active_and_spare_addresses(
            arguments.worker_ip_addresses, config.max_nodes
        )
    else:
        dashboard.set_master_state("discovering workers")
        start_time = time.time()
        timeout_seconds = 60.0
        deadline = start_time + timeout_seconds

        fd = None
        old_settings = None
        if sys.stdin.isatty():
            try:
                import termios
                import tty

                fd = sys.stdin.fileno()
                old_settings = termios.tcgetattr(fd)
                tty.setcbreak(fd)
            except Exception:
                fd = None
                old_settings = None

        try:
            print("mDNS Worker Discovery active. Press 'y' to start task execution when ready...")
            while True:
                now = time.time()
                remaining = int(max(0.0, deadline - now))
                with worker_pool.lock:
                    connected_ips = list(worker_pool.connections.keys())

                dashboard.add_message(
                    f"Discovered {len(connected_ips)} worker(s). Press 'y' to start processing, 'q' to quit ({remaining}s remaining)",
                    timeout_seconds=2,
                )

                if fd is not None:
                    rlist, _, _ = select.select([sys.stdin], [], [], 0.5)
                    if rlist:
                        char = sys.stdin.read(1).lower()
                        if char == "y":
                            if not connected_ips:
                                dashboard.add_message("Cannot start: 0 workers discovered yet! Waiting...", timeout_seconds=5)
                            else:
                                break
                        elif char == "q":
                            print("\nUser pressed 'q'. Exiting discovery...")
                            sys.exit(0)
                else:
                    if connected_ips:
                        print(f"\nAuto-discovered workers: {connected_ips}")
                        break

                if now >= deadline:
                    if not connected_ips:
                        print(
                            f"\n[DISCOVERY TIMEOUT] No workers discovered within {int(timeout_seconds)}s. Exiting safely.",
                            file=sys.stderr,
                        )
                        sys.exit(0)
                    else:
                        print(
                            f"\n[DISCOVERY TIMEOUT] Timeout reached ({int(timeout_seconds)}s). Proceeding with {len(connected_ips)} discovered worker(s)."
                        )
                        break
                time.sleep(0.5)
        finally:
            if fd is not None and old_settings is not None:
                try:
                    import termios

                    termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                except Exception:
                    pass

        with worker_pool.lock:
            connected_ips = list(worker_pool.connections.keys())

        if len(connected_ips) >= config.max_nodes:
            active_nodes, spare_nodes = split_active_and_spare_addresses(
                connected_ips, config.max_nodes
            )
        else:
            active_nodes = connected_ips
            spare_nodes = []

    device_mapping = write_devices_json(active_nodes)
    print(f"Active nodes ({len(active_nodes)}): {', '.join(active_nodes)}")
    print(f"Spare nodes ({len(spare_nodes)}): {', '.join(spare_nodes)}")
    printlog(f"Wrote devices.json: {device_mapping}")


    try:
        print(f"Waiting for active worker connections ({', '.join(active_nodes)})...")
        for ip in active_nodes:
            worker_pool.get_connection(ip)
        print("All active workers connected!")

        dashboard.set_master_state("splitting file")
        run_split_command(
            config.split_command,
            len(active_nodes),
            input_video=arguments.input_video,
            parts_directory=arguments.parts_directory,
        )
        verify_part_files(
            len(active_nodes),
            parts_directory=arguments.parts_directory,
            split_command=config.split_command,
        )

        for idx, ip in enumerate(active_nodes, start=1):
            dashboard.update_node(
                ip, f"node{idx}", part_filename_for_node(idx, split_command=config.split_command, parts_directory=arguments.parts_directory), "sending file",
                receiving=False, executing=False, sending=True
            )

        initial_daemons = distribute_part_files_to_active_nodes(
            active_nodes,
            worker_pool,
            arguments.parts_directory,
            master_ip_address=arguments.master_ip,
            dashboard=dashboard,
        )
        daemons.extend(initial_daemons)

        printlog("\nPhase 2 complete: split files distributed to all active workers.")

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

        printlog("\nAll part files received.")

        # Phase 4 (Task 4.2): Merge output part files into single final video
        dashboard.set_master_state("merging files")
        run_merge_command(
            config.merge_command,
            len(active_nodes),
            output_directory=MASTER_OUTPUT_DIRECTORY,
            split_command=config.split_command,
        )
        dashboard.set_master_state("finished")
        printlog("\nAll tasks and merging completed successfully.")

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
