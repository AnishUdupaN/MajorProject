"""Master node entry point: load config, validate addresses, split video, distribute parts, track execution with same-node retries & dynamic pooling, collect results, and merge."""

import argparse
import select
import signal
import socket
import sys
import threading
import time
import os
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
    cleanup_master_temporary_files,
    compute_md5,
    get_available_configs,
    get_binary_path,
    get_local_ip_for_peer,
    has_buffered_message,
    load_config,
    part_filename_for_node,
    receive_binary_info,
    receive_file_received_message,
    receive_json_message,
    run_merge_command,
    run_split_command,
    send_binary_info_request,
    send_binary_ready,
    send_ready_message,
    send_shutdown_message,
    start_file_transfer_daemon,
    swap_device_ip,
    verify_part_files,
    write_devices_json,
    printlog,
    get_shared_secret,
    verify_auth_message,
    generate_pin,
    send_pin_challenge,
    receive_pin_response,
    send_pin_accepted,
    send_pin_rejected,
    receive_worker_telemetry,
    rank_workers_by_affinity,
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
        password: str | None = None,
    ) -> None:
        self.allowed_ips = set(allowed_ips) if allowed_ips else None
        self.listening_socket = listening_socket
        self.dashboard = dashboard
        self.connections: dict[str, socket.socket] = {}
        self.paired_ips: set[str] = set()
        self.session_pin = password if password else generate_pin()
        self.worker_telemetry: dict[str, dict] = {}
        self.killed_ips: set[str] = set()
        self.lock = threading.Lock()
        self.running = True
        self._shared_secret = get_shared_secret()
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def mark_killed(self, key: str) -> None:
        """Mark a worker key/IP as permanently killed after failover so reconnects are rejected."""
        with self.lock:
            ip = key.split(":")[0]
            self.killed_ips.add(key)
            self.killed_ips.add(ip)

            keys_to_remove = [k for k in list(self.connections.keys()) if k == key or k == ip or k.startswith(f"{ip}:")]
            for k in keys_to_remove:
                conn = self.connections.pop(k, None)
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
                if self.dashboard:
                    existing = self.dashboard.node_states.get(k, {})
                    node_id = existing.get("node_id", "worker")
                    if "(killed)" not in node_id:
                        node_id = f"{node_id} (killed)"
                    filename = existing.get("filename", "-")
                    self.dashboard.update_node(
                        k, node_id, filename, "killed",
                        receiving=False, executing=False, sending=False, connected=False
                    )

    def _accept_loop(self) -> None:
        while self.running:
            try:
                r, _, _ = select.select([self.listening_socket], [], [], 0.5)
                if not r:
                    continue
                conn, addr = self.listening_socket.accept()
                ip = addr[0]
                port = addr[1]
                endpoint = f"{ip}:{port}"

                if self.allowed_ips and ip not in self.allowed_ips and endpoint not in self.allowed_ips:
                    conn.close()
                    continue
                if self._shared_secret:
                    if not verify_auth_message(conn, self._shared_secret):
                        printlog(f"Worker {endpoint} failed authentication. Rejecting.")
                        conn.close()
                        continue
                else:
                    msg = f"Worker {ip} wants to join. Password/PIN: {self.session_pin}"
                    printlog(msg)
                    if self.dashboard:
                        self.dashboard.add_message(msg, timeout_seconds=120)
                        self.dashboard.add_message(f"Master Password/PIN: {self.session_pin}", timeout_seconds=120)
                    import socket as _sock
                    send_pin_challenge(conn, _sock.gethostname())
                    
                    attempts_left = 3
                    paired = False
                    while attempts_left > 0:
                        try:
                            conn.settimeout(10.0)
                            resp = receive_pin_response(conn)
                            conn.settimeout(None)
                            if resp["pin"] == self.session_pin:
                                send_pin_accepted(conn)
                                paired = True
                                break
                            else:
                                attempts_left -= 1
                                send_pin_rejected(conn, "Invalid PIN/Password", attempts_left)
                        except Exception as e:
                            break
                    if not paired:
                        printlog(f"Worker {endpoint} failed authentication pairing. Rejecting.")
                        conn.close()
                        continue

                try:
                    telemetry = receive_worker_telemetry(conn)
                    with self.lock:
                        self.worker_telemetry[endpoint] = telemetry
                        self.worker_telemetry[ip] = telemetry
                except Exception as e:
                    printlog(f"Worker {endpoint} failed to send telemetry: {e}")
                    conn.close()
                    continue

                with self.lock:
                    if ip in self.killed_ips or endpoint in self.killed_ips or (
                        self.dashboard
                        and (
                            self.dashboard.node_states.get(ip, {}).get("state") == "killed"
                            or self.dashboard.node_states.get(endpoint, {}).get("state") == "killed"
                            or "(killed)" in self.dashboard.node_states.get(ip, {}).get("node_id", "")
                            or "(killed)" in self.dashboard.node_states.get(endpoint, {}).get("node_id", "")
                        )
                    ):
                        printlog(f"Worker {endpoint} is KILLED. Rejecting reconnect.")
                        conn.close()
                        continue

                    # Check existing connections for this IP
                    matching_keys = [k for k in list(self.connections.keys()) if k == ip or k.startswith(f"{ip}:")]
                    dead_keys = []
                    for k in matching_keys:
                        old_c = self.connections[k]
                        try:
                            rlist, _, _ = select.select([old_c], [], [], 0.0)
                            if rlist:
                                peek = old_c.recv(1, socket.MSG_PEEK)
                                if not peek:
                                    dead_keys.append(k)
                        except Exception:
                            dead_keys.append(k)

                    for k in dead_keys:
                        old_c = self.connections.pop(k, None)
                        if old_c:
                            try:
                                old_c.close()
                            except Exception:
                                pass
                        if self.dashboard and k in self.dashboard.node_states:
                            existing = self.dashboard.node_states.get(k, {})
                            node_id = existing.get("node_id", "worker")
                            filename = existing.get("filename", "-")
                            self.dashboard.update_node(
                                k, node_id, filename, "disconnected",
                                receiving=False, executing=False, sending=False, connected=False
                            )

                    alive_keys = [k for k in list(self.connections.keys()) if k == ip or k.startswith(f"{ip}:")]
                    if not alive_keys:
                        # Single active worker for this IP -> store under key `ip`
                        node_key = ip
                    else:
                        # Multiple active workers on the same IP -> promote/use endpoint keys `f"{ip}:{port}"`
                        for k in alive_keys:
                            if k == ip:
                                old_c = self.connections.pop(ip)
                                try:
                                    old_p = old_c.getpeername()[1]
                                    old_ep = f"{ip}:{old_p}"
                                except Exception:
                                    old_ep = f"{ip}:old"
                                self.connections[old_ep] = old_c
                                if self.dashboard and ip in self.dashboard.node_states:
                                    st = self.dashboard.node_states.pop(ip)
                                    self.dashboard.node_states[old_ep] = st

                        node_key = endpoint

                    self.connections[node_key] = conn
                    printlog(f"\nWorker {endpoint} connected as key '{node_key}', kept IDLE.")

                    if self.dashboard:
                        existing = self.dashboard.node_states.get(node_key, {})
                        node_id = existing.get("node_id", "worker")
                        filename = existing.get("filename", "-")
                        was_connected = existing.get("connected", False) if existing else False
                        state = existing.get("state", "idle")
                        if state in ("disconnected", "killed") or not was_connected:
                            state = "idle"
                        telemetry = self.worker_telemetry.get(ip, {})
                        self.dashboard.update_node(
                            node_key, node_id, filename, state,
                            receiving=False, executing=False, sending=False, connected=True,
                            hostname=telemetry.get("hostname"), username=telemetry.get("username")
                        )
                        if not existing or not was_connected:
                            msg_action = "reconnected" if (existing and not was_connected) else "connected"
                            self.dashboard.add_message(f"Node {node_key} {msg_action} ({node_id})", timeout_seconds=5)
            except Exception:
                pass

    def get_connection(self, key: str, timeout_seconds: float = 30.0) -> socket.socket:
        """Fetch connection for key/ip from pool, waiting if not yet connected."""
        deadline = time.time() + timeout_seconds
        ip = key.split(":")[0]
        while time.time() < deadline:
            with self.lock:
                if key in self.connections:
                    return self.connections[key]
                # Look for matching IP or endpoint
                for k, conn in self.connections.items():
                    if k == ip or k.startswith(f"{ip}:") or k.split(":")[0] == ip:
                        return conn
            time.sleep(0.2)
        raise TimeoutError(f"Worker {key} did not connect within {timeout_seconds}s")

    def remove_connection(self, key: str) -> None:
        """Remove a dead or closed socket connection for key from the pool."""
        with self.lock:
            ip = key.split(":")[0]
            target_key = key if key in self.connections else None
            if not target_key:
                for k in self.connections:
                    if k == ip or k.startswith(f"{ip}:") or k.split(":")[0] == ip:
                        target_key = k
                        break

            if target_key:
                conn = self.connections.pop(target_key, None)
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
                if self.dashboard:
                    existing = self.dashboard.node_states.get(target_key, {})
                    node_id = existing.get("node_id", "worker")
                    filename = existing.get("filename", "-")
                    self.dashboard.update_node(
                        target_key, node_id, filename, "disconnected",
                        receiving=False, executing=False, sending=False, connected=False
                    )

    def shutdown_all_workers(self) -> None:
        """Broadcast shutdown message to all connected workers in the pool."""
        with self.lock:
            for key, conn in list(self.connections.items()):
                try:
                    send_shutdown_message(conn)
                except Exception:
                    pass
        import time
        time.sleep(0.2)

    def stop(self) -> None:
        self.running = False



def parse_master_arguments() -> argparse.Namespace:
    """Parse CLI arguments for master startup with optional list of worker IP addresses."""
    parser = argparse.ArgumentParser(
        description="Start the master node for distributed video processing."
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
        "--input-video",
        default=None,
        help="Path to input video file (default: auto-detected from split_command or master/input/)",
    )
    parser.add_argument(
        "--password",
        default=None,
        help="Static password for worker authentication (disables random PIN)",
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
    config: Config | None = None,
) -> tuple[str, socket.socket, FileTransferDaemon]:
    """When no spare nodes are available, ask user to (k)ill task, (w)ait 1 minute for new nodes, (r)eassign to a healthy node when finished, or (b)oth (listen for new nodes & fallback)."""
    printlog(f"\n[RECOVERY ALERT] Node {failed_ip} failed and no spare nodes are available for recovery.")

    while True:
        if dashboard:
            dashboard.add_message(
                f"ALERT: Node {failed_ip} failed! No spare nodes. Press 'k' to kill, 'w' to wait 1m, 'r' to reassign, 'b' for both.",
                timeout_seconds=60,
            )

        prompt_text = (
            f"\n==================================================================\n"
            f"  NO RECOVERY NODES AVAILABLE FOR NODE{node_index} ({failed_ip})  \n"
            f"==================================================================\n"
            "One of the active devices has failed and no spare nodes are connected.\n"
            "Options:\n"
            "  [k] Kill task: Safely terminate running tasks on healthy nodes and exit.\n"
            "  [w] Wait 1 minute: Keep healthy nodes running and wait for a new node to connect.\n"
            "  [r] Reassign when finished: Wait until a healthy node finishes its task and reassign this task to it.\n"
            "  [b] Both: Listen for new nodes for 60s; if none found, fallback to reassigning to first finished node.\n"
            "Enter choice (k/w/r/b): "
        )
        if dashboard:
            dashboard.set_prompt(prompt_text)
        else:
            sys.stdout.write(prompt_text)
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
                        if char in ("k", "w", "r", "b"):
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
                            elif line.startswith("r"):
                                choice = "r"
                            elif line.startswith("b"):
                                choice = "b"
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
                    
        if dashboard:
            dashboard.set_prompt(None)

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

                idle_ips = set()
                with worker_pool.lock:
                    for ip in list(worker_pool.connections.keys()):
                        base_ip = ip.split(":")[0]
                        if ip not in active_tasks and base_ip not in active_tasks and ip not in worker_pool.killed_ips and base_ip not in worker_pool.killed_ips:
                            idle_ips.add(ip)
                if idle_ips:
                    resource_type = config.resource_type if config else "auto"
                    idle_telemetry = {ip: worker_pool.worker_telemetry.get(ip, {}) for ip in idle_ips}
                    ranked_ips = rank_workers_by_affinity(idle_telemetry, resource_type)
                    if ranked_ips:
                        found_spare_ip = ranked_ips[0]

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

        elif choice == "r":
            print("\nUser selected to REASSIGN. Waiting for a healthy node to complete its current task...")
            printlog(f"\nUser selected 'r' (reassign when finished) after failure of node {failed_ip}.")

            reassigned_ip = None
            reassigned_conn = None

            while True:
                healthy_candidates = [ip for ip in active_tasks if ip != failed_ip]
                if not healthy_candidates:
                    print("\n[ERROR] No healthy running nodes available to take over the task.")
                    printlog("[REASSIGN ERROR] No healthy nodes available.")
                    break

                # Check if any candidate has already finished its execution
                for healthy_ip in healthy_candidates:
                    t = active_tasks[healthy_ip]
                    if t.get("exec_finished"):
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
                    rlist, _, _ = select.select([sys.stdin], [], [], 0.0)
                    if rlist:
                        char = sys.stdin.read(1).lower()
                        if char == "k":
                            print("\nUser pressed 'k' during wait. Shutting down task...")
                            raise RuntimeError("Task execution killed by user during node wait period.")

                # Check control sockets for finished messages from healthy nodes
                socket_map = {
                    t["connection"]: a_ip
                    for a_ip, t in active_tasks.items()
                    if a_ip != failed_ip and not t.get("exec_finished")
                }
                if socket_map:
                    readable, _, _ = select.select(list(socket_map.keys()), [], [], 0.5)
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
                                printlog(f"[NODE FAILURE] Healthy candidate {a_ip} failed during wait.")
                        except Exception as exc:
                            printlog(f"[SOCKET ERROR] Node {a_ip} error during wait: {exc}")

                time.sleep(0.2)

            if reassigned_ip:
                print(f"\n[TASK REASSIGNMENT] Node {reassigned_ip} finished its task and is taking over {part_filename}!")
                printlog(f"[REASSIGNMENT] Reassigning {part_filename} (node{node_index}) to healthy node {reassigned_ip}.")

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

                daemon = distribute_part_file_to_single_node(
                    reassigned_ip,
                    part_filename,
                    reassigned_conn,
                    parts_directory,
                    master_ip_address,
                )
                return reassigned_ip, reassigned_conn, daemon

        elif choice == "b":
            print("\nUser selected BOTH. Listening up to 60s for new nodes or waiting for healthy node to finish...")
            printlog(f"\nUser selected 'b' (both) after failure of node {failed_ip}.")

            wait_start = time.time()
            wait_timeout = 60.0
            found_spare_ip = None
            reassigned_ip = None

            while time.time() - wait_start < wait_timeout:
                elapsed_wait = int(time.time() - wait_start)
                remaining = int(wait_timeout - elapsed_wait)

                idle_ips = set()
                with worker_pool.lock:
                    for ip in list(worker_pool.connections.keys()):
                        base_ip = ip.split(":")[0]
                        if ip not in active_tasks and base_ip not in active_tasks and ip not in worker_pool.killed_ips and base_ip not in worker_pool.killed_ips:
                            idle_ips.add(ip)
                if idle_ips:
                    resource_type = config.resource_type if config else "auto"
                    idle_telemetry = {ip: worker_pool.worker_telemetry.get(ip, {}) for ip in idle_ips}
                    ranked_ips = rank_workers_by_affinity(idle_telemetry, resource_type)
                    if ranked_ips:
                        found_spare_ip = ranked_ips[0]

                if found_spare_ip:
                    print(f"\n[NEW NODE CONNECTED] Discovered new node {found_spare_ip}!")
                    printlog(f"[FAILOVER] New node {found_spare_ip} connected during both-wait period.")
                    break

                healthy_candidates = [ip for ip in active_tasks if ip != failed_ip]
                for healthy_ip in healthy_candidates:
                    t = active_tasks[healthy_ip]
                    if t.get("exec_finished"):
                        reassigned_ip = healthy_ip
                        break

                if reassigned_ip:
                    print(f"\n[HEALTHY NODE FINISHED] Healthy node {reassigned_ip} finished its task!")
                    printlog(f"[FAILOVER] Healthy node {reassigned_ip} finished during both-wait period.")
                    break

                if dashboard:
                    dashboard.add_message(
                        f"Listening for new nodes or healthy node completion ({remaining}s remaining)... Press 'k' to kill.",
                        timeout_seconds=2,
                    )

                if sys.stdin.isatty():
                    rlist, _, _ = select.select([sys.stdin], [], [], 0.0)
                    if rlist:
                        char = sys.stdin.read(1).lower()
                        if char == "k":
                            print("\nUser pressed 'k' during wait. Shutting down task...")
                            raise RuntimeError("Task execution killed by user during node wait period.")

                socket_map = {
                    t["connection"]: a_ip
                    for a_ip, t in active_tasks.items()
                    if a_ip != failed_ip and not t.get("exec_finished")
                }
                if socket_map:
                    readable, _, _ = select.select(list(socket_map.keys()), [], [], 0.5)
                    for sock in readable:
                        a_ip = socket_map[sock]
                        t = active_tasks[a_ip]
                        try:
                            msg = receive_json_message(sock)
                            if msg.get("type") == "finished":
                                t["exec_finished"] = True
                                exec_time = msg.get("execution_time", 0.0)
                                printlog(f"[EXECUTION COMPLETE] Node {a_ip} finished {t['part_filename']} in {exec_time:.2f}s")
                                if dashboard:
                                    dashboard.update_node(
                                        a_ip, f"node{t['node_index']}", t["part_filename"], PROGRESS_RECEIVING_FILES,
                                        receiving=True, executing=False, sending=False
                                    )
                        except Exception:
                            pass

                time.sleep(0.2)

            if not found_spare_ip and not reassigned_ip:
                print("\n[TIMEOUT] 60s elapsed with no new nodes. Falling back to waiting for healthy node to finish...")
                printlog("[RECOVERY FALLBACK] Falling back to waiting for healthy node to finish.")

                while True:
                    healthy_candidates = [ip for ip in active_tasks if ip != failed_ip]
                    if not healthy_candidates:
                        print("\n[ERROR] No healthy running nodes available to take over the task.")
                        break

                    for healthy_ip in healthy_candidates:
                        t = active_tasks[healthy_ip]
                        if t.get("exec_finished"):
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
                        rlist, _, _ = select.select([sys.stdin], [], [], 0.0)
                        if rlist:
                            char = sys.stdin.read(1).lower()
                            if char == "k":
                                print("\nUser pressed 'k' during wait. Shutting down task...")
                                raise RuntimeError("Task execution killed by user during node wait period.")

                    socket_map = {
                        t["connection"]: a_ip
                        for a_ip, t in active_tasks.items()
                        if a_ip != failed_ip and not t.get("exec_finished")
                    }
                    if socket_map:
                        readable, _, _ = select.select(list(socket_map.keys()), [], [], 0.5)
                        for sock in readable:
                            a_ip = socket_map[sock]
                            t = active_tasks[a_ip]
                            try:
                                msg = receive_json_message(sock)
                                if msg.get("type") == "finished":
                                    t["exec_finished"] = True
                            except Exception:
                                pass

                    time.sleep(0.2)

            if found_spare_ip:
                updated_mapping = swap_device_ip(failed_ip, found_spare_ip)
                printlog(f"[FAILOVER] Updated devices.json: {updated_mapping}")
                spare_connection = worker_pool.get_connection(found_spare_ip)
                daemon = distribute_part_file_to_single_node(
                    found_spare_ip, part_filename, spare_connection, parts_directory, master_ip_address
                )
                return found_spare_ip, spare_connection, daemon
            elif reassigned_ip:
                print(f"\n[TASK REASSIGNMENT] Node {reassigned_ip} finished its task and is taking over {part_filename}!")
                printlog(f"[REASSIGNMENT] Reassigning {part_filename} (node{node_index}) to healthy node {reassigned_ip}.")
                try:
                    updated_mapping = swap_device_ip(failed_ip, reassigned_ip)
                except Exception:
                    pass
                reassigned_conn = worker_pool.get_connection(reassigned_ip)
                if dashboard:
                    dashboard.update_node(
                        reassigned_ip, f"node{node_index}", part_filename, "reassigned",
                        receiving=False, executing=False, sending=False, connected=True
                    )
                daemon = distribute_part_file_to_single_node(
                    reassigned_ip, part_filename, reassigned_conn, parts_directory, master_ip_address
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
    """Swap IP in devices.json and reassign task to the next spare node or prompt user if no spare is available."""
    idle_ips = set()
    if active_tasks is not None:
        with worker_pool.lock:
            for ip in list(worker_pool.connections.keys()):
                base_ip = ip.split(":")[0]
                if ip not in active_tasks and base_ip not in active_tasks and ip not in worker_pool.killed_ips and base_ip not in worker_pool.killed_ips:
                    idle_ips.add(ip)

    for ip in list(spare_nodes):
        base_ip = ip.split(":")[0]
        if (active_tasks is None or (ip not in active_tasks and base_ip not in active_tasks)) and ip not in worker_pool.killed_ips and base_ip not in worker_pool.killed_ips:
            idle_ips.add(ip)

    spare_ip = None
    if idle_ips:
        resource_type = config.resource_type if config else "auto"
        idle_telemetry = {ip: worker_pool.worker_telemetry.get(ip, {}) for ip in idle_ips}
        ranked_ips = rank_workers_by_affinity(idle_telemetry, resource_type)
        if ranked_ips:
            spare_ip = ranked_ips[0]
            if spare_ip in spare_nodes:
                spare_nodes.remove(spare_ip)

    if spare_ip:
        printlog(f"\n Node {failed_ip} failed. Reassigning task to node{node_index}, {spare_ip}")
        worker_pool.mark_killed(failed_ip)
        updated_mapping = swap_device_ip(failed_ip, spare_ip)
        printlog(f"[FAILOVER] Updated devices.json: {updated_mapping}")

        if dashboard:
            dashboard.update_node(
                failed_ip, f"node{node_index} (killed)", "-", "killed",
                receiving=False, executing=False, sending=False, connected=False
            )

        spare_connection = worker_pool.get_connection(spare_ip)
        part_filename = part_filename_for_node(node_index)

        if dashboard:
            dashboard.update_node(
                spare_ip, f"node{node_index}", part_filename, "assigned",
                receiving=False, executing=False, sending=False, connected=True
            )

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
            config=config,
        )


def monitor_worker_executions_and_collect_results(
    active_nodes: list[str],
    worker_pool: WorkerConnectionPool,
    spare_nodes: list[str],
    parts_directory: str,
    master_ip_address: str | None = None,
    dashboard: StatusDashboard | None = None,
    config: Config | None = None,
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
                        dashboard.update_node(ip, f"node{node_index}", "-", "failed", receiving=False, executing=False, sending=False, connected=False)
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
                            if dashboard:
                                dashboard.add_message(f"Waiting for node {ip} to reconnect...", timeout_seconds=30)
                            retry_conn = worker_pool.get_connection(ip, timeout_seconds=30.0)

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
                        config=config,
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
                            ip, f"node{task['node_index']}", task["part_filename"], PROGRESS_FINISHED,
                            receiving=False, executing=False, sending=False, connected=True
                        )

        time.sleep(0.5)

    return active_daemons


def check_and_distribute_binaries(
    active_nodes: list[str],
    worker_pool: "WorkerConnectionPool",
    config: "Config",
    master_ip_address: str | None = None,
    dashboard: "StatusDashboard | None" = None,
) -> tuple[list[str], list[str]]:
    """Phase 0: Check binary presence on each worker, transfer if needed.

    Returns (binary_ready, binary_idle):
      - binary_ready: workers that have the correct binary (MD5-verified) or just received it.
      - binary_idle: workers kept idle because user declined transfer, master lacked the binary,
                     or a handshake error occurred.

    For each worker:
      1. Send binary_info_request (name of required binary).

      2. Receive binary_info (worker OS/arch, whether it has the binary, its MD5).
      3. Compare MD5 with the master's copy. If they match, skip transfer.
      4. If worker is missing the binary (or has a stale one):
           - Prompt user interactively: "Send binary to <ip>? [y/n]"
           - If y: start a file-transfer daemon, send the binary, wait for confirmation.
           - If n: mark worker as "binary_idle" — keep it in the pool but skip for tasks.
      5. Send binary_ready to every worker that now has a good binary.
    After processing all workers, re-sort active_nodes so binary-ready ones lead,
    and return only that re-sorted list (binary_idle workers are silently moved to
    spare and remain connected but receive no tasks).
    """
    if not config.require_binary or not config.binary_name:
        # Binary distribution disabled; all nodes are ready as-is.
        return active_nodes, []

    printlog("\n--- Phase 0: Binary Check & Distribution ---")
    if dashboard:
        dashboard.set_master_state("checking binaries")

    binary_ready: list[str] = []    # workers that have (or will have) the binary
    binary_idle: list[str] = []     # workers skipped by user — stay idle

    for worker_ip in active_nodes:
        try:
            connection = worker_pool.get_connection(worker_ip, timeout_seconds=15.0)

            # Step 1 — ask worker for its platform + binary status
            send_binary_info_request(connection, config.binary_name)

            # Step 2 — receive worker's platform + binary status
            info = receive_binary_info(connection)
            os_folder = info["os_name"]
            arch_folder = info["arch"]
            worker_has = info["has_binary"]
            worker_md5 = info.get("md5", "")

            # Step 3 — locate master's binary for that platform
            master_binary_path = get_binary_path(
                config.binaries_directory, os_folder, arch_folder, config.binary_name
            )

            if master_binary_path is None:
                printlog(
                    f"[BINARY] No binary for {os_folder}/{arch_folder} on master. "
                    f"Worker {worker_ip} will be kept idle."
                )
                print(
                    f"\n[BINARY] Master has no binary for {os_folder}/{arch_folder}. "
                    f"Worker {worker_ip} kept idle."
                )
                binary_idle.append(worker_ip)
                # No binary_ready sent — worker stays blocked until task phase ignores it
                continue

            master_md5 = compute_md5(str(master_binary_path))

            # Step 4 — decide if transfer is needed
            if worker_has and worker_md5 == master_md5:
                printlog(f"[BINARY] Worker {worker_ip} already has {config.binary_name} (MD5 match). Skipping transfer.")
                print(f"  [BINARY] Worker {worker_ip}: {config.binary_name} up-to-date. ✓")
                send_binary_ready(connection)
                binary_ready.append(worker_ip)
                continue

            # Worker is missing the binary or has a stale version — prompt user
            reason = "stale/different version" if worker_has else "not present"
            prompt_text = (
                f"\n[BINARY] Worker {worker_ip} ({os_folder}/{arch_folder}): "
                f"'{config.binary_name}' {reason}.\n"
                f"  Binary to send: {master_binary_path} ({master_binary_path.stat().st_size // 1024} KB)\n"
                f"  Send binary to {worker_ip}? [y/n]: "
            )
            if dashboard:
                dashboard.set_prompt(prompt_text)
            else:
                sys.stdout.write(prompt_text)
                sys.stdout.flush()

            # Read single-char answer (works in both tty and pipe)
            user_choice = ""
            if sys.stdin.isatty():
                try:
                    import termios, tty as _tty
                    fd = sys.stdin.fileno()
                    old = termios.tcgetattr(fd)
                    _tty.setcbreak(fd)
                    try:
                        user_choice = sys.stdin.read(1).lower()
                        print(user_choice)
                    finally:
                        termios.tcsetattr(fd, termios.TCSADRAIN, old)
                except Exception:
                    user_choice = sys.stdin.readline().strip().lower()[:1]
            else:
                user_choice = sys.stdin.readline().strip().lower()[:1]
                
            if dashboard:
                dashboard.set_prompt(None)

            if user_choice != "y":
                printlog(f"[BINARY] User declined to send binary to {worker_ip}. Worker kept idle.")
                print(f"  [BINARY] Skipped. Worker {worker_ip} will be kept idle.")
                binary_idle.append(worker_ip)
                # Don't send binary_ready — worker stays blocked waiting
                continue

            # User said yes — transfer binary via HTTP daemon
            printlog(f"[BINARY] Transferring {config.binary_name} to {worker_ip} ({os_folder}/{arch_folder})...")
            print(f"  [BINARY] Sending {config.binary_name} to {worker_ip}...")
            if dashboard:
                dashboard.add_message(f"Sending binary '{config.binary_name}' to {worker_ip}", timeout_seconds=30)

            reachable_master_ip = master_ip_address or get_local_ip_for_peer(worker_ip)
            binary_daemon = start_file_transfer_daemon(
                allocated_filenames=[master_binary_path.name],
                serve_directory=str(master_binary_path.parent),
                output_directory=str(master_binary_path.parent),
            )
            if binary_daemon.port is None:
                raise RuntimeError("Binary file-transfer daemon did not report a port")

            # Tell the worker to fetch the binary, then send binary_ready
            send_ready_message(connection, reachable_master_ip, binary_daemon.port)
            receive_file_received_message(connection)
            send_binary_ready(connection)
            binary_daemon.stop()

            printlog(f"[BINARY] Binary sent to {worker_ip}. MD5={master_md5}")
            print(f"  [BINARY] Transfer complete. Worker {worker_ip} is ready. ✓")
            binary_ready.append(worker_ip)

        except (TimeoutError, ConnectionError, OSError, RuntimeError, ValueError) as exc:
            printlog(f"[BINARY] Error during binary handshake with {worker_ip}: {exc}")
            print(f"  [BINARY] Binary handshake failed for {worker_ip}: {exc}. Kept idle.")
            binary_idle.append(worker_ip)

    printlog(
        f"[BINARY] Phase 0 complete. "
        f"Ready: {binary_ready}, Idle (no binary): {binary_idle}"
    )
    if binary_idle:
        print(f"\n[BINARY] Workers kept idle (no binary): {', '.join(binary_idle)}")
    # Return both lists: caller uses binary_ready for tasks, binary_idle stays connected but idle.
    return binary_ready, binary_idle



def run_master() -> None:
    """Master node entry point: load config, validate addresses, split video, distribute parts, track execution, collect results, and merge."""
    arguments = parse_master_arguments()

    # Discover available config files and determine initial selection
    available_configs = get_available_configs()
    if not available_configs:
        print("Error: No .ini configuration files found.", file=sys.stderr)
        sys.exit(1)

    if arguments.config:
        try:
            initial_config = str(Path(arguments.config).resolve().relative_to(Path(".").resolve()))
        except ValueError:
            initial_config = arguments.config
    else:
        initial_config = available_configs[0]

    if initial_config not in available_configs:
        available_configs.insert(0, initial_config)

    # master/input holds split parts; master/output holds merged results.
    Path(arguments.parts_directory).mkdir(parents=True, exist_ok=True)
    Path(MASTER_OUTPUT_DIRECTORY).mkdir(parents=True, exist_ok=True)

    dashboard = StatusDashboard()
    dashboard.set_available_configs(available_configs, initial_config)

    # Move Network Listener Initialization HERE (Outside the loop)
    mdns_announcer = MdnsAnnouncer(
        fqdn=MDNS_MASTER_FQDN, service=MDNS_SERVICE_MASTER
    )
    mdns_announcer.start()

    print(f"Listening on control port {CONTROL_PORT}...")
    daemons: list[FileTransferDaemon] = []
    listening_socket = open_listening_socket(FIXED_PORT)
    worker_pool = WorkerConnectionPool(
        arguments.worker_ip_addresses, listening_socket, dashboard=dashboard, password=arguments.password
    )

    def _master_sig_handler(signum, frame):
        print(f"\nReceived signal {signum}. Cleaning up master temporary files and exiting...", file=sys.stderr)
        if worker_pool:
            try:
                worker_pool.shutdown_all_workers()
                worker_pool.stop()
            except Exception:
                pass
        for daemon in daemons:
            try:
                daemon.stop()
            except Exception:
                pass
        if listening_socket:
            try:
                listening_socket.close()
            except Exception:
                pass
        cleanup_master_temporary_files(
            parts_directory=arguments.parts_directory,
            output_directory=MASTER_OUTPUT_DIRECTORY,
            input_video=arguments.input_video,
        )
        sys.exit(128 + signum)

    try:
        signal.signal(signal.SIGTERM, _master_sig_handler)
        signal.signal(signal.SIGINT, _master_sig_handler)
    except Exception:
        pass

    try:
        while True:
            # Phase 1: Config Selection Phase
            if not arguments.config:
                dashboard.set_master_state("config selection")
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
                        new_cfg = dashboard.get_selected_config()
                        dashboard.set_prompt(f"Select config: {new_cfg}.\nUse 'w'/'s' to navigate. Press 'y' to confirm, 'q' to quit: ")

                        if fd is not None:
                            rlist, _, _ = select.select([fd], [], [], 0.5)
                            if rlist:
                                char = os.read(fd, 1).decode(errors="replace").lower()
                                if char == "w":
                                    dashboard.move_config_selection(-1)
                                elif char == "s":
                                    dashboard.move_config_selection(1)
                                elif char in ("y", "\n", "\r"):
                                    dashboard.set_prompt(None)
                                    dashboard.add_message(f"Selected config: {new_cfg}", timeout_seconds=3)
                                    break
                                elif char == "q":
                                    print("\nUser pressed 'q'. Exiting selection...")
                                    sys.exit(0)
                        else:
                            break
                        time.sleep(0.5)
                finally:
                    dashboard.set_prompt(None)
                    if fd is not None and old_settings is not None:
                        try:
                            import termios
                            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                        except Exception:
                            pass

            selected_config_path = dashboard.get_selected_config()
            try:
                config = load_config(selected_config_path)
            except ValueError as exc:
                print(f"Config error: {exc}", file=sys.stderr)
                sys.exit(1)

            print(f"Loaded config from {selected_config_path}")

            if not arguments.worker_ip_addresses:
                # Device Discovery Phase on Dashboard
                dashboard.set_master_state("device discovery")
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
                    while True:
                        now = time.time()
                        remaining = int(max(0.0, deadline - now))
                        with worker_pool.lock:
                            connected_ips = list(worker_pool.connections.keys())

                        dashboard.set_prompt(
                            f"Discovered {len(connected_ips)} worker(s). Config: '{selected_config_path}'.\nPress 'y' to continue, 'q' to quit ({remaining}s remaining): "
                        )

                        if fd is not None:
                            rlist, _, _ = select.select([fd], [], [], 0.5)
                            if rlist:
                                char = os.read(fd, 1).decode(errors="replace").lower()
                                if char in ("y", "\n", "\r"):
                                    if len(connected_ips) < config.min_devices:
                                        dashboard.add_message(f"Need at least {config.min_devices} workers (discovered {len(connected_ips)} so far). Waiting...", timeout_seconds=5)
                                    else:
                                        break
                                elif char == "q":
                                    print("\nUser pressed 'q'. Exiting selection...")
                                    sys.exit(0)
                        else:
                            if len(connected_ips) >= config.min_devices:
                                break

                        if now >= deadline:
                            if len(connected_ips) < config.min_devices:
                                print(f"\n[DISCOVERY TIMEOUT] Fewer than {config.min_devices} workers discovered within {int(timeout_seconds)}s. Exiting safely.", file=sys.stderr)
                                sys.exit(0)
                            else:
                                print(f"\n[SELECTION TIMEOUT] Timeout reached ({int(timeout_seconds)}s). Proceeding with {len(connected_ips)} workers.")
                                break
                        time.sleep(0.5)
                finally:
                    dashboard.set_prompt(None)
                    if fd is not None and old_settings is not None:
                        try:
                            import termios
                            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                        except Exception:
                            pass

            with worker_pool.lock:
                connected_ips = list(worker_pool.connections.keys())

            if arguments.worker_ip_addresses:
                worker_addresses_to_use = arguments.worker_ip_addresses
            else:
                worker_addresses_to_use = connected_ips

            with worker_pool.lock:
                telemetry = dict(worker_pool.worker_telemetry)
            
            ranked = rank_workers_by_affinity(telemetry, config.resource_type, config.min_vram_mb)
            sorted_addresses = [ip for ip in ranked if ip in worker_addresses_to_use]
            for ip in worker_addresses_to_use:
                if ip not in sorted_addresses:
                    sorted_addresses.append(ip)
            worker_addresses_to_use = sorted_addresses

            try:
                validate_worker_address_count(worker_addresses_to_use, config.min_devices)
            except ValueError as exc:
                print(f"Startup error: {exc}", file=sys.stderr)
                sys.exit(1)

            active_nodes, spare_nodes = split_active_and_spare_addresses(worker_addresses_to_use, config.max_nodes)
            device_mapping = write_devices_json(active_nodes)
            print(f"Active nodes ({len(active_nodes)}): {', '.join(active_nodes)}")
            if spare_nodes:
                print(f"Spare nodes ({len(spare_nodes)}): {', '.join(spare_nodes)}")
            else:
                print("Spare nodes: None")

            dashboard.total_parts = len(active_nodes)
            dashboard.reset_nodes()
            for idx, ip in enumerate(active_nodes, start=1):
                is_conn = ip in worker_pool.connections
                tel = worker_pool.worker_telemetry.get(ip, {})
                dashboard.update_node(
                    ip, f"node{idx}", "-", "idle" if is_conn else "disconnected", connected=is_conn,
                    hostname=tel.get("hostname"), username=tel.get("username")
                )
            for ip in spare_nodes:
                is_conn = ip in worker_pool.connections
                tel = worker_pool.worker_telemetry.get(ip, {})
                dashboard.update_node(
                    ip, "spare", "-", "idle" if is_conn else "disconnected", connected=is_conn,
                    hostname=tel.get("hostname"), username=tel.get("username")
                )

            print(f"Waiting for active worker connections ({', '.join(active_nodes)})...")
            for ip in active_nodes:
                worker_pool.get_connection(ip)
            print("All active workers connected!")

            # Phase 0: Binary check
            binary_ready_nodes, binary_idle_nodes = check_and_distribute_binaries(
                active_nodes, worker_pool, config, master_ip_address=arguments.master_ip, dashboard=dashboard
            )
            active_nodes = binary_ready_nodes
            if not active_nodes:
                print("[BINARY] No workers have the required binary. Cannot proceed.", file=sys.stderr)
                sys.exit(1)

            dashboard.set_master_state("splitting file")
            run_split_command(
                config.split_command, len(active_nodes), input_video=arguments.input_video, parts_directory=arguments.parts_directory
            )
            verify_part_files(
                len(active_nodes), parts_directory=arguments.parts_directory, split_command=config.split_command
            )

            for idx, ip in enumerate(active_nodes, start=1):
                dashboard.update_node(
                    ip, f"node{idx}", part_filename_for_node(idx, split_command=config.split_command, parts_directory=arguments.parts_directory), "sending file",
                    receiving=False, executing=False, sending=True
                )

            initial_daemons = distribute_part_files_to_active_nodes(
                active_nodes, worker_pool, arguments.parts_directory, master_ip_address=arguments.master_ip, dashboard=dashboard
            )
            daemons.extend(initial_daemons)
            printlog("\nPhase 2 complete: split files distributed to all active workers.")

            dashboard.set_master_state("executing")
            failover_daemons = monitor_worker_executions_and_collect_results(
                active_nodes, worker_pool, spare_nodes, arguments.parts_directory, master_ip_address=arguments.master_ip, dashboard=dashboard, config=config
            )
            daemons.extend(failover_daemons)
            printlog("\nAll part files received.")

            dashboard.set_master_state("merging files")
            run_merge_command(
                config.merge_command, len(active_nodes), output_directory=MASTER_OUTPUT_DIRECTORY, split_command=config.split_command
            )
            dashboard.set_master_state("finished")
            printlog("\nAll tasks and merging completed successfully.")

            # Multi-job prompt
            print("\nAll tasks completed successfully!")
            try:
                ans = input("Do you want to process another job? [y/N]: ").strip().lower()
            except EOFError:
                ans = 'n'

            if ans == 'y':
                arguments.config = None
                continue
            else:
                print("Exiting master.")
                worker_pool.shutdown_all_workers()
                break

    except KeyboardInterrupt:
        print("\nMaster shutting down.")
    except (OSError, RuntimeError, ValueError, FileNotFoundError) as exc:
        print(f"Master error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        if worker_pool:
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
        if listening_socket:
            try:
                listening_socket.close()
            except Exception:
                pass
        cleanup_master_temporary_files(
            parts_directory=arguments.parts_directory,
            output_directory=MASTER_OUTPUT_DIRECTORY,
            input_video=arguments.input_video,
        )

if __name__ == "__main__":
    run_master()
