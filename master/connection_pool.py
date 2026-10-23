"""Connection management, thread-safe pooling, and worker socket operations."""

from __future__ import annotations

import select
import socket
import threading
import time

from core.dashboard import StatusDashboard, printlog
from core.messaging import (
    generate_pin,
    get_shared_secret,
    receive_pin_response,
    receive_worker_telemetry,
    send_pin_accepted,
    send_pin_challenge,
    send_pin_rejected,
    send_shutdown_message,
    verify_auth_message,
)


def open_listening_socket(port: int) -> socket.socket:
    """Open a listening socket on the fixed port."""
    listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listening_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listening_socket.bind(("0.0.0.0", port))
    listening_socket.listen()
    return listening_socket


def safe_select_sockets(
    sockets: list[socket.socket], timeout: float = 0.5
) -> list[socket.socket]:
    """Safely select readable sockets, silently dropping closed or invalid sockets (fileno < 0)."""
    valid = []
    for s in sockets:
        try:
            if s is not None and getattr(s, "fileno", lambda: -1)() >= 0:
                valid.append(s)
        except Exception:
            pass
    if not valid:
        return []
    try:
        readable, _, _ = select.select(valid, [], [], timeout)
        return readable
    except (ValueError, OSError):
        readable = []
        for s in valid:
            try:
                if s.fileno() >= 0:
                    r, _, _ = select.select([s], [], [], 0.0)
                    if r:
                        readable.append(s)
            except Exception:
                pass
        return readable


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
        if self.dashboard:
            self.dashboard.session_pin = self.session_pin
        self.worker_telemetry: dict[str, dict] = {}
        self.killed_ips: set[str] = set()
        self.worker_id_to_key: dict[str, str] = {}
        self.key_to_worker_id: dict[str, str] = {}
        self.worker_slot_counter: int = 1
        self.lock = threading.Lock()
        self.running = True
        self._shared_secret = get_shared_secret()
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def mark_killed(self, key: str) -> None:
        """Mark a worker key as permanently killed after failover so reconnects are rejected."""
        with self.lock:
            self.killed_ips.add(key)
            conn = self.connections.pop(key, None)
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            if self.dashboard:
                existing = self.dashboard.node_states.get(key, {})
                node_id = existing.get("node_id", "worker")
                if "(killed)" not in node_id:
                    node_id = f"{node_id} (killed)"
                filename = existing.get("filename", "-")
                self.dashboard.update_node(
                    key,
                    node_id,
                    filename,
                    "killed",
                    receiving=False,
                    executing=False,
                    sending=False,
                    connected=False,
                )

    def remove_connection(self, key: str) -> None:
        """Remove a dead or closed socket connection for key from the pool."""
        with self.lock:
            conn = self.connections.pop(key, None)
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            if self.dashboard and key in self.dashboard.node_states:
                existing = self.dashboard.node_states.get(key, {})
                node_id = existing.get("node_id", "worker")
                filename = existing.get("filename", "-")
                self.dashboard.update_node(
                    key,
                    node_id,
                    filename,
                    "disconnected",
                    receiving=False,
                    executing=False,
                    sending=False,
                    connected=False,
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

                if (
                    self.allowed_ips
                    and ip not in self.allowed_ips
                    and endpoint not in self.allowed_ips
                ):
                    conn.close()
                    continue
                if self._shared_secret:
                    if not verify_auth_message(conn, self._shared_secret):
                        printlog(f"Worker {endpoint} failed authentication. Rejecting.")
                        conn.close()
                        continue
                else:
                    msg = f"Worker {ip} pairing request (PIN: {self.session_pin})"
                    printlog(msg)
                    import socket as _sock

                    send_pin_challenge(conn, _sock.gethostname())

                    attempts_left = 3
                    paired = False
                    while attempts_left > 0:
                        try:
                            conn.settimeout(300.0)  # 300-second window to enter PIN
                            resp = receive_pin_response(conn)
                            conn.settimeout(None)
                            if resp["pin"] == self.session_pin:
                                send_pin_accepted(conn)
                                paired = True
                                break
                            else:
                                attempts_left -= 1
                                send_pin_rejected(
                                    conn, "Invalid PIN/Password", attempts_left
                                )
                        except Exception:
                            break
                    if not paired:
                        printlog(
                            f"Worker {endpoint} failed authentication pairing. Rejecting."
                        )
                        conn.close()
                        continue

                try:
                    telemetry = receive_worker_telemetry(conn)
                except Exception as e:
                    printlog(f"Worker {endpoint} failed to send telemetry: {e}")
                    conn.close()
                    continue

                worker_id = telemetry.get("worker_id")
                with self.lock:
                    node_key = None
                    if worker_id and worker_id in self.worker_id_to_key:
                        node_key = self.worker_id_to_key[worker_id]
                        printlog(
                            f"Worker {worker_id} recognized as existing node '{node_key}'."
                        )
                    else:
                        # Check if there is an existing disconnected slot matching this worker on this IP
                        if self.dashboard:
                            for k, st in self.dashboard.node_states.items():
                                if (
                                    not st.get("connected")
                                    and k not in self.killed_ips
                                    and k not in self.connections
                                ):
                                    k_ip = k.split(":")[0]
                                    if k_ip == ip:
                                        node_key = k
                                        printlog(
                                            f"Binding reconnecting worker to disconnected node slot '{node_key}'."
                                        )
                                        break

                        if not node_key:
                            # Brand new worker: determine a stable node_key
                            known_keys = set(
                                list(self.connections.keys())
                                + list(self.worker_id_to_key.values())
                            )
                            if self.dashboard:
                                known_keys.update(self.dashboard.node_states.keys())

                            if ip not in known_keys and ip not in self.connections:
                                node_key = ip
                            else:
                                slot = (
                                    worker_id[-4:]
                                    if worker_id
                                    else f"{self.worker_slot_counter}"
                                )
                                self.worker_slot_counter += 1
                                node_key = f"{ip}:{slot}"

                        if worker_id:
                            self.worker_id_to_key[worker_id] = node_key
                            self.key_to_worker_id[node_key] = worker_id

                    if node_key in self.killed_ips:
                        printlog(
                            f"Worker {node_key} ({worker_id}) is KILLED. Rejecting reconnect."
                        )
                        conn.close()
                        continue

                    # Replace old connection if any
                    old_c = self.connections.pop(node_key, None)
                    if old_c:
                        try:
                            old_c.close()
                        except Exception:
                            pass

                    self.connections[node_key] = conn
                    self.worker_telemetry[node_key] = telemetry
                    self.worker_telemetry[ip] = telemetry
                    printlog(
                        f"\nWorker {endpoint} registered as key '{node_key}', kept IDLE."
                    )

                    if self.dashboard:
                        existing = self.dashboard.node_states.get(node_key, {})
                        node_id = existing.get("node_id", "worker")
                        filename = existing.get("filename", "-")
                        was_connected = (
                            existing.get("connected", False) if existing else False
                        )
                        state = existing.get("state", "idle")
                        if (
                            state in ("disconnected", "killed", "failed")
                            or not was_connected
                        ):
                            state = "idle"
                        self.dashboard.update_node(
                            node_key,
                            node_id,
                            filename,
                            state,
                            receiving=False,
                            executing=False,
                            sending=False,
                            connected=True,
                            hostname=telemetry.get("hostname"),
                            username=telemetry.get("username"),
                        )
                        msg_action = (
                            "reconnected"
                            if (existing and not was_connected)
                            else "connected"
                        )
                        self.dashboard.add_message(
                            f"Node {node_key} {msg_action} ({node_id})",
                            timeout_seconds=5,
                        )
            except Exception:
                pass

    def get_connection(
        self, key: str, timeout_seconds: float = 30.0
    ) -> socket.socket:
        """Fetch connection for key from pool, waiting if not yet connected."""
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            with self.lock:
                if key in self.connections:
                    return self.connections[key]
                # If key has no port and there is exactly one connection on that IP
                base_ip = key.split(":")[0]
                matching = [
                    c
                    for k, c in self.connections.items()
                    if k == key or k == base_ip
                ]
                if len(matching) == 1 and ":" not in key:
                    return matching[0]
            time.sleep(0.2)
        raise TimeoutError(f"Worker {key} did not connect within {timeout_seconds}s")

    def shutdown_all_workers(self) -> None:
        """Broadcast shutdown message to all connected workers in the pool."""
        with self.lock:
            for key, conn in list(self.connections.items()):
                try:
                    send_shutdown_message(conn)
                except Exception:
                    pass
        time.sleep(0.2)

    def stop(self) -> None:
        self.running = False
