"""Phase 0 binary dependency verification, user prompts, and HTTP distribution."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from core.binaries import compute_md5, get_binary_path
from core.dashboard import StatusDashboard, printlog
from core.discovery import get_local_ip_for_peer
from core.file_transfer import start_file_transfer_daemon
from core.messaging import (
    receive_binary_info,
    receive_file_received_message,
    send_binary_info_request,
    send_binary_ready,
    send_ready_message,
)

if TYPE_CHECKING:
    from core.config import Config
    from master.connection_pool import WorkerConnectionPool


def check_and_distribute_binaries(
    active_nodes: list[str],
    worker_pool: WorkerConnectionPool,
    config: Config,
    master_ip_address: str | None = None,
    dashboard: StatusDashboard | None = None,
) -> tuple[list[str], list[str]]:
    """Phase 0: Check binary presence on each worker, transfer if needed.

    Returns (binary_ready, binary_idle):
      - binary_ready: workers that have the correct binary (MD5-verified) or just received it.
      - binary_idle: workers kept idle because user declined transfer, master lacked the binary,
                     or a handshake error occurred.
    """
    if not config.binary_name:
        return active_nodes, []

    printlog("\n--- Phase 0: Binary Check & Distribution ---")
    if dashboard:
        dashboard.set_master_state("checking binaries")

    binary_ready: list[str] = []
    binary_idle: list[str] = []

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

            # Step 3 — decide if transfer is needed
            if worker_has and not config.require_binary:
                printlog(
                    f"[BINARY] Worker {worker_ip} has {config.binary_name} locally (require_binary=false). Skipping transfer."
                )
                print(
                    f"  [BINARY] Worker {worker_ip}: {config.binary_name} present locally. ✓"
                )
                send_binary_ready(connection)
                binary_ready.append(worker_ip)
                continue

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
                continue

            master_md5 = compute_md5(str(master_binary_path))

            if worker_has and worker_md5 == master_md5:
                printlog(
                    f"[BINARY] Worker {worker_ip} already has {config.binary_name} (MD5 match). Skipping transfer."
                )
                print(
                    f"  [BINARY] Worker {worker_ip}: {config.binary_name} up-to-date. ✓"
                )
                send_binary_ready(connection)
                binary_ready.append(worker_ip)
                continue

            # Worker is missing binary or has stale version — prompt user
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

            # Read single-char answer
            user_choice = ""
            if dashboard and getattr(dashboard, "webserver_mode", False):
                dashboard.pending_input_event.clear()
                dashboard.pending_input_event.wait()
                ans = dashboard.pending_input_answer
                user_choice = "y" if ans == "confirm" else "n"
            elif sys.stdin.isatty():
                try:
                    import termios
                    import tty as _tty

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
                printlog(
                    f"[BINARY] User declined to send binary to {worker_ip}. Worker kept idle."
                )
                print(
                    f"  [BINARY] Skipped. Worker {worker_ip} will be kept idle."
                )
                binary_idle.append(worker_ip)
                continue

            # User said yes — transfer binary via HTTP daemon
            printlog(
                f"[BINARY] Transferring {config.binary_name} to {worker_ip} ({os_folder}/{arch_folder})..."
            )
            print(f"  [BINARY] Sending {config.binary_name} to {worker_ip}...")
            if dashboard:
                dashboard.add_message(
                    f"Sending binary '{config.binary_name}' to {worker_ip}",
                    timeout_seconds=30,
                )

            reachable_master_ip = master_ip_address or get_local_ip_for_peer(
                worker_ip
            )
            binary_daemon = start_file_transfer_daemon(
                allocated_filenames=[master_binary_path.name],
                serve_directory=str(master_binary_path.parent),
                output_directory=str(master_binary_path.parent),
            )
            if binary_daemon.port is None:
                raise RuntimeError("Binary file-transfer daemon did not report a port")

            send_ready_message(connection, reachable_master_ip, binary_daemon.port)
            receive_file_received_message(connection)
            send_binary_ready(connection)
            binary_daemon.stop()

            printlog(f"[BINARY] Binary sent to {worker_ip}. MD5={master_md5}")
            print(
                f"  [BINARY] Transfer complete. Worker {worker_ip} is ready. ✓"
            )
            binary_ready.append(worker_ip)

        except (
            TimeoutError,
            ConnectionError,
            OSError,
            RuntimeError,
            ValueError,
        ) as exc:
            printlog(f"[BINARY] Error during binary handshake with {worker_ip}: {exc}")
            print(
                f"  [BINARY] Binary handshake failed for {worker_ip}: {exc}. Kept idle."
            )
            binary_idle.append(worker_ip)

    printlog(
        f"[BINARY] Phase 0 complete. "
        f"Ready: {binary_ready}, Idle (no binary): {binary_idle}"
    )
    if binary_idle:
        print(f"\n[BINARY] Workers kept idle (no binary): {', '.join(binary_idle)}")
    return binary_ready, binary_idle
