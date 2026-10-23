"""Worker binary caching, executable permissions, and master binary handshake."""

from __future__ import annotations

import os
import shutil
import socket
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from core.binaries import (
    compute_md5,
    get_worker_platform,
    resolve_worker_binary_cache_path,
)
from core.file_transfer import request_file, request_file_list
from core.messaging import (
    MESSAGE_TYPE_BINARY_INFO_REQUEST,
    MESSAGE_TYPE_BINARY_READY,
    MESSAGE_TYPE_READY,
    MESSAGE_TYPE_SHUTDOWN,
    MasterShutdownError,
    receive_binary_ready,
    receive_json_message,
    send_binary_info,
    send_file_received_message,
)

if TYPE_CHECKING:
    from core.config import Config


def ensure_executable(path: Path) -> None:
    """Ensure that the binary file has executable permissions."""
    if not path.is_file():
        return
    try:
        if not os.access(path, os.X_OK):
            path.chmod(0o755)
            if not os.access(path, os.X_OK):
                raise PermissionError(
                    f"Failed to set executable permissions on {path}"
                )
    except Exception as e:
        print(
            f"\n[ERROR] Failed to make binary '{path.name}' executable: {e}",
            file=sys.stderr,
        )
        sys.exit(1)


def handle_binary_handshake_request(
    connection: socket.socket,
    master_ip_address: str,
    req: dict,
    config: Config | None = None,
) -> None:
    """Handle an incoming binary_info_request from master: check binary cache and fetch if needed."""
    binary_name = req["binary_name"]
    os_folder, arch_folder = get_worker_platform()

    cache_path = resolve_worker_binary_cache_path(
        os_folder, arch_folder, binary_name
    )
    ensure_executable(cache_path)
    has_binary = cache_path.is_file()
    local_md5 = compute_md5(str(cache_path)) if has_binary else None

    if not has_binary and (config is None or not config.require_binary):
        if shutil.which(binary_name):
            has_binary = True
            local_md5 = "system"

    print(
        f"[BINARY] Platform: {os_folder}/{arch_folder}. "
        f"Binary '{binary_name}': {'present (MD5=' + str(local_md5) + ')' if has_binary else 'not found'}."
    )

    send_binary_info(
        connection, os_folder, arch_folder, binary_name, has_binary, local_md5
    )

    msg = receive_json_message(connection)

    if msg.get("type") == MESSAGE_TYPE_BINARY_READY:
        print(f"[BINARY] Master acknowledged binary '{binary_name}'. Proceeding.")
        return

    if msg.get("type") == MESSAGE_TYPE_READY:
        file_transfer_port = msg["file_transfer_port"]
        master_addr = msg.get("master_ip_address", master_ip_address)
        print(
            f"[BINARY] Receiving binary '{binary_name}' from master on port {file_transfer_port}..."
        )
        allocated_files = request_file_list(master_addr, file_transfer_port)
        for fname in allocated_files:
            dest = request_file(
                master_addr, file_transfer_port, fname, str(cache_path.parent)
            )
            dest_path = Path(dest)
            expected = cache_path.parent / binary_name
            if dest_path != expected and dest_path.is_file():
                dest_path.rename(expected)
        send_file_received_message(connection)

        if cache_path.is_file():
            ensure_executable(cache_path)

        print(f"[BINARY] Binary '{binary_name}' saved to {cache_path}. ✓")

        receive_binary_ready(connection)
        print("[BINARY] Binary ready confirmed.")
        return

    raise ValueError(
        f"[BINARY] Unexpected message during binary handshake: {msg!r}"
    )


def do_binary_handshake(
    connection: socket.socket,
    master_ip_address: str,
    config: Config | None = None,
) -> dict | None:
    """Phase 0 worker side: wait for master's initial decision.

    If master sends binary_info_request, perform binary handshake and return None.
    If master sends ready directly (binary check skipped), return the ready message.
    """
    msg = receive_json_message(connection)

    if msg.get("type") == MESSAGE_TYPE_SHUTDOWN:
        raise MasterShutdownError("Master sent shutdown signal")

    if msg.get("type") == MESSAGE_TYPE_READY:
        print(
            "[BINARY] Master skipped binary handshake. Proceeding directly to task."
        )
        return msg

    if msg.get("type") == MESSAGE_TYPE_BINARY_INFO_REQUEST:
        handle_binary_handshake_request(
            connection, master_ip_address, msg, config=config
        )
        return None

    raise ValueError(
        f"[BINARY] Unexpected message during initial handshake: {msg!r}"
    )
