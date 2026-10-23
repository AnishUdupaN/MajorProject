"""Worker task execution lifecycle, status reporting, and result uploading."""

from __future__ import annotations

import socket
from pathlib import Path
from typing import TYPE_CHECKING

from core.constants import (
    PROGRESS_FINISHED,
    WORKER_OUTPUT_DIRECTORY,
)
from core.dashboard import printlog
from core.execution import run_execute_command
from core.file_transfer import upload_result_file
from core.messaging import (
    MasterShutdownError,
    receive_json_message,
    receive_ready_message,
    send_failed_message,
    send_file_received_message,
    send_finished_message,
)
from worker.binary_manager import handle_binary_handshake_request
from worker.task_fetcher import fetch_allocated_part_file

if TYPE_CHECKING:
    from core.config import Config


def run_worker_task(
    connection: socket.socket,
    master_ip_address: str,
    download_directory: str,
    execute_command_template: str,
    simulate_failure_after: float | None = None,
    config: Config | None = None,
    early_ready_msg: dict | None = None,
) -> None:
    """Handle ready message, fetch allocated part file, run execute_command, send finished signal, and upload result."""
    downloaded_path = None
    output_filepath = None
    output_dir = Path(WORKER_OUTPUT_DIRECTORY)
    uploaded_names: set[str] = set()

    try:
        if early_ready_msg:
            ready_message = early_ready_msg
        else:
            msg = receive_json_message(connection, timeout=600.0)
            if msg.get("type") == "shutdown":
                raise MasterShutdownError("Master sent shutdown signal")
            if msg.get("type") == "binary_info_request":
                handle_binary_handshake_request(
                    connection, master_ip_address, msg, config=config
                )
                ready_message = receive_ready_message(connection, timeout=600.0)
            elif msg.get("type") == "ready":
                ready_message = msg
            else:
                raise ValueError(
                    f"Expected ready or binary_info_request, got: {msg!r}"
                )

        file_transfer_port = ready_message["file_transfer_port"]
        execute_cmd = ready_message.get(
            "execute_command", execute_command_template
        )

        print("\n[STATUS] Receiving file from master...")
        downloaded_path = fetch_allocated_part_file(
            master_ip_address,
            file_transfer_port,
            download_directory,
        )
        print(f"[STATUS] Received part file: {downloaded_path}")

        send_file_received_message(connection)

        print("\n[STATUS] Task is running...")
        try:
            elapsed_time = run_execute_command(
                execute_cmd,
                input_file=downloaded_path,
                output_directory=WORKER_OUTPUT_DIRECTORY,
                connection=connection,
                simulate_failure_after=simulate_failure_after,
                config=config,
            )
        except (KeyboardInterrupt, RuntimeError, ValueError) as exc:
            try:
                send_failed_message(connection, str(exc))
            except Exception:
                pass
            raise

        print("[STATUS] Task complete.")

        part_filename = Path(downloaded_path).name
        output_dir = Path(WORKER_OUTPUT_DIRECTORY)

        # Send finished signal to Master over control port BEFORE upload starts
        send_finished_message(connection, elapsed_time, part_filename)

        # Upload ALL files in output directory to master daemon
        output_files = (
            sorted(output_dir.iterdir()) if output_dir.is_dir() else []
        )
        uploaded_names = set()
        if output_files:
            print("\n[STATUS] Sending result file(s) to master...")
            for out_file in output_files:
                if out_file.is_file():
                    upload_result_file(
                        master_ip_address, file_transfer_port, str(out_file)
                    )
                    uploaded_names.add(out_file.name)
                    print(f"[STATUS] Sent result {out_file.name} to master.")

        # Upload a zero-byte sentinel named after part file so master polling finds it
        if part_filename not in uploaded_names:
            sentinel_dir = output_dir
            sentinel_dir.mkdir(parents=True, exist_ok=True)
            sentinel_path = sentinel_dir / part_filename
            sentinel_path.write_bytes(b"")
            try:
                upload_result_file(
                    master_ip_address, file_transfer_port, str(sentinel_path)
                )
                print(
                    f"[STATUS] Sent completion sentinel {part_filename} to master."
                )
            finally:
                try:
                    sentinel_path.unlink()
                except Exception:
                    pass

        output_filepath = output_dir / part_filename
        print(f"{PROGRESS_FINISHED} for worker ({part_filename})")

    finally:
        if downloaded_path:
            try:
                p = Path(downloaded_path)
                if p.is_file():
                    p.unlink()
                    printlog(
                        f"Cleaned up worker downloaded part file: {downloaded_path}"
                    )
            except Exception:
                pass
        if output_filepath:
            try:
                p = Path(output_filepath)
                if p.is_file():
                    p.unlink()
                    printlog(
                        f"Cleaned up worker output part file: {output_filepath}"
                    )
            except Exception:
                pass
        for fname in uploaded_names:
            try:
                p = output_dir / fname
                if p.is_file():
                    p.unlink()
            except Exception:
                pass
