"""Worker node entry point: connect to master, download part file, execute task, and send finished signal."""

import argparse
import signal
import socket
import sys
import time
from pathlib import Path


from core import (
    CONTROL_PORT,
    FIXED_PORT,
    MDNS_SERVICE_WORKER,
    MDNS_WORKER_FQDN,
    PROGRESS_FINISHED,
    WORKER_INPUT_DIRECTORY,
    WORKER_OUTPUT_DIRECTORY,
    Config,
    MasterShutdownError,
    MdnsAnnouncer,
    cleanup_worker_temporary_files,
    compute_md5,
    printlog,
    discover_master_ip,
    discover_master_udp,
    get_worker_platform,
    get_available_configs,
    load_config,
    receive_binary_info_request,
    receive_binary_ready,
    receive_ready_message,
    request_file,
    request_file_list,
    resolve_worker_binary_cache_path,
    run_execute_command,
    get_shared_secret,
    send_auth_message,
    send_binary_info,
    send_failed_message,
    send_file_received_message,
    send_finished_message,
    upload_result_file,
    receive_pin_challenge,
    send_pin_response,
    receive_json_message,
    MESSAGE_TYPE_PIN_ACCEPTED,
    MESSAGE_TYPE_PIN_REJECTED,
    run_node_benchmark,
    send_worker_telemetry,
)


def parse_worker_arguments() -> argparse.Namespace:
    """Parse CLI arguments for worker startup with optional master IP address and flags."""
    parser = argparse.ArgumentParser(
        description="Start a worker node for distributed video processing."
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to the config file (default: auto-discovered from config/ directory)",
    )
    parser.add_argument(
        "--password",
        default=None,
        help="Static password for connection authentication",
    )
    parser.add_argument(
        "--download-directory",
        default=WORKER_INPUT_DIRECTORY,
        help=f"Directory where downloaded part files are saved (default: {WORKER_INPUT_DIRECTORY})",
    )
    parser.add_argument(
        "--simulate-failure-after",
        type=float,
        default=None,
        help="Simulate node failure after specified seconds of execution (for automated testing)",
    )
    parser.add_argument(
        "--bind-ip",
        default=None,
        help="Optional local IP to bind outgoing connection from (useful for multi-node loopback testing)",
    )
    parser.add_argument(
        "--reconnect-timeout",
        type=float,
        default=10.0,
        help="Maximum time in seconds to retry connecting to master before exiting safely (default: 10.0)",
    )
    parser.add_argument(
        "master_ip_address",
        nargs="?",
        default=None,
        help="Optional IP address of master node. If omitted, auto-discovers Master via mDNS.",
    )
    return parser.parse_args()



def connect_to_master(
    master_ip_address: str, port: int, bind_ip: str | None = None
) -> socket.socket:
    """Connect to the master node on the fixed port."""
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if bind_ip:
        connection.bind((bind_ip, 0))
    connection.connect((master_ip_address, port))
    return connection


def ensure_executable(path: Path) -> None:
    import os, sys
    if not path.is_file():
        return
    try:
        if not os.access(path, os.X_OK):
            path.chmod(0o755)
            if not os.access(path, os.X_OK):
                raise PermissionError(f"Failed to set executable permissions on {path}")
    except Exception as e:
        print(f"\n[ERROR] Failed to make binary '{path.name}' executable: {e}", file=sys.stderr)
        sys.exit(1)


def do_binary_handshake(
    connection: socket.socket,
    master_ip_address: str,
    config: "Config",
) -> None:
    """Phase 0 worker side: respond to master's binary check and fetch binary if needed.

    Steps:
      1. Receive binary_info_request from master (contains binary_name).
      2. Detect own platform (os_folder, arch_folder).
      3. Check local binary cache: worker/binaries/<os>/<arch>/<binary_name>.
      4. Compute MD5 if the file exists.
      5. Send binary_info to master (platform, has_binary, md5).
      6. If master decides to send the binary: receive it via HTTP daemon
         (master re-uses the existing ready/file_received protocol) and save
         it to the local cache.
      7. Wait for binary_ready from master before returning.

    If require_binary is False or binary_name is empty, this function is a no-op.
    """
    if not config.binary_name:
        return

    # Step 1 — receive the request
    req = receive_binary_info_request(connection)
    binary_name = req["binary_name"]

    # Step 2 — detect platform
    os_folder, arch_folder = get_worker_platform()

    # Step 3 & 4 — check local cache
    cache_path = resolve_worker_binary_cache_path(os_folder, arch_folder, binary_name)
    ensure_executable(cache_path)
    has_binary = cache_path.is_file()
    local_md5 = compute_md5(str(cache_path)) if has_binary else None

    if not has_binary and not config.require_binary:
        import shutil
        if shutil.which(binary_name):
            has_binary = True
            local_md5 = "system"

    print(
        f"[BINARY] Platform: {os_folder}/{arch_folder}. "
        f"Binary '{binary_name}': {'present (MD5=' + local_md5 + ')' if has_binary else 'not found'}."
    )

    # Step 5 — tell master
    send_binary_info(connection, os_folder, arch_folder, binary_name, has_binary, local_md5)

    # Step 6 — read master's next decision:
    #   - MESSAGE_TYPE_BINARY_READY  → master confirmed we're good (skip transfer)
    #   - MESSAGE_TYPE_READY         → master is sending the binary; fetch it via HTTP daemon
    from core import receive_json_message, MESSAGE_TYPE_READY, MESSAGE_TYPE_BINARY_READY
    msg = receive_json_message(connection)

    # Master may skip handshake and send ready directly if it reused state
    if msg.get("type") == MESSAGE_TYPE_READY:
        print(f"[BINARY] Master skipped handshake. Proceeding directly to task.")
        return msg

    if msg.get("type") == MESSAGE_TYPE_BINARY_READY:
        # Master confirmed we already have it (or decided not to send)
        print(f"[BINARY] Master acknowledged binary '{binary_name}'. Proceeding.")
        return

    if msg.get("type") == MESSAGE_TYPE_READY:
        # Master is sending the binary — download it like a regular part file
        file_transfer_port = msg["file_transfer_port"]
        master_addr = msg.get("master_ip_address", master_ip_address)
        print(f"[BINARY] Receiving binary '{binary_name}' from master on port {file_transfer_port}...")
        allocated_files = request_file_list(master_addr, file_transfer_port)
        for fname in allocated_files:
            dest = request_file(master_addr, file_transfer_port, fname, str(cache_path.parent))
            # Rename to the expected binary name if daemon served it under original name
            dest_path = Path(dest)
            expected = cache_path.parent / binary_name
            if dest_path != expected and dest_path.is_file():
                dest_path.rename(expected)
        send_file_received_message(connection)
        
        # Make the downloaded binary executable
        if cache_path.is_file():
            ensure_executable(cache_path)

        print(f"[BINARY] Binary '{binary_name}' saved to {cache_path}. ✓")

        # Step 7 — wait for binary_ready
        receive_binary_ready(connection)
        print("[BINARY] Binary ready confirmed.")
        return

    raise ValueError(f"[BINARY] Unexpected message during binary handshake: {msg!r}")




def fetch_allocated_part_file(
    master_ip_address: str,
    file_transfer_port: int,
    download_directory: str,
) -> str:
    """On receiving ready, list and fetch this worker's allocated part file."""
    allocated_files = request_file_list(master_ip_address, file_transfer_port)
    if not allocated_files:
        raise RuntimeError("No files listed for this worker")

    downloaded_paths = [
        request_file(
            master_ip_address,
            file_transfer_port,
            filename,
            download_directory,
        )
        for filename in allocated_files
    ]
    return downloaded_paths[0]


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
    try:
        if early_ready_msg:
            ready_message = early_ready_msg
        else:
            ready_message = receive_ready_message(connection, timeout=600.0)
            
        file_transfer_port = ready_message["file_transfer_port"]
        execute_cmd = ready_message.get("execute_command", execute_command_template)

        print(f"\n[STATUS] Receiving file from master...")
        downloaded_path = fetch_allocated_part_file(
            master_ip_address,
            file_transfer_port,
            download_directory,
        )
        print(f"[STATUS] Received part file: {downloaded_path}")

        send_file_received_message(connection)

        # Phase 3 & 4: Execute task on the downloaded part file.
        # Force-stops process if connection is lost or user presses 'k'.
        print(f"\n[STATUS] Task is running...")
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
            
        print(f"[STATUS] Task complete.")

        part_filename = Path(downloaded_path).name
        output_filepath = Path(WORKER_OUTPUT_DIRECTORY) / part_filename

        # Send finished signal to Master over control port BEFORE upload starts
        # This allows the master dashboard to show "Receiving Files" during the upload
        send_finished_message(connection, elapsed_time, part_filename)

        # Phase 4 Task 4.1: Upload completed output file back to Master's per-node file-transfer daemon FIRST
        if output_filepath.is_file():
            print(f"\n[STATUS] Sending result file to master...")
            upload_result_file(master_ip_address, file_transfer_port, str(output_filepath))
            print(f"[STATUS] Sent result {part_filename} to master.")

        print(f"{PROGRESS_FINISHED} for worker ({part_filename})")
    finally:
        if downloaded_path:
            try:
                p = Path(downloaded_path)
                if p.is_file():
                    p.unlink()
                    printlog(f"Cleaned up worker downloaded part file: {downloaded_path}")
            except Exception:
                pass
        if output_filepath:
            try:
                p = Path(output_filepath)
                if p.is_file():
                    p.unlink()
                    printlog(f"Cleaned up worker output part file: {output_filepath}")
            except Exception:
                pass




def run_worker() -> None:
    """Connect to master and persist in an idle loop, executing tasks as assigned."""
    arguments = parse_worker_arguments()
    master_ip_address = arguments.master_ip_address

    config_path = arguments.config
    if config_path is None:
        available = get_available_configs()
        if not available:
            print("Error: No .ini configuration files found.", file=sys.stderr)
            sys.exit(1)
        config_path = available[0]

    try:
        config = load_config(config_path)
    except ValueError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded config from {config_path}")

    shared_secret = get_shared_secret()

    # worker/input holds downloaded parts; worker/output holds processed results.
    Path(arguments.download_directory).mkdir(parents=True, exist_ok=True)
    Path(WORKER_OUTPUT_DIRECTORY).mkdir(parents=True, exist_ok=True)

    # Start worker mDNS announcer in background
    mdns_announcer = MdnsAnnouncer(
        fqdn=MDNS_WORKER_FQDN, service=MDNS_SERVICE_WORKER
    )
    mdns_announcer.start()

    if not master_ip_address:
        print("No master IP address specified. Auto-discovering Master...")
        try:
            # Try mDNS first (works on home routers with mDNS reflectors)
            master_ip_address = discover_master_ip(timeout_seconds=10.0)
            print(f"Found master via mDNS: {master_ip_address}")
        except TimeoutError:
            print("mDNS timed out. Trying UDP broadcast (works on mobile hotspots)...")
            try:
                master_ip_address, _ = discover_master_udp(timeout_seconds=30.0)
                print(f"Found master via UDP broadcast: {master_ip_address}")
            except TimeoutError:
                print("Error: Could not discover master on the network.", file=sys.stderr)
                sys.exit(1)

    print(f"Connecting to Master at {master_ip_address}:{FIXED_PORT}...")
    print("Press 'k' to kill the task.")

    connection = None
    simulate_fail = arguments.simulate_failure_after
    reconnect_start_time: float | None = None
    max_reconnect_timeout = arguments.reconnect_timeout

    active_conn_ref: list[socket.socket | None] = [None]

    def _worker_sig_handler(signum, frame):
        print(f"\nReceived signal {signum}. Cleaning up worker temporary files and exiting...", file=sys.stderr)
        if active_conn_ref[0]:
            try:
                active_conn_ref[0].close()
            except Exception:
                pass
        cleanup_worker_temporary_files(arguments.download_directory, WORKER_OUTPUT_DIRECTORY)
        sys.exit(128 + signum)

    try:
        signal.signal(signal.SIGTERM, _worker_sig_handler)
        signal.signal(signal.SIGINT, _worker_sig_handler)
    except Exception:
        pass

    cached_pin = arguments.password

    try:
        while True:
            if connection is None:
                if reconnect_start_time is None:
                    reconnect_start_time = time.time()
                elif time.time() - reconnect_start_time >= max_reconnect_timeout:
                    print(
                        f"\nMaster at {master_ip_address}:{FIXED_PORT} unreachable for {max_reconnect_timeout:.0f}s. Exiting safely.",
                        file=sys.stderr,
                    )
                    sys.exit(0)

                print(f"\nConnecting to master at {master_ip_address}:{FIXED_PORT}...")
                try:
                    connection = connect_to_master(
                        master_ip_address, FIXED_PORT, bind_ip=arguments.bind_ip
                    )
                    active_conn_ref[0] = connection
                    # VULN-08: Send shared secret for authentication if configured
                    if shared_secret:
                        send_auth_message(connection, shared_secret)
                    else:
                        challenge = receive_pin_challenge(connection)
                        master_name = challenge.get("master_name", "unknown")
                        if cached_pin:
                            pin = cached_pin
                        else:
                            pin = input(f"Master '{master_name}' requests pairing. Enter Password/PIN shown on master's screen: ")
                            cached_pin = pin.strip()
                        send_pin_response(connection, pin.strip())
                        
                        resp = receive_json_message(connection)
                        if resp.get("type") == MESSAGE_TYPE_PIN_REJECTED:
                            print(f"Pairing rejected: {resp.get('reason')}. Retrying...", file=sys.stderr)
                            cached_pin = arguments.password  # Reset to arg if present, else None
                            connection.close()
                            connection = None
                            continue
                        elif resp.get("type") != MESSAGE_TYPE_PIN_ACCEPTED:
                            print(f"Unexpected response during PIN pairing: {resp}", file=sys.stderr)
                            sys.exit(1)
                            
                    # Run hardware benchmark (~2 seconds)
                    print("Running hardware benchmarks...")
                    telemetry = run_node_benchmark()
                    send_worker_telemetry(connection, telemetry)

                    # Phase 0: Perform binary handshake
                    early_ready = do_binary_handshake(connection, master_ip_address, config)
                    
                    print("Connected to master. Node state: IDLE. Waiting for task assignment...")
                    reconnect_start_time = None
                except OSError as exc:
                    print(f"Connection attempt failed ({exc}). Retrying in 2 seconds...", file=sys.stderr)
                    time.sleep(2.0)
                    continue

            try:
                # Wait for ready message or disconnection check
                current_sim_fail = simulate_fail
                simulate_fail = None  # One-shot simulation flag: consume immediately
                run_worker_task(
                    connection,
                    master_ip_address,
                    arguments.download_directory,
                    config.execute_command,
                    simulate_failure_after=current_sim_fail,
                    config=config,
                    early_ready_msg=early_ready,
                )
                print("Task completed. Node returning to IDLE state. Waiting for next task...")

            except MasterShutdownError:
                print("\nMaster completed processing and initiated shutdown. Exiting safely.")
                if connection:
                    try:
                        connection.close()
                    except Exception:
                        pass
                sys.exit(0)
            except KeyboardInterrupt as exc:
                print(f"\nTask aborted by user ('k' key / interrupt): {exc}")
                if connection:
                    try:
                        connection.close()
                    except Exception:
                        pass
                connection = None
                active_conn_ref[0] = None
                simulate_fail = None
                print("Node state: IDLE. Reconnecting to master...")
                time.sleep(0.5)
            except TimeoutError:
                print("\nNo tasks received for 10 minutes. Exiting to save resources.")
                if connection:
                    try:
                        connection.close()
                    except Exception:
                        pass
                sys.exit(0)
            except (ConnectionError, OSError) as exc:
                print(f"\nControl socket disconnected after pairing ({exc}). Reconnecting to master...", file=sys.stderr)
                if connection:
                    try:
                        connection.close()
                    except Exception:
                        pass
                connection = None
                active_conn_ref[0] = None
                time.sleep(1.0)
            except (RuntimeError, ValueError) as exc:
                print(f"\nTask error ({exc}). Reconnecting to master...", file=sys.stderr)
                if connection:
                    try:
                        connection.close()
                    except Exception:
                        pass
                connection = None
                active_conn_ref[0] = None
                time.sleep(1.0)
    finally:
        cleanup_worker_temporary_files(arguments.download_directory, WORKER_OUTPUT_DIRECTORY)



if __name__ == "__main__":
    run_worker()

