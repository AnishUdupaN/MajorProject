"""Worker node main entry point and lifecycle orchestrator.

Handles connection to master, pairing authentication, hardware benchmarking,
binary caching, receiving split tasks, executing bare-metal workloads with CPU throttling,
and uploading finished artifacts.
"""

from __future__ import annotations

import os
import signal
import socket
import sys
import time
import uuid
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = (
    Path(__file__).resolve().parent.parent
    if Path(__file__).resolve().parent.name == "worker"
    else Path(__file__).resolve().parent
)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.benchmark import run_node_benchmark
from core.config import get_available_configs, load_config
from core.constants import (
    FIXED_PORT,
    MDNS_SERVICE_WORKER,
    MDNS_WORKER_FQDN,
    WORKER_OUTPUT_DIRECTORY,
)
from core.discovery import MdnsAnnouncer
from core.messaging import (
    MasterShutdownError,
    get_shared_secret,
    send_worker_telemetry,
)
from core.task_prep import cleanup_worker_temporary_files
from worker.binary_manager import (
    do_binary_handshake,
    ensure_executable,
    handle_binary_handshake_request,
)
from worker.cli import parse_worker_arguments
from worker.connection import (
    connect_to_master,
    discover_master_address,
    perform_handshake_authentication,
)
from worker.task_executor import run_worker_task
from worker.task_fetcher import fetch_allocated_part_file

# Global dynamic CPU throttling parameters
nice_initial = 5
nice_step = 5
nice_max = 19
nice_check_interval_seconds = 5.0
max_throttle_duration_seconds = 600.0
unthrottle_threshold_seconds = 20.0


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
        config.nice_initial = nice_initial
        config.nice_step = nice_step
        config.nice_max = nice_max
        config.nice_check_interval_seconds = nice_check_interval_seconds
        config.max_throttle_duration_seconds = max_throttle_duration_seconds
        config.unthrottle_threshold_seconds = unthrottle_threshold_seconds
    except ValueError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded config from {config_path}")

    worker_id = arguments.worker_id
    if not worker_id:
        worker_id = (
            f"{arguments.username or 'worker'}_{os.getpid()}_{uuid.uuid4().hex[:6]}"
        )

    shared_secret = get_shared_secret()

    Path(arguments.download_directory).mkdir(parents=True, exist_ok=True)
    Path(WORKER_OUTPUT_DIRECTORY).mkdir(parents=True, exist_ok=True)

    mdns_announcer = MdnsAnnouncer(
        fqdn=MDNS_WORKER_FQDN, service=MDNS_SERVICE_WORKER
    )
    mdns_announcer.start()

    if not master_ip_address:
        master_ip_address = discover_master_address()

    print(f"Connecting to Master at {master_ip_address}:{FIXED_PORT}...")
    print("Press 'k' to kill the task.")

    connection = None
    simulate_fail = arguments.simulate_failure_after
    reconnect_start_time: float | None = None
    max_reconnect_timeout = arguments.reconnect_timeout

    active_conn_ref: list[socket.socket | None] = [None]

    def _worker_sig_handler(signum, frame):
        print(
            f"\nReceived signal {signum}. Cleaning up worker temporary files and exiting...",
            file=sys.stderr,
        )
        if active_conn_ref[0]:
            try:
                active_conn_ref[0].close()
            except Exception:
                pass
        cleanup_worker_temporary_files(
            arguments.download_directory, WORKER_OUTPUT_DIRECTORY
        )
        sys.exit(128 + signum)

    try:
        signal.signal(signal.SIGTERM, _worker_sig_handler)
        signal.signal(signal.SIGINT, _worker_sig_handler)
    except Exception:
        pass

    cached_pin = arguments.password
    early_ready: dict | None = None

    try:
        while True:
            if connection is None:
                early_ready = None
                if reconnect_start_time is None:
                    reconnect_start_time = time.time()
                elif (
                    time.time() - reconnect_start_time >= max_reconnect_timeout
                ):
                    print(
                        f"\nMaster at {master_ip_address}:{FIXED_PORT} unreachable for {max_reconnect_timeout:.0f}s. Exiting safely.",
                        file=sys.stderr,
                    )
                    sys.exit(0)

                print(
                    f"\nConnecting to master at {master_ip_address}:{FIXED_PORT}..."
                )
                try:
                    connection = connect_to_master(
                        master_ip_address, FIXED_PORT, bind_ip=arguments.bind_ip
                    )
                    active_conn_ref[0] = connection

                    cached_pin = perform_handshake_authentication(
                        connection=connection,
                        shared_secret=shared_secret,
                        password=arguments.password,
                        cached_pin=cached_pin,
                    )

                    print("Running hardware benchmarks...")
                    telemetry = run_node_benchmark(
                        custom_username=arguments.username
                    )
                    telemetry["worker_id"] = worker_id
                    send_worker_telemetry(connection, telemetry)

                    early_ready = do_binary_handshake(
                        connection, master_ip_address, config
                    )

                    print(
                        "Connected to master. Node state: IDLE. Waiting for task assignment..."
                    )
                    reconnect_start_time = None
                except MasterShutdownError:
                    print(
                        "\nMaster completed processing and initiated shutdown. Exiting safely."
                    )
                    if connection:
                        try:
                            connection.close()
                        except Exception:
                            pass
                    sys.exit(0)
                except (
                    OSError,
                    ConnectionError,
                    ValueError,
                    RuntimeError,
                ) as exc:
                    print(
                        f"Connection attempt failed ({exc}). Retrying in 2 seconds...",
                        file=sys.stderr,
                    )
                    if connection:
                        try:
                            connection.close()
                        except Exception:
                            pass
                    connection = None
                    active_conn_ref[0] = None
                    early_ready = None
                    time.sleep(2.0)
                    continue

            try:
                current_sim_fail = simulate_fail
                simulate_fail = None
                run_worker_task(
                    connection,
                    master_ip_address,
                    arguments.download_directory,
                    config.execute_command,
                    simulate_failure_after=current_sim_fail,
                    config=config,
                    early_ready_msg=early_ready,
                )
                early_ready = None
                print(
                    "Task completed. Node returning to IDLE state. Waiting for next task..."
                )

            except MasterShutdownError:
                print(
                    "\nMaster completed processing and initiated shutdown. Exiting safely."
                )
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
                print(
                    "\nNo tasks received for 10 minutes. Exiting to save resources."
                )
                if connection:
                    try:
                        connection.close()
                    except Exception:
                        pass
                sys.exit(0)
            except (ConnectionError, OSError) as exc:
                print(
                    f"\nControl socket disconnected after pairing ({exc}). Reconnecting to master...",
                    file=sys.stderr,
                )
                if connection:
                    try:
                        connection.close()
                    except Exception:
                        pass
                connection = None
                active_conn_ref[0] = None
                time.sleep(1.0)
            except (RuntimeError, ValueError) as exc:
                print(
                    f"\nTask error ({exc}). Reconnecting to master...",
                    file=sys.stderr,
                )
                if connection:
                    try:
                        connection.close()
                    except Exception:
                        pass
                connection = None
                active_conn_ref[0] = None
                time.sleep(1.0)
            except Exception as exc:
                print(
                    f"\nUnexpected task error ({type(exc).__name__}: {exc}). Reconnecting to master...",
                    file=sys.stderr,
                )
                if connection:
                    try:
                        connection.close()
                    except Exception:
                        pass
                connection = None
                active_conn_ref[0] = None
                time.sleep(1.0)
    finally:
        cleanup_worker_temporary_files(
            arguments.download_directory, WORKER_OUTPUT_DIRECTORY
        )


if __name__ == "__main__":
    run_worker()
