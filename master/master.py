"""Master node main entry point and workflow orchestrator.

Coordinates config loading, worker discovery & pairing, binary distribution,
parallel task splitting, execution monitoring with failover, and output merging.
"""

from __future__ import annotations

import os
import select
import signal
import sys
import time
from pathlib import Path

# Ensure project root and master/ directory are in sys.path
PROJECT_ROOT = (
    Path(__file__).resolve().parent.parent
    if Path(__file__).resolve().parent.name == "master"
    else Path(__file__).resolve().parent
)
MASTER_DIR = (
    Path(__file__).resolve().parent
    if Path(__file__).resolve().parent.name == "master"
    else Path(__file__).resolve().parent / "master"
)
for p in [str(PROJECT_ROOT), str(MASTER_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)

from core.benchmark import rank_workers_by_affinity
from core.config import load_config
from core.constants import (
    CONTROL_PORT,
    FIXED_PORT,
    MASTER_OUTPUT_DIRECTORY,
    MDNS_MASTER_FQDN,
    MDNS_SERVICE_MASTER,
)
from core.dashboard import StatusDashboard, printlog
from core.devices import write_devices_json
from core.discovery import MdnsAnnouncer
from core.file_transfer import FileTransferDaemon
from core.task_prep import (
    cleanup_master_temporary_files,
    part_filename_for_node,
    run_merge_command,
    run_split_command,
    verify_part_files,
)
from master.bench_and_sort import (
    rank_and_partition_workers,
    split_active_and_spare_addresses,
)
from master.binary_distributor import check_and_distribute_binaries
from master.cli import parse_master_arguments, validate_worker_address_count
from master.connection_pool import (
    WorkerConnectionPool,
    open_listening_socket,
    safe_select_sockets,
)
from master.failsafe import (
    handle_no_spare_nodes_recovery,
    reassign_task_to_spare_node,
)
from master.monitor import monitor_worker_executions_and_collect_results
from master.task_distributor import (
    distribute_part_file_to_single_node,
    distribute_part_files_to_active_nodes,
)

# Clean old log file
with open("logs.txt", "w") as f:
    f.write("")


def run_master() -> None:
    """Master node entry point: load config, validate addresses, split video, distribute parts, track execution, collect results, and merge."""
    from core.config import get_available_configs

    arguments = parse_master_arguments()

    available_configs = get_available_configs()
    if not available_configs:
        print("Error: No .ini configuration files found.", file=sys.stderr)
        sys.exit(1)

    if arguments.config:
        try:
            initial_config = str(
                Path(arguments.config).resolve().relative_to(Path(".").resolve())
            )
        except ValueError:
            initial_config = arguments.config
    else:
        initial_config = available_configs[0]

    if initial_config not in available_configs:
        available_configs.insert(0, initial_config)

    Path(arguments.parts_directory).mkdir(parents=True, exist_ok=True)
    Path(MASTER_OUTPUT_DIRECTORY).mkdir(parents=True, exist_ok=True)

    dashboard = StatusDashboard()
    dashboard.auto_confirm = getattr(arguments, "yes", False)
    dashboard.set_available_configs(available_configs, initial_config)

    if arguments.webserver:
        dashboard.webserver_mode = True
        from master.web_dashboard import start_web_server

        start_web_server(dashboard)
        print("Web dashboard started on http://0.0.0.0:8080")
        print("Terminal UI is suppressed. Open the browser to interact.")

    mdns_announcer = MdnsAnnouncer(
        fqdn=MDNS_MASTER_FQDN, service=MDNS_SERVICE_MASTER
    )
    mdns_announcer.start()

    print(f"Listening on control port {CONTROL_PORT}...")
    daemons: list[FileTransferDaemon] = []
    listening_socket = open_listening_socket(FIXED_PORT)
    worker_pool = WorkerConnectionPool(
        arguments.worker_ip_addresses,
        listening_socket,
        dashboard=dashboard,
        password=arguments.password,
    )

    def _master_sig_handler(signum, frame):
        print(
            f"\nReceived signal {signum}. Cleaning up master temporary files and exiting...",
            file=sys.stderr,
        )
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
        )
        sys.exit(128 + signum)

    try:
        signal.signal(signal.SIGTERM, _master_sig_handler)
        signal.signal(signal.SIGINT, _master_sig_handler)
    except Exception:
        pass

    try:
        while True:
            dashboard.set_available_configs(available_configs, initial_config)

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
                        if fd < 0:
                            fd = None
                        else:
                            old_settings = termios.tcgetattr(fd)
                            tty.setcbreak(fd)
                    except Exception:
                        fd = None
                        old_settings = None

                try:
                    while True:
                        new_cfg = dashboard.get_selected_config()
                        dashboard.set_prompt(
                            f"Select config: {new_cfg}.\nUse 'w'/'s' to navigate. Press 'y' to confirm, 'q' to quit: "
                        )

                        if getattr(dashboard, "webserver_mode", False):
                            dashboard.pending_input_event.clear()
                            dashboard.pending_input_event.wait()
                            ans = dashboard.pending_input_answer
                            if ans == "confirm":
                                dashboard.set_prompt(None)
                                dashboard.add_message(
                                    f"Selected config: {new_cfg}",
                                    timeout_seconds=3,
                                )
                                break
                            elif ans == "up":
                                dashboard.move_config_selection(-1)
                            elif ans == "down":
                                dashboard.move_config_selection(1)
                            continue

                        if fd is not None and fd >= 0:
                            try:
                                rlist, _, _ = select.select([fd], [], [], 0.5)
                            except (ValueError, OSError):
                                rlist = []
                            if rlist:
                                char = (
                                    os.read(fd, 1)
                                    .decode(errors="replace")
                                    .lower()
                                )
                                if char == "w":
                                    dashboard.move_config_selection(-1)
                                elif char == "s":
                                    dashboard.move_config_selection(1)
                                elif char in ("y", "\n", "\r"):
                                    dashboard.set_prompt(None)
                                    dashboard.add_message(
                                        f"Selected config: {new_cfg}",
                                        timeout_seconds=3,
                                    )
                                    break
                                elif char == "q":
                                    print(
                                        "\nUser pressed 'q'. Exiting selection..."
                                    )
                                    sys.exit(0)
                        else:
                            break
                        time.sleep(0.5)
                finally:
                    dashboard.set_prompt(None)
                    if fd is not None and old_settings is not None:
                        try:
                            import termios

                            termios.tcsetattr(
                                fd, termios.TCSADRAIN, old_settings
                            )
                        except Exception:
                            pass

            selected_config_path = dashboard.get_selected_config()
            try:
                config = load_config(selected_config_path)
            except ValueError as exc:
                print(f"Config error: {exc}", file=sys.stderr)
                sys.exit(1)

            print(f"Loaded config from {selected_config_path}")
            dashboard.set_available_configs([])

            if not arguments.worker_ip_addresses:
                dashboard.set_master_state("device discovery")
                start_time = time.time()
                timeout_seconds = 300.0
                deadline = start_time + timeout_seconds

                fd = None
                old_settings = None
                if sys.stdin.isatty():
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
                    while True:
                        now = time.time()
                        remaining = int(max(0.0, deadline - now))
                        with worker_pool.lock:
                            connected_ips = list(worker_pool.connections.keys())

                        remaining_str = (
                            f" ({remaining}s remaining)" if remaining < 30 else ""
                        )
                        dashboard.set_prompt(
                            f"Discovered {len(connected_ips)} worker(s). Config: '{selected_config_path}'.\nPress 'y' to continue, 'q' to quit{remaining_str}: "
                        )

                        if (
                            getattr(arguments, "yes", False)
                            and len(connected_ips) >= config.min_nodes
                        ):
                            break

                        if getattr(dashboard, "webserver_mode", False):
                            dashboard.pending_input_event.clear()
                            dashboard.pending_input_event.wait(0.5)
                            ans = dashboard.pending_input_answer
                            if ans == "confirm":
                                if len(connected_ips) < config.min_nodes:
                                    dashboard.add_message(
                                        f"Need at least {config.min_nodes} workers (discovered {len(connected_ips)} so far). Waiting...",
                                        timeout_seconds=5,
                                    )
                                    dashboard.pending_input_answer = None
                                else:
                                    break
                            elif ans == "quit":
                                sys.exit(0)
                        elif fd is not None and fd >= 0:
                            try:
                                rlist, _, _ = select.select([fd], [], [], 0.5)
                            except (ValueError, OSError):
                                rlist = []
                            if rlist:
                                char = (
                                    os.read(fd, 1)
                                    .decode(errors="replace")
                                    .lower()
                                )
                                if char in ("y", "\n", "\r"):
                                    if len(connected_ips) < config.min_nodes:
                                        dashboard.add_message(
                                            f"Need at least {config.min_nodes} workers (discovered {len(connected_ips)} so far). Waiting...",
                                            timeout_seconds=5,
                                        )
                                    else:
                                        break
                                elif char == "q":
                                    print(
                                        "\nUser pressed 'q'. Exiting selection..."
                                    )
                                    sys.exit(0)
                        else:
                            if len(connected_ips) >= config.min_nodes:
                                break

                        if now >= deadline:
                            if len(connected_ips) < config.min_nodes:
                                print(
                                    f"\n[DISCOVERY TIMEOUT] Fewer than {config.min_nodes} workers discovered within {int(timeout_seconds)}s. Exiting safely.",
                                    file=sys.stderr,
                                )
                                sys.exit(0)
                            else:
                                print(
                                    f"\n[SELECTION TIMEOUT] Timeout reached ({int(timeout_seconds)}s). Proceeding with {len(connected_ips)} workers."
                                )
                                break
                        time.sleep(0.5)
                finally:
                    dashboard.set_prompt(None)
                    if fd is not None and old_settings is not None:
                        try:
                            import termios

                            termios.tcsetattr(
                                fd, termios.TCSADRAIN, old_settings
                            )
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

            worker_addresses_to_use = rank_workers_by_affinity(
                telemetry,
                config.resource_type,
                config.min_vram_mb,
                worker_addresses=worker_addresses_to_use,
            )

            try:
                validate_worker_address_count(
                    worker_addresses_to_use, config.min_nodes
                )
            except ValueError as exc:
                print(f"Startup error: {exc}", file=sys.stderr)
                sys.exit(1)

            active_nodes, spare_nodes = split_active_and_spare_addresses(
                worker_addresses_to_use, config.max_nodes
            )
            device_mapping = write_devices_json(active_nodes)
            print(f"Active nodes ({len(active_nodes)}): {', '.join(active_nodes)}")
            if spare_nodes:
                print(
                    f"Spare nodes ({len(spare_nodes)}): {', '.join(spare_nodes)}"
                )
            else:
                print("Spare nodes: None")

            if not spare_nodes:
                warning_msg = (
                    f"WARNING: Total {len(worker_addresses_to_use)} node(s) connected, task requires at least "
                    f"{config.min_nodes}. All {len(active_nodes)} node(s) allocated as active, but NO spare nodes are left for failover!"
                )
                printlog(f"\n[SPARE NODES WARNING] {warning_msg}")
                if dashboard:
                    dashboard.add_message(warning_msg, timeout_seconds=60)

                prompt_text = (
                    f"\n======================================================================\n"
                    f"  WARNING: NO SPARE NODES AVAILABLE FOR FAILOVER RECOVERY\n"
                    f"======================================================================\n"
                    f"Total connected nodes:   {len(worker_addresses_to_use)}\n"
                    f"Task minimum required:   {config.min_nodes}\n"
                    f"Active nodes allocated:  {len(active_nodes)}\n"
                    f"Spare nodes available:   0 (None)\n\n"
                    f"If an active worker node fails during execution, automatic failover\n"
                    f"will NOT be possible without waiting or user intervention.\n"
                    f"Do you want to continue without spare nodes? [y/N]: "
                )
                if dashboard:
                    dashboard.set_prompt(prompt_text)
                else:
                    sys.stdout.write(prompt_text)
                    sys.stdout.flush()

                confirmed = False
                if getattr(arguments, "yes", False):
                    confirmed = True
                elif dashboard and getattr(dashboard, "webserver_mode", False):
                    dashboard.pending_input_event.clear()
                    dashboard.pending_input_event.wait()
                    ans = dashboard.pending_input_answer
                    dashboard.pending_input_answer = None
                    confirmed = ans == "confirm"
                elif sys.stdin.isatty():
                    try:
                        import termios
                        import tty as _tty

                        fd = sys.stdin.fileno()
                        old = termios.tcgetattr(fd)
                        _tty.setcbreak(fd)
                        try:
                            char = sys.stdin.read(1).lower()
                            print(char)
                            confirmed = char == "y"
                        finally:
                            termios.tcsetattr(fd, termios.TCSADRAIN, old)
                    except Exception:
                        line = sys.stdin.readline().strip().lower()
                        confirmed = line.startswith("y")
                else:
                    try:
                        line = sys.stdin.readline().strip().lower()
                        confirmed = line.startswith("y")
                    except Exception:
                        confirmed = False

                if dashboard:
                    dashboard.set_prompt(None)

                if not confirmed:
                    print(
                        "\nUser declined to continue without spare nodes. Exiting safely..."
                    )
                    printlog(
                        "[SPARE NODES] User declined execution with 0 spare nodes. Exiting."
                    )
                    if dashboard:
                        dashboard.add_message(
                            "Execution aborted: no spare nodes.", timeout_seconds=10
                        )
                    worker_pool.shutdown_all_workers()
                    sys.exit(0)

                print("User confirmed: proceeding without spare nodes.")
                printlog(
                    "[SPARE NODES] User confirmed proceeding without spare nodes."
                )

            dashboard.total_parts = len(active_nodes)
            dashboard.reset_nodes()
            for idx, ip in enumerate(active_nodes, start=1):
                is_conn = ip in worker_pool.connections
                tel = worker_pool.worker_telemetry.get(ip, {})
                dashboard.update_node(
                    ip,
                    f"node{idx}",
                    "-",
                    "idle" if is_conn else "disconnected",
                    connected=is_conn,
                    hostname=tel.get("hostname"),
                    username=tel.get("username"),
                )
            for ip in spare_nodes:
                is_conn = ip in worker_pool.connections
                tel = worker_pool.worker_telemetry.get(ip, {})
                dashboard.update_node(
                    ip,
                    "spare",
                    "-",
                    "idle" if is_conn else "disconnected",
                    connected=is_conn,
                    hostname=tel.get("hostname"),
                    username=tel.get("username"),
                )

            print(
                f"Waiting for active worker connections ({', '.join(active_nodes)})..."
            )
            for ip in active_nodes:
                worker_pool.get_connection(ip)
            print("All active workers connected!")

            # Phase 0: Binary check
            binary_ready_nodes, binary_idle_nodes = check_and_distribute_binaries(
                active_nodes,
                worker_pool,
                config,
                master_ip_address=arguments.master_ip,
                dashboard=dashboard,
            )
            active_nodes = binary_ready_nodes
            if not active_nodes:
                print(
                    "[BINARY] No workers have the required binary. Cannot proceed.",
                    file=sys.stderr,
                )
                sys.exit(1)

            dashboard.set_master_state("splitting file")
            cleanup_master_temporary_files(
                parts_directory=arguments.parts_directory,
                output_directory=MASTER_OUTPUT_DIRECTORY,
            )
            run_split_command(
                config.split_command,
                len(active_nodes),
                parts_directory=arguments.parts_directory,
            )
            verify_part_files(
                len(active_nodes),
                parts_directory=arguments.parts_directory,
                split_command=config.split_command,
            )

            for idx, ip in enumerate(active_nodes, start=1):
                dashboard.update_node(
                    ip,
                    f"node{idx}",
                    part_filename_for_node(
                        idx,
                        split_command=config.split_command,
                        parts_directory=arguments.parts_directory,
                    ),
                    "sending file",
                    receiving=False,
                    executing=False,
                    sending=True,
                )

            initial_daemons = distribute_part_files_to_active_nodes(
                active_nodes,
                worker_pool,
                arguments.parts_directory,
                master_ip_address=arguments.master_ip,
                dashboard=dashboard,
                config=config,
            )
            daemons.extend(initial_daemons)
            printlog(
                "\nPhase 2 complete: split files distributed to all active workers."
            )

            dashboard.set_master_state("executing")
            failover_daemons = monitor_worker_executions_and_collect_results(
                active_nodes,
                worker_pool,
                spare_nodes,
                arguments.parts_directory,
                master_ip_address=arguments.master_ip,
                dashboard=dashboard,
                config=config,
                retry_timeout=getattr(arguments, "retry_timeout", 15.0),
            )
            daemons.extend(failover_daemons)
            printlog("\nAll part files received.")

            dashboard.set_master_state("merging files")
            run_merge_command(
                config.merge_command,
                len(active_nodes),
                output_directory=MASTER_OUTPUT_DIRECTORY,
                split_command=config.split_command,
            )
            dashboard.set_master_state("finished")
            printlog("\nAll tasks and merging completed successfully.")

            printlog("\nAll tasks completed successfully!")
            try:
                dashboard.set_prompt("Do you want to process another job? [y/N]:")
                if getattr(dashboard, "webserver_mode", False):
                    dashboard.pending_input_event.clear()
                    dashboard.pending_input_event.wait()
                    ans = (
                        "y"
                        if dashboard.pending_input_answer == "confirm"
                        else "n"
                    )
                elif sys.stdin.isatty():
                    try:
                        import termios
                        import tty as _tty

                        fd = sys.stdin.fileno()
                        old = termios.tcgetattr(fd)
                        _tty.setcbreak(fd)
                        ans = os.read(fd, 1).decode(errors="replace").lower()
                        termios.tcsetattr(fd, termios.TCSADRAIN, old)
                    except Exception:
                        ans = (
                            input("Do you want to process another job? [y/N]: ")
                            .strip()
                            .lower()
                        )
                else:
                    ans = (
                        input("Do you want to process another job? [y/N]: ")
                        .strip()
                        .lower()
                    )
            except EOFError:
                ans = "n"
            finally:
                dashboard.set_prompt(None)

            if ans == "y":
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
        )


if __name__ == "__main__":
    run_master()
