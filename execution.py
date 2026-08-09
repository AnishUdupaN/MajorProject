"""Worker execution module: build execute_command, track live running time, listen for 'k' key kill and connection loss."""

import select
import shlex
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Callable

from constants import PROGRESS_EXECUTING, WORKER_OUTPUT_DIRECTORY


def build_execute_command(
    execute_command_template: str,
    input_file: str,
    output_directory: str = WORKER_OUTPUT_DIRECTORY,
    output_file: str | None = None,
) -> str:
    """Build the execute_command string by substituting input/output placeholders."""
    input_path = Path(input_file)
    if output_file is None:
        output_file = input_path.name

    resolved_command = execute_command_template.replace("{input}", str(input_path))
    resolved_command = resolved_command.replace("{output_directory}", output_directory)
    resolved_command = resolved_command.replace("{output}", output_file)
    resolved_command = resolved_command.replace(
        "{input_directory}", str(input_path.parent)
    )
    return resolved_command


def format_elapsed_time(seconds: float) -> str:
    """Format elapsed seconds as MM:SS string."""
    minutes = int(seconds) // 60
    secs = int(seconds) % 60
    return f"{minutes:02d}:{secs:02d}"


def force_stop_process(process: subprocess.Popen) -> None:
    """Force stop a running execution process (SIGTERM then SIGKILL)."""
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=1.0)


def check_socket_connection_lost(connection: socket.socket) -> bool:
    """Check if the control socket connection to the master node has been lost/closed."""
    try:
        rlist, _, _ = select.select([connection], [], [], 0.0)
        if rlist:
            data = connection.recv(1024, socket.MSG_PEEK)
            if not data:
                return True
    except (OSError, ConnectionError):
        return True
    return False


def run_execute_command(
    execute_command_template: str,
    input_file: str,
    output_directory: str = WORKER_OUTPUT_DIRECTORY,
    output_file: str | None = None,
    connection: socket.socket | None = None,
    simulate_failure_after: float | None = None,
    progress_callback: Callable[[float], None] | None = None,
) -> float:
    """Run execute_command on worker, show live progress executing with elapsed time, support 'k' kill and connection loss force-stop."""
    Path(output_directory).mkdir(parents=True, exist_ok=True)
    resolved_command = build_execute_command(
        execute_command_template, input_file, output_directory, output_file
    )
    command_args = shlex.split(resolved_command)

    print(f"{PROGRESS_EXECUTING} ({Path(input_file).name}) - starting: {resolved_command}")
    start_time = time.time()
    process = subprocess.Popen(command_args)

    # Prepare non-blocking terminal input for 'k' key press if running in interactive tty.
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
            return_code = process.poll()
            elapsed_seconds = time.time() - start_time
            formatted_time = format_elapsed_time(elapsed_seconds)

            sys.stdout.write(
                f"\r{PROGRESS_EXECUTING} ({Path(input_file).name}) - running time: {formatted_time}  "
            )
            sys.stdout.flush()

            if progress_callback:
                progress_callback(elapsed_seconds)

            if return_code is not None:
                sys.stdout.write("\n")
                if return_code != 0:
                    raise RuntimeError(
                        f"execute_command failed with exit code {return_code}"
                    )
                print(
                    f"{PROGRESS_EXECUTING} complete for {Path(input_file).name} in {formatted_time}"
                )
                return elapsed_seconds

            # Check if connection to master server is lost
            if connection is not None and check_socket_connection_lost(connection):
                sys.stdout.write("\n")
                print("\nConnection to server lost! Force-stopping running process...")
                force_stop_process(process)
                raise ConnectionError("Server connection lost during task execution")

            # Check simulated failure timer (if set for automated tests)
            if (
                simulate_failure_after is not None
                and elapsed_seconds >= simulate_failure_after
            ):
                sys.stdout.write("\n")
                print("\nSimulated failure triggered! Force-stopping process...")
                force_stop_process(process)
                raise KeyboardInterrupt(
                    f"Simulated failure triggered after {simulate_failure_after}s"
                )

            # Check for interactive 'k' key press
            if fd is not None:
                rlist, _, _ = select.select([sys.stdin], [], [], 0.0)
                if rlist:
                    char = sys.stdin.read(1)
                    if char.lower() == "k":
                        sys.stdout.write("\n")
                        print("\n'k' key pressed! Force-killing task process and simulating node drop...")
                        force_stop_process(process)
                        raise KeyboardInterrupt("Task force-killed by user pressing 'k'")

            time.sleep(0.2)
    except Exception:
        force_stop_process(process)
        raise
    finally:
        if fd is not None and old_settings is not None:
            try:
                import termios

                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            except Exception:
                pass
