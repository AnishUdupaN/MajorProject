"""Worker execution module: build execute_command, track live percentage & ETA, listen for 'k' key kill and connection loss."""

import re
import select
import shlex
import socket
import subprocess
import sys
import threading
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


def parse_ffmpeg_time_seconds(time_str: str) -> float:
    """Parse HH:MM:SS.ss timestamp string into total seconds."""
    parts = time_str.split(":")
    if len(parts) == 3:
        return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
    elif len(parts) == 2:
        return float(parts[0]) * 60 + float(parts[1])
    return float(parts[0])


def get_video_duration_ffprobe(filepath: str) -> float | None:
    """Try reading video duration in seconds using ffprobe."""
    try:
        res = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                filepath,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode == 0 and res.stdout.strip():
            return float(res.stdout.strip())
    except Exception:
        pass
    return None


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
    from control_messages import _SOCKET_BUFFERS

    if _SOCKET_BUFFERS.get(connection, ""):
        return False
    try:
        rlist, _, _ = select.select([connection], [], [], 0.0)
        if rlist:
            data = connection.recv(1024, socket.MSG_PEEK)
            if not data:
                return True
    except (OSError, ConnectionError):
        return True
    return False



def _stderr_reader_thread(process: subprocess.Popen, state: dict, input_file: str) -> None:
    """Background thread to suppress raw ffmpeg stderr noise and parse progress/ETA."""
    total_dur = get_video_duration_ffprobe(input_file)
    if total_dur:
        state["total_duration"] = total_dur

    start_wall = time.time()
    if process.stderr is None:
        return

    for line in iter(process.stderr.readline, ""):
        if not line:
            break
        state["last_lines"].append(line.strip())
        if len(state["last_lines"]) > 10:
            state["last_lines"].pop(0)

        if "Duration:" in line and state.get("total_duration") is None:
            match = re.search(r"Duration:\s*(\d+:\d+:\d+\.\d+|\d+:\d+:\d+)", line)
            if match:
                state["total_duration"] = parse_ffmpeg_time_seconds(match.group(1))

        if "time=" in line:
            match = re.search(r"time=\s*(\d+:\d+:\d+\.\d+|\d+:\d+:\d+)", line)
            if match:
                curr_sec = parse_ffmpeg_time_seconds(match.group(1))
                state["current_seconds"] = curr_sec
                dur = state.get("total_duration")
                if dur and dur > 0:
                    pct = min(100.0, max(0.0, (curr_sec / dur) * 100.0))
                    state["pct"] = pct
                    elapsed_wall = time.time() - start_wall
                    if pct > 0:
                        est_total = elapsed_wall / (pct / 100.0)
                        eta_sec = max(0.0, est_total - elapsed_wall)
                        state["eta"] = format_elapsed_time(eta_sec)

    process.stderr.close()


def run_execute_command(
    execute_command_template: str,
    input_file: str,
    output_directory: str = WORKER_OUTPUT_DIRECTORY,
    output_file: str | None = None,
    connection: socket.socket | None = None,
    simulate_failure_after: float | None = None,
    progress_callback: Callable[[float], None] | None = None,
) -> float:
    """Run execute_command on worker, show clean percentage & ETA progress, support 'k' kill and connection loss force-stop."""
    Path(output_directory).mkdir(parents=True, exist_ok=True)
    resolved_command = build_execute_command(
        execute_command_template, input_file, output_directory, output_file
    )
    command_args = shlex.split(resolved_command)

    filename = Path(input_file).name
    print(f"{PROGRESS_EXECUTING} ({filename}) - starting: {resolved_command}")
    start_time = time.time()

    # Capture stdout and stderr to suppress raw ffmpeg output
    process = subprocess.Popen(
        command_args,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )


    state = {
        "pct": None,
        "eta": None,
        "current_seconds": 0.0,
        "total_duration": None,
        "last_lines": [],
    }

    reader_thread = threading.Thread(
        target=_stderr_reader_thread,
        args=(process, state, input_file),
        daemon=True,
    )
    reader_thread.start()

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

            pct = state.get("pct")
            eta = state.get("eta")

            if pct is not None and eta is not None:
                display_str = f"\r{PROGRESS_EXECUTING} ({filename}) - {pct:.1f}% [ETA: {eta}]   "
            elif pct is not None:
                display_str = f"\r{PROGRESS_EXECUTING} ({filename}) - {pct:.1f}%   "
            else:
                display_str = f"\r{PROGRESS_EXECUTING} ({filename}) - running time: {formatted_time}   "

            sys.stdout.write(display_str)
            sys.stdout.flush()

            if progress_callback:
                progress_callback(elapsed_seconds)

            if return_code is not None:
                sys.stdout.write("\n")
                if return_code != 0:
                    err_details = "\n".join(state["last_lines"])
                    raise RuntimeError(
                        f"execute_command failed with exit code {return_code}:\n{err_details}"
                    )
                print(
                    f"{PROGRESS_EXECUTING} complete for {filename} in {formatted_time}"
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
