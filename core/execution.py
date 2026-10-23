"""Task execution, CPU tracking, and dynamic throttling on worker nodes."""

from __future__ import annotations

import os
import re
import select
import shlex
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path

from core.benchmark import (
    get_num_cpus,
    get_process_cpu_percent,
    get_system_cpu_percent,
)
from core.binaries import (
    get_binary_base_dir,
    get_worker_platform,
    resolve_worker_binary_base_dir,
)
from core.config import Config
from core.constants import (
    MESSAGE_TYPE_SHUTDOWN,
    PROGRESS_EXECUTING,
    WORKER_OUTPUT_DIRECTORY,
)
from core.messaging import (
    MasterShutdownError,
    _SOCKET_BUFFERS,
    has_buffered_message,
    receive_json_message,
)


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


def _stderr_reader_thread(
    process: subprocess.Popen, state: dict, input_file: str
) -> None:
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


class ProcessCpuTracker:
    """Track system CPU usage and subprocess CPU usage over time."""

    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.last_time = time.time()
        self.num_cpus = get_num_cpus()
        self._last_sys_ticks = self._read_sys_ticks()
        self._last_proc_ticks = self._read_proc_ticks()

    def _read_sys_ticks(self) -> tuple[float, float] | None:
        """Return (total_jiffies, idle_jiffies) from /proc/stat if available."""
        try:
            with open("/proc/stat", "r") as f:
                line = f.readline()
            if line.startswith("cpu "):
                parts = [float(x) for x in line.split()[1:]]
                total = sum(parts)
                idle = parts[3] + (parts[4] if len(parts) > 4 else 0.0)
                return total, idle
        except Exception:
            pass
        return None

    def _read_proc_ticks(self) -> float | None:
        """Return total process jiffies from /proc/<pid>/stat if available."""
        try:
            with open(f"/proc/{self.pid}/stat", "r") as f:
                content = f.read()
            rparen_idx = content.rfind(")")
            if rparen_idx != -1:
                fields = content[rparen_idx + 1 :].split()
                utime = float(fields[11])
                stime = float(fields[12])
                cutime = float(fields[13])
                cstime = float(fields[14])
                return utime + stime + cutime + cstime
        except Exception:
            pass
        return None

    def get_cpu_usage(self) -> tuple[float, float, float]:
        """Return (system_cpu_pct, process_cpu_pct, background_cpu_pct)."""
        now = time.time()
        dt = now - self.last_time
        self.last_time = now

        sys_ticks = self._read_sys_ticks()
        proc_ticks = self._read_proc_ticks()

        system_cpu_pct = 0.0
        process_cpu_pct = 0.0

        if sys_ticks is not None and self._last_sys_ticks is not None:
            tot_diff = sys_ticks[0] - self._last_sys_ticks[0]
            idle_diff = sys_ticks[1] - self._last_sys_ticks[1]
            if tot_diff > 0:
                system_cpu_pct = max(
                    0.0, min(100.0, 100.0 * (1.0 - idle_diff / tot_diff))
                )

            if proc_ticks is not None and self._last_proc_ticks is not None:
                proc_diff = proc_ticks - self._last_proc_ticks
                if tot_diff > 0:
                    process_cpu_pct = max(
                        0.0, min(100.0, 100.0 * (proc_diff / tot_diff))
                    )
            self._last_sys_ticks = sys_ticks
            self._last_proc_ticks = proc_ticks
        else:
            try:
                system_cpu_pct = get_system_cpu_percent()
                process_cpu_pct = get_process_cpu_percent(self.pid, self.num_cpus)
            except Exception:
                try:
                    import psutil

                    system_cpu_pct = psutil.cpu_percent()
                    p = psutil.Process(self.pid)
                    process_cpu_pct = p.cpu_percent() / self.num_cpus
                except Exception:
                    system_cpu_pct = 0.0
                    process_cpu_pct = 0.0

        background_cpu_pct = max(0.0, system_cpu_pct - process_cpu_pct)
        return system_cpu_pct, process_cpu_pct, background_cpu_pct


def set_process_nice(pid: int, nice_value: int) -> bool:
    """Set nice priority score for a process PID."""
    try:
        os.setpriority(os.PRIO_PROCESS, pid, nice_value)
        return True
    except Exception:
        pass

    try:
        res = subprocess.run(
            ["renice", str(nice_value), "-p", str(pid)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if res.returncode == 0:
            return True
    except Exception:
        pass

    try:
        import psutil

        psutil.Process(pid).nice(nice_value)
        return True
    except Exception:
        return False


def run_execute_command(
    execute_command_template: str,
    input_file: str,
    output_directory: str = WORKER_OUTPUT_DIRECTORY,
    output_file: str | None = None,
    connection: socket.socket | None = None,
    simulate_failure_after: float | None = None,
    progress_callback: Callable[[float], None] | None = None,
    config: Config | None = None,
) -> float:
    """Run execute_command on worker, monitor CPU usage, support stepped nice throttling and report progress."""
    Path(output_directory).mkdir(parents=True, exist_ok=True)
    resolved_command = build_execute_command(
        execute_command_template, input_file, output_directory, output_file
    )
    command_args = shlex.split(resolved_command)

    filename = Path(input_file).name
    print(f"{PROGRESS_EXECUTING} ({filename}) - starting: {resolved_command}")
    start_time = time.time()

    process_env = os.environ.copy()
    try:
        os_folder, arch_folder = get_worker_platform()
        candidate_binaries = []
        if command_args:
            first_arg = Path(command_args[0]).name
            candidate_binaries.append(first_arg)
            candidate_binaries.append(Path(first_arg).stem)
        if config and config.binary_name:
            candidate_binaries.append(config.binary_name)

        bin_dirs = []
        for b_name in candidate_binaries:
            bin_dirs.append(resolve_worker_binary_base_dir(b_name, os_folder))
            if config and getattr(config, "binaries_directory", None):
                bin_dirs.append(
                    get_binary_base_dir(config.binaries_directory, b_name, os_folder)
                )
            else:
                bin_dirs.append(get_binary_base_dir("binaries", b_name, os_folder))

        # Also search general worker/binaries and binaries directories for the platform
        for root_b in [Path("worker/binaries"), Path("binaries")]:
            if root_b.is_dir():
                for sub in root_b.iterdir():
                    if sub.is_dir():
                        os_sub = sub / os_folder
                        if os_sub.is_dir() and os_sub not in bin_dirs:
                            bin_dirs.append(os_sub)

        for base_bin_dir in bin_dirs:
            if base_bin_dir.is_dir():
                process_env["PATH"] = (
                    f"{base_bin_dir.resolve()}{os.pathsep}{process_env.get('PATH', '')}"
                )
                if command_args:
                    sub_candidate = base_bin_dir / command_args[0]
                    if sub_candidate.is_file():
                        ensure_executable(sub_candidate)
                        if sub_candidate.suffix == ".py":
                            command_args = [
                                sys.executable,
                                str(sub_candidate.resolve()),
                            ] + command_args[1:]
                        else:
                            command_args[0] = str(sub_candidate.resolve())
                        break
    except Exception:
        pass

    process = subprocess.Popen(
        command_args,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env=process_env,
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

    tracker = ProcessCpuTracker(process.pid)
    high_usage_start: float | None = None
    low_usage_start: float | None = None
    is_throttled = False
    current_nice = 0
    throttled_start_time: float | None = None
    last_nice_check_time: float | None = None
    last_bg_usage = 0.0
    last_cpu_check_time = 0.0

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
            now = time.time()
            elapsed_seconds = now - start_time
            formatted_time = format_elapsed_time(elapsed_seconds)

            # Resource monitoring check every second
            if config is not None and now - last_cpu_check_time >= 1.0:
                last_cpu_check_time = now
                sys_pct, proc_pct, bg_pct = tracker.get_cpu_usage()

                if bg_pct > config.cpu_threshold_percent:
                    low_usage_start = None
                    if high_usage_start is None:
                        high_usage_start = now
                    elif now - high_usage_start >= config.cpu_threshold_seconds:
                        if config.on_high_usage == "kill":
                            sys.stdout.write("\n")
                            print(
                                f"\n[RESOURCE ALERT] Background CPU usage ({bg_pct:.1f}%) exceeded threshold ({config.cpu_threshold_percent}%) for >{config.cpu_threshold_seconds}s! Killing task process..."
                            )
                            force_stop_process(process)
                            raise RuntimeError(
                                f"Task process killed: background CPU usage ({bg_pct:.1f}%) exceeded {config.cpu_threshold_percent}% for >{config.cpu_threshold_seconds}s"
                            )
                        elif config.on_high_usage == "throttle":
                            if not is_throttled:
                                is_throttled = True
                                current_nice = config.nice_initial
                                set_process_nice(process.pid, current_nice)
                                throttled_start_time = now
                                last_nice_check_time = now
                                last_bg_usage = bg_pct
                                sys.stdout.write("\n")
                                print(
                                    f"\n[RESOURCE ALERT] Background CPU usage ({bg_pct:.1f}%) high for >{config.cpu_threshold_seconds}s. Throttled PID {process.pid} (nice={current_nice})."
                                )
                            else:
                                if (
                                    last_nice_check_time is not None
                                    and now - last_nice_check_time
                                    >= config.nice_check_interval_seconds
                                ):
                                    last_nice_check_time = now
                                    if bg_pct > last_bg_usage:
                                        next_nice = min(
                                            config.nice_max,
                                            current_nice + config.nice_step,
                                        )
                                        if next_nice != current_nice:
                                            current_nice = next_nice
                                            set_process_nice(
                                                process.pid, current_nice
                                            )
                                            sys.stdout.write("\n")
                                            print(
                                                f"\n[RESOURCE ALERT] Background CPU increased ({last_bg_usage:.1f}% -> {bg_pct:.1f}%). Escalated nice to {current_nice}."
                                            )
                                    last_bg_usage = bg_pct

                                if (
                                    throttled_start_time is not None
                                    and now - throttled_start_time
                                    >= config.max_throttle_duration_seconds
                                ):
                                    if not config.long_runnable:
                                        sys.stdout.write("\n")
                                        print(
                                            f"\n[RESOURCE ALERT] Task throttled for >{config.max_throttle_duration_seconds}s and long_runnable is False! Killing task process..."
                                        )
                                        force_stop_process(process)
                                        raise RuntimeError(
                                            f"Task process killed: throttled for >{config.max_throttle_duration_seconds}s and long_runnable is False"
                                        )
                else:
                    high_usage_start = None
                    if is_throttled:
                        if low_usage_start is None:
                            low_usage_start = now
                        elif (
                            now - low_usage_start
                            >= config.unthrottle_threshold_seconds
                        ):
                            is_throttled = False
                            current_nice = 0
                            set_process_nice(process.pid, 0)
                            throttled_start_time = None
                            last_nice_check_time = None
                            low_usage_start = None
                            sys.stdout.write("\n")
                            print(
                                f"\n[RESOURCE RECOVERY] Background CPU usage ({bg_pct:.1f}%) returned to normal. Restored nice to 0."
                            )

            pct = state.get("pct")
            eta = state.get("eta")

            status_prefix = (
                f"{PROGRESS_EXECUTING} ({filename}) [throttled nice={current_nice}]"
                if is_throttled
                else f"{PROGRESS_EXECUTING} ({filename})"
            )

            if pct is not None and eta is not None:
                display_str = f"\r{status_prefix} - {pct:.1f}% [ETA: {eta}]   "
            elif pct is not None:
                display_str = f"\r{status_prefix} - {pct:.1f}%   "
            else:
                display_str = f"\r{status_prefix} - running time: {formatted_time}   "

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

            if connection is not None:
                if has_buffered_message(connection):
                    msg = receive_json_message(connection)
                    if msg.get("type") == MESSAGE_TYPE_SHUTDOWN:
                        sys.stdout.write("\n")
                        print(
                            "\nMaster sent shutdown signal! Force-stopping running process..."
                        )
                        force_stop_process(process)
                        raise MasterShutdownError(
                            "Master sent shutdown signal during task execution"
                        )
                else:
                    try:
                        rlist, _, _ = select.select([connection], [], [], 0.0)
                        if rlist:
                            peek = connection.recv(1024, socket.MSG_PEEK)
                            if not peek:
                                sys.stdout.write("\n")
                                print(
                                    "\nConnection to server lost! Force-stopping running process..."
                                )
                                force_stop_process(process)
                                raise ConnectionError(
                                    "Server connection lost during task execution"
                                )
                            else:
                                msg = receive_json_message(connection)
                                if msg.get("type") == MESSAGE_TYPE_SHUTDOWN:
                                    sys.stdout.write("\n")
                                    print(
                                        "\nMaster sent shutdown signal! Force-stopping running process..."
                                    )
                                    force_stop_process(process)
                                    raise MasterShutdownError(
                                        "Master sent shutdown signal during task execution"
                                    )
                    except (OSError, ConnectionError):
                        sys.stdout.write("\n")
                        print(
                            "\nConnection to server lost! Force-stopping running process..."
                        )
                        force_stop_process(process)
                        raise ConnectionError(
                            "Server connection lost during task execution"
                        )

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

            try:
                rlist, _, _ = select.select([sys.stdin], [], [], 0.0)
                if rlist:
                    char = sys.stdin.read(1)
                    if char.lower() == "k":
                        sys.stdout.write("\n")
                        print(
                            "\n'k' key pressed! Force-killing task process and simulating node drop..."
                        )
                        force_stop_process(process)
                        raise KeyboardInterrupt("Task force-killed by user pressing 'k'")
            except Exception:
                pass

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
