"""Hardware benchmarking, system telemetry probing, and affinity ranking."""

from __future__ import annotations

import getpass
import hashlib
import json
import os
import platform
import re
import socket
import subprocess
import sys
import time
from collections import namedtuple
from pathlib import Path

from core.constants import WORKER_DIRECTORY


def get_num_cpus() -> int:
    """Native replacement for len(os.sched_getaffinity(0)) / os.cpu_count()."""
    try:
        return len(os.sched_getaffinity(0))
    except (AttributeError, OSError, ValueError):
        pass
    return os.cpu_count() or 1


def get_system_cpu_percent(interval: float = 0.1) -> float:
    """Native replacement for psutil.cpu_percent()."""
    # --- macOS Implementation ---
    if sys.platform == "darwin":
        try:
            output = subprocess.check_output(
                ["top", "-l", "1", "-n", "0"], text=True
            )
            # Parses line: "CPU usage: 12.34% user, 5.67% sys, 81.99% idle"
            match = re.search(
                r"CPU usage:\s*([\d\.]+)%\s*user,\s*([\d\.]+)%\s*sys,\s*([\d\.]+)%\s*idle",
                output,
            )
            if match:
                user_pct = float(match.group(1))
                sys_pct = float(match.group(2))
                return user_pct + sys_pct
        except Exception:
            return 0.0

    # --- Linux Implementation ---
    elif sys.platform.startswith("linux"):
        try:

            def _read_stat():
                with open("/proc/stat", "r") as f:
                    fields = [float(x) for x in f.readline().split()[1:]]
                # fields[3] = idle. Do NOT add fields[4] (iowait).
                idle = fields[3]
                return idle, sum(fields)

            idle1, total1 = _read_stat()
            time.sleep(interval)
            idle2, total2 = _read_stat()

            diff_total = total2 - total1
            if diff_total > 0:
                diff_idle = idle2 - idle1
                return (1.0 - (diff_idle / diff_total)) * 100.0
        except Exception:
            return 0.0

    return 0.0


def get_process_cpu_percent(pid: int, num_cpus: int | None = None) -> float:
    """Native replacement for: psutil.Process(pid).cpu_percent() / num_cpus"""
    if num_cpus is None:
        num_cpus = get_num_cpus()

    try:
        # `ps -p <pid> -o %cpu=` works on both macOS & Linux.
        # It outputs CPU usage on a single-core scale (e.g., 200.0 = 2 full cores used).
        output = subprocess.check_output(
            ["ps", "-p", str(pid), "-o", "%cpu="], text=True
        )
        raw_cpu_pct = float(output.strip())

        # Divide by core count to normalize to total system capacity (0% - 100%)
        return raw_cpu_pct / num_cpus
    except Exception:
        return 0.0


def benchmark_single_core(duration_seconds: float = 0.5) -> float:
    """Run single-threaded SHA-256 benchmark loop and return hashes per second."""
    data = b"benchmark_payload_block_" * 43
    count = 0
    start = time.time()
    while time.time() - start < duration_seconds:
        hashlib.sha256(data).digest()
        count += 1
    elapsed = time.time() - start
    return count / elapsed if elapsed > 0 else 0.0


def benchmark_multi_core(duration_seconds: float = 1.0) -> float:
    """Run multi-threaded CPU benchmark across all available CPU cores."""
    num_cores = get_num_cpus()
    per_core_duration = max(0.2, duration_seconds / 2)
    try:
        from multiprocessing import Pool

        with Pool(num_cores) as pool:
            scores = pool.starmap(
                benchmark_single_core, [(per_core_duration,)] * num_cores
            )
        return sum(scores)
    except Exception:
        single = benchmark_single_core(duration_seconds)
        return single * num_cores


def detect_cpu_name() -> str:
    """Detect brand string of the host processor."""
    cpu_name = "Unknown CPU"
    system = platform.system()
    if system == "Darwin":
        try:
            result = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True,
                text=True,
                timeout=2,
            )
            if result.returncode == 0 and result.stdout.strip():
                cpu_name = result.stdout.strip()
        except Exception:
            pass
    elif system == "Linux":
        try:
            with open("/proc/cpuinfo", "r") as f:
                for line in f:
                    if line.startswith("model name"):
                        cpu_name = line.split(":", 1)[1].strip()
                        break
        except Exception:
            pass
    else:
        try:
            cpu = platform.processor()
            if cpu:
                cpu_name = cpu
        except Exception:
            pass
    return cpu_name


def detect_gpu_info() -> dict:
    """Detect GPU hardware name, total VRAM, and supported hardware encoders."""
    info = {"gpu_name": "", "vram_mb": 0, "hw_encoders": []}
    if platform.system() == "Linux":
        try:
            result = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=name,memory.total",
                    "--format=csv,noheader,nounits",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                line = result.stdout.strip().split("\n")[0]
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 2:
                    info["gpu_name"] = parts[0]
                    info["vram_mb"] = int(float(parts[1]))
                    info["hw_encoders"].extend(["h264_nvenc", "hevc_nvenc"])
        except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
            pass
    if platform.system() == "Darwin":
        try:
            result = subprocess.run(
                ["system_profiler", "SPDisplaysDataType", "-json"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                data = json.loads(result.stdout)
                displays = data.get("SPDisplaysDataType", [])
                if displays:
                    gpu = displays[0]
                    info["gpu_name"] = gpu.get("sppci_model", "Apple Silicon GPU")
                    info["hw_encoders"].extend(
                        ["h264_videotoolbox", "hevc_videotoolbox"]
                    )
        except (
            FileNotFoundError,
            subprocess.TimeoutExpired,
            json.JSONDecodeError,
        ):
            pass
    try:
        result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            known_hw_encoders = [
                "h264_nvenc",
                "hevc_nvenc",
                "h264_videotoolbox",
                "hevc_videotoolbox",
                "h264_qsv",
                "hevc_qsv",
                "h264_vaapi",
                "hevc_vaapi",
            ]
            for enc in known_hw_encoders:
                if enc in result.stdout and enc not in info["hw_encoders"]:
                    info["hw_encoders"].append(enc)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return info


def sensors_battery():
    """Native battery sensor information query (percent, secsleft, power_plugged)."""
    POWER_TIME_UNKNOWN = -1
    POWER_TIME_UNLIMITED = -2
    sbattery = namedtuple("sbattery", ["percent", "secsleft", "power_plugged"])

    if sys.platform == "darwin":
        try:
            output = subprocess.check_output(["pmset", "-g", "batt"], text=True)
            match = re.search(r"(\d+)%;\s*([^;]+);", output)
            if not match:
                return None
            percent = int(match.group(1))
            status = match.group(2).strip().lower()

            power_plugged = "ac power" in output.lower() or status in (
                "charging",
                "charged",
                "finishing charge",
            )
            time_match = re.search(r"(\d+):(\d+)\s+remaining", output)
            if power_plugged:
                secsleft = (
                    POWER_TIME_UNLIMITED
                    if status == "charged"
                    else POWER_TIME_UNKNOWN
                )
            elif time_match:
                hours, mins = map(int, time_match.groups())
                secsleft = hours * 3600 + mins * 60
            else:
                secsleft = POWER_TIME_UNKNOWN

            return sbattery(
                percent=percent, secsleft=secsleft, power_plugged=power_plugged
            )
        except Exception:
            return None

    elif sys.platform.startswith("linux"):
        base_path = "/sys/class/power_supply"
        if not os.path.exists(base_path):
            return None

        for supply in os.listdir(base_path):
            if supply.startswith("BAT"):
                bat_path = os.path.join(base_path, supply)
                cap_file = os.path.join(bat_path, "capacity")
                status_file = os.path.join(bat_path, "status")
                if not os.path.exists(cap_file):
                    continue
                with open(cap_file, "r") as f:
                    percent = int(f.read().strip())
                status = "unknown"
                if os.path.exists(status_file):
                    with open(status_file, "r") as f:
                        status = f.read().strip().lower()
                power_plugged = status != "discharging"
                secsleft = POWER_TIME_UNKNOWN
                if power_plugged:
                    if status in ("full", "charged"):
                        secsleft = POWER_TIME_UNLIMITED
                else:
                    time_empty_file = os.path.join(bat_path, "time_to_empty_now")
                    if os.path.exists(time_empty_file):
                        try:
                            with open(time_empty_file, "r") as f:
                                secsleft = int(f.read().strip())
                        except ValueError:
                            pass

                return sbattery(
                    percent=percent,
                    secsleft=secsleft,
                    power_plugged=power_plugged,
                )

    try:
        import psutil

        return psutil.sensors_battery()
    except Exception:
        pass

    return None


def detect_power_state() -> dict:
    """Detect whether host laptop is running on AC power or discharging on battery."""
    info = {"is_plugged_in": True, "battery_pct": 100, "has_battery": False}
    try:
        battery = sensors_battery()
        if battery is not None:
            info["has_battery"] = True
            info["is_plugged_in"] = bool(battery.power_plugged)
            info["battery_pct"] = int(battery.percent)
    except Exception:
        pass
    return info


def _get_total_ram_mb() -> int:
    """Read total system RAM in Megabytes without external dependencies."""
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        phys_pages = os.sysconf("SC_PHYS_PAGES")
        if page_size > 0 and phys_pages > 0:
            return (page_size * phys_pages) // (1024 * 1024)
    except (ValueError, OSError, AttributeError):
        pass

    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) // 1024
    except Exception:
        pass

    if sys.platform == "darwin":
        try:
            out = subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True)
            return int(out.strip()) // (1024 * 1024)
        except Exception:
            pass

    try:
        import psutil

        return psutil.virtual_memory().total // (1024 * 1024)
    except Exception:
        pass

    return 0


def run_node_benchmark(custom_username: str | None = None) -> dict:
    """Run hardware benchmark binary (persistent cache) and return telemetry dict."""
    power = detect_power_state()
    gpu_info = detect_gpu_info()
    if custom_username:
        username = custom_username
    else:
        try:
            username = getpass.getuser()
        except Exception:
            username = "unknown"
    hostname_str = socket.gethostname()
    os_type_str = platform.system().lower()
    cpu_count_val = get_num_cpus()

    # Cache paths - check standard locations in root and worker directory
    worker_dir = Path(WORKER_DIRECTORY)
    candidate_cache_files = [
        Path("benchmark.json"),
        Path("benchmark_results.json"),
        worker_dir / "benchmark.json",
        worker_dir / "benchmark_results.json",
    ]
    REQUIRED_BENCH_KEYS = {
        "ram_mb",
        "vram_mb",
        "single_core_score",
        "multi_core_score",
        "gpu_score",
    }

    bench_data: dict | None = None

    # --- 1. Try loading from cache ---
    for cp in candidate_cache_files:
        if cp.is_file():
            try:
                with open(cp, "r") as _f:
                    _cached = json.load(_f)
                if REQUIRED_BENCH_KEYS.issubset(_cached.keys()):
                    bench_data = _cached
                    print(f"[BENCHMARK] Loaded cached results from {cp}")
                    break
            except Exception as _e:
                print(f"[BENCHMARK] Cache read from {cp} failed ({_e})")

    # --- 2. Run binary if no valid cache ---
    if bench_data is None:
        sys_name = platform.system()
        os_folder = "macos" if sys_name == "Darwin" else "linux"
        candidate_binaries = [
            Path("binaries") / "benchmark" / os_folder / "bench",
            Path(__file__).resolve().parent.parent
            / "binaries"
            / "benchmark"
            / os_folder
            / "bench",
        ]
        bench_binary = None
        for cb in candidate_binaries:
            if cb.is_file():
                bench_binary = cb
                break

        if bench_binary is not None:
            try:
                import stat as _stat

                if not os.access(bench_binary, os.X_OK):
                    bench_binary.chmod(
                        bench_binary.stat().st_mode
                        | _stat.S_IXUSR
                        | _stat.S_IXGRP
                        | _stat.S_IXOTH
                    )

                print(f"[BENCHMARK] Running {bench_binary} ...")
                result_proc = subprocess.run(
                    [str(bench_binary)],
                    capture_output=True,
                    text=True,
                    timeout=120,
                )
                if result_proc.returncode == 0 and result_proc.stdout.strip():
                    bench_data = json.loads(result_proc.stdout.strip())
                    if not REQUIRED_BENCH_KEYS.issubset(bench_data.keys()):
                        print(
                            "[BENCHMARK] Binary output missing expected keys, falling back to Python benchmark"
                        )
                        bench_data = None
                else:
                    print(
                        f"[BENCHMARK] Binary exited {result_proc.returncode}: {result_proc.stderr.strip()[:200]}"
                    )
            except Exception as _e:
                print(
                    f"[BENCHMARK] Binary failed ({_e}), falling back to Python benchmark"
                )
                bench_data = None
        else:
            print(
                f"[BENCHMARK] Binary not found at ./binaries/benchmark/{os_folder}/bench, using Python benchmark"
            )

    # --- 3. Python fallback if binary failed/missing ---
    if bench_data is None:
        print("[BENCHMARK] Running Python-based benchmark (slower)...")
        bench_data = {
            "single_core_score": round(benchmark_single_core(0.5), 1),
            "multi_core_score": round(benchmark_multi_core(1.0), 1),
            "gpu_score": float(gpu_info.get("vram_mb", 0)) * 10.0,
            "ram_mb": _get_total_ram_mb(),
            "vram_mb": gpu_info.get("vram_mb", 0),
        }

    # --- 4. Save cache for persistent access ---
    for save_path in [Path("benchmark.json"), worker_dir / "benchmark_results.json"]:
        try:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, "w") as _f:
                json.dump(bench_data, _f, indent=2)
            print(f"[BENCHMARK] Results saved to {save_path}")
        except Exception:
            pass

    # --- 5. Build full telemetry result ---
    cpu_name_str = detect_cpu_name()
    result = {
        "cpu_name": cpu_name_str,
        "single_core_score": float(bench_data["single_core_score"]),
        "multi_core_score": float(bench_data["multi_core_score"]),
        "gpu_score": float(bench_data.get("gpu_score", 0.0)),
        "ram_mb": int(bench_data.get("ram_mb", _get_total_ram_mb())),
        "gpu_name": gpu_info["gpu_name"],
        "vram_mb": int(bench_data.get("vram_mb", gpu_info["vram_mb"])),
        "hw_encoders": gpu_info["hw_encoders"],
        "os_type": os_type_str,
        "cpu_count": cpu_count_val,
        "total_ram_mb": int(bench_data.get("ram_mb", _get_total_ram_mb())),
        "is_plugged_in": power.get("is_plugged_in", True),
        "battery_pct": power.get("battery_pct", 100),
        "has_battery": power.get("has_battery", False),
        "hostname": hostname_str,
        "username": username,
    }

    # --- 6. Print summary ---
    print(f"  CPU: {result['cpu_name']} ({result['cpu_count']} cores)")
    print(
        f"  Single-core: {result['single_core_score']:.1f} | Multi-core: {result['multi_core_score']:.1f}"
    )
    print(f"  RAM: {result['ram_mb']} MB")
    if result["gpu_score"] > 0:
        print(
            f"  GPU score: {result['gpu_score']:.1f} | VRAM: {result['vram_mb']} MB"
        )
    else:
        print(
            "  GPU: not detected (gpu_score=0, treated as lowest priority for GPU tasks)"
        )
    print(
        f"  Power: {'AC' if result['is_plugged_in'] else 'Battery'} ({result['battery_pct']}%)"
    )

    return result


def rank_workers_by_affinity(
    telemetry_map: dict[str, dict],
    resource_type: str,
    min_vram_mb: int = 0,
    worker_addresses: list[str] | None = None,
) -> list[str]:
    """Rank worker IPs by affinity for the given resource_type."""

    def _get_telemetry(addr: str) -> dict:
        if addr in telemetry_map:
            return telemetry_map[addr]
        base_ip = addr.split(":")[0]
        if base_ip in telemetry_map:
            return telemetry_map[base_ip]
        return {}

    def _is_low_battery(tel: dict) -> bool:
        return (
            bool(tel.get("has_battery", False))
            and tel.get("battery_pct", 100) < 20
            and not tel.get("is_plugged_in", True)
        )

    def _has_no_telemetry(tel: dict) -> bool:
        return not tel or tel.get("single_core_score") is None

    # Determine candidate addresses to rank
    if worker_addresses is not None:
        candidates = []
        for addr in worker_addresses:
            if addr not in candidates:
                candidates.append(addr)
    else:
        candidates = list(telemetry_map.keys())

    rt = (resource_type or "auto").lower()

    if rt == "gpu":
        gpu_capable = []
        gpu_lacking = []
        for addr in candidates:
            tel = _get_telemetry(addr)
            vram = int(tel.get("vram_mb", 0) or 0)
            gscore = float(tel.get("gpu_score", 0.0) or 0.0)
            low_bat = _is_low_battery(tel)
            no_tel = _has_no_telemetry(tel)

            qualifies = (
                gscore > 0.0 and vram > 0 and vram >= min_vram_mb and not no_tel
            )
            entry = (addr, tel, low_bat, no_tel, gscore, vram)
            if qualifies:
                gpu_capable.append(entry)
            else:
                gpu_lacking.append(entry)

        # Sort GPU-capable: high priority
        gpu_capable.sort(
            key=lambda x: (
                1 if x[2] else 0,
                -(x[4]),
                -(x[5]),
                -float(x[1].get("multi_core_score", 0.0) or 0.0),
            )
        )

        # Sort GPU-lacking: LEAST priority
        gpu_lacking.sort(
            key=lambda x: (
                1 if x[3] else 0,
                1 if x[2] else 0,
                -float(x[1].get("multi_core_score", 0.0) or 0.0),
                -int(x[1].get("ram_mb", 0) or 0),
            )
        )

        ranked = [x[0] for x in gpu_capable] + [x[0] for x in gpu_lacking]
        return ranked

    elif rt == "single_core":

        def _sc_key(addr: str):
            tel = _get_telemetry(addr)
            low_bat = _is_low_battery(tel)
            no_tel = _has_no_telemetry(tel)
            sc_score = float(tel.get("single_core_score", 0.0) or 0.0)
            mc_score = float(tel.get("multi_core_score", 0.0) or 0.0)
            return (
                1 if no_tel else 0,
                1 if low_bat else 0,
                -sc_score,
                -mc_score,
            )

        sorted_candidates = sorted(candidates, key=_sc_key)
        return sorted_candidates

    else:  # multi_core / auto / cpu / default

        def _mc_key(addr: str):
            tel = _get_telemetry(addr)
            low_bat = _is_low_battery(tel)
            no_tel = _has_no_telemetry(tel)
            mc_score = float(tel.get("multi_core_score", 0.0) or 0.0)
            sc_score = float(tel.get("single_core_score", 0.0) or 0.0)
            cores = int(tel.get("cpu_count", 1) or 1)
            return (
                1 if no_tel else 0,
                1 if low_bat else 0,
                -mc_score,
                -sc_score,
                -cores,
            )

        sorted_candidates = sorted(candidates, key=_mc_key)
        return sorted_candidates
