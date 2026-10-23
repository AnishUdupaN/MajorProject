import os
import re
import subprocess
import sys
import time
from collections import namedtuple



def sensors_battery():
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

            power_plugged = ("ac power" in output.lower() or status in ("charging", "charged", "finishing charge"))
            time_match = re.search(r"(\d+):(\d+)\s+remaining", output)
            if power_plugged:
                secsleft = (POWER_TIME_UNLIMITED if status == "charged" else POWER_TIME_UNKNOWN)
            elif time_match:
                hours, mins = map(int, time_match.groups())
                secsleft = hours * 3600 + mins * 60
            else:
                secsleft = POWER_TIME_UNKNOWN

            return sbattery(percent=percent, secsleft=secsleft, power_plugged=power_plugged)
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

    return None





def get_num_cpus():
    """Native replacement for len(os.sched_getaffinity(0)) / os.cpu_count()."""
    return os.cpu_count() or 1


def get_system_cpu_percent(interval=0.1):
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
                idle = fields[3] + (fields[4] if len(fields) > 4 else 0)
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


def get_process_cpu_percent(pid, num_cpus=None):
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
    except (subprocess.CalledProcessError, ValueError):
        return 0.0
battery = sensors_battery()
print(battery)
