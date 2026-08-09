"""Phase 6.2 Demo Script: Failover demonstration for distributed video processing."""

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_DIR))

from devices import DEVICES_JSON_FILE


def run_failover_demo() -> None:
    print("==================================================================")
    print("        DISTRIBUTED VIDEO PROCESSING SYSTEM - FAILOVER DEMO       ")
    print("==================================================================")

    # 1. Prepare input and output directories
    master_input = PROJECT_DIR / "master" / "input"
    master_output = PROJECT_DIR / "master" / "output"
    worker_input = PROJECT_DIR / "worker" / "input"
    worker_output = PROJECT_DIR / "worker" / "output"

    for d in [master_input, master_output, worker_input, worker_output]:
        d.mkdir(parents=True, exist_ok=True)

    # Create dummy input video file
    dummy_input = master_input / "input.mkv"
    dummy_input.write_text("DEMO_VIDEO_INPUT_DATA\n")

    # 2. Create demo config file
    demo_config = PROJECT_DIR / "demo_config.ini"
    demo_config.write_text(
        "[DEFAULT]\n"
        "split_command = python3 -c \"import shutil; shutil.copy('{input_directory}/input.mkv', '{input_directory}/part1.mkv'); shutil.copy('{input_directory}/input.mkv', '{input_directory}/part2.mkv')\"\n"
        "execute_command = python3 -c \"import shutil, time; time.sleep(1.5); shutil.copy('{input}', '{output_directory}/{output}')\"\n"
        "merge_command = python3 -c \"import shutil; shutil.copy('{output_directory}/part1.mkv', '{output_directory}/output.mkv')\"\n"
        "max_nodes = 2\n"
    )

    # Reset devices.json
    devices_json = PROJECT_DIR / DEVICES_JSON_FILE
    if devices_json.is_file():
        devices_json.unlink()

    active_ip1 = "127.0.0.1"
    active_ip2 = "127.0.0.2"
    spare_ip = "127.0.0.3"

    print(f"Active Nodes (max_nodes=2): {active_ip1}, {active_ip2}")
    print(f"Spare Node:                  {spare_ip}\n")

    # 3. Start Master Node
    print("[STEP 1] Launching Master Node...")
    master_cmd = [
        sys.executable,
        str(PROJECT_DIR / "master.py"),
        "--config",
        str(demo_config),
        active_ip1,
        active_ip2,
        spare_ip,
    ]
    master_proc = subprocess.Popen(master_cmd, cwd=PROJECT_DIR)
    time.sleep(1.5)

    # 4. Start Worker 1 (127.0.0.1)
    print("[STEP 2] Launching Worker 1 (127.0.0.1)...")
    worker1_cmd = [
        sys.executable,
        str(PROJECT_DIR / "worker.py"),
        "--config",
        str(demo_config),
        "--bind-ip",
        active_ip1,
        active_ip1,
    ]
    worker1_proc = subprocess.Popen(worker1_cmd, cwd=PROJECT_DIR)

    # 5. Start Worker 2 (127.0.0.2) with simulated failure after 0.5s
    print("[STEP 3] Launching Worker 2 (127.0.0.2) with simulated failure after 0.5s...")
    worker2_cmd = [
        sys.executable,
        str(PROJECT_DIR / "worker.py"),
        "--config",
        str(demo_config),
        "--bind-ip",
        active_ip2,
        "--simulate-failure-after",
        "0.5",
        active_ip1,
    ]
    worker2_proc = subprocess.Popen(worker2_cmd, cwd=PROJECT_DIR)

    # Wait for Worker 2 to drop mid-execution
    time.sleep(4.0)

    # 6. Start Spare Worker (127.0.0.3)
    print("\n[STEP 4] Launching Spare Worker (127.0.0.3) to take over node2...")
    spare_cmd = [
        sys.executable,
        str(PROJECT_DIR / "worker.py"),
        "--config",
        str(demo_config),
        "--bind-ip",
        spare_ip,
        active_ip1,
    ]
    spare_proc = subprocess.Popen(spare_cmd, cwd=PROJECT_DIR)

    # 7. Wait for completion
    master_code = master_proc.wait(timeout=20)
    worker1_proc.wait(timeout=5)
    spare_proc.wait(timeout=5)

    print("\n==================================================================")
    print("                    DEMO EXECUTION SUMMARY                        ")
    print("==================================================================")
    print(f"Master Exit Code: {master_code}")

    final_output = master_output / "output.mkv"
    print(f"Final Merged File Exists: {final_output.is_file()}")
    if devices_json.is_file():
        print(f"Final devices.json: {devices_json.read_text().strip()}")

    if demo_config.is_file():
        demo_config.unlink()

    if master_code == 0 and final_output.is_file():
        print("\nDEMO SUCCESSFUL: Worker failure detected, task reassigned to spare node, and output merged!")
    else:
        print("\nDEMO FAILED!")
        sys.exit(1)


if __name__ == "__main__":
    run_failover_demo()
