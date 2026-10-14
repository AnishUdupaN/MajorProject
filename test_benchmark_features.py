"""Unit test suite verifying:
1. Benchmark binary execution and persistent filesystem caching.
2. Hardware telemetry schema and validation.
3. Affinity priority ranking for GPU, single-core, multi-core/auto, low-battery, and zero-GPU devices.
4. Spare nodes detection and warning logic.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from core import (
    validate_message,
    MESSAGE_TYPE_WORKER_TELEMETRY,
    rank_workers_by_affinity,
    run_node_benchmark,
)
from master import split_active_and_spare_addresses

def test_telemetry_schema():
    print("--- Test 1: Telemetry Message Schema ---")
    valid_msg = {
        "type": MESSAGE_TYPE_WORKER_TELEMETRY,
        "single_core_score": 210.47,
        "multi_core_score": 1169.67,
        "gpu_score": 0.00,
        "ram_mb": 15700,
        "os_type": "linux",
        "cpu_count": 8,
    }
    validate_message(valid_msg)
    print("✓ Valid telemetry message passed validation")

    invalid_msg = {
        "type": MESSAGE_TYPE_WORKER_TELEMETRY,
        "single_core_score": 210.47,
        # missing gpu_score, ram_mb
    }
    try:
        validate_message(invalid_msg)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        print(f"✓ Invalid message caught properly: {e}")

def test_benchmark_caching():
    print("\n--- Test 2: Benchmark Persistence & Caching ---")
    # Verify benchmark.json exists and has valid values
    assert Path("benchmark.json").is_file(), "benchmark.json should exist"
    with open("benchmark.json") as f:
        data = json.load(f)
    print("Loaded benchmark.json:", data)
    assert "single_core_score" in data
    assert "multi_core_score" in data
    assert "gpu_score" in data
    assert "ram_mb" in data
    assert "vram_mb" in data
    print("✓ Cache file format is valid")

    # Re-running benchmark should load from cache
    res = run_node_benchmark()
    assert res["single_core_score"] == data["single_core_score"]
    assert res["multi_core_score"] == data["multi_core_score"]
    print("✓ run_node_benchmark successfully fetched cached results directly")

def test_priority_ranking():
    print("\n--- Test 3: Priority Ranking by Affinity ---")
    nodes = {
        "node_gpu_high": {
            "gpu_score": 64527.75,
            "vram_mb": 12124,
            "single_core_score": 396.27,
            "multi_core_score": 3083.09,
            "ram_mb": 16384,
            "is_plugged_in": True,
            "has_battery": False,
        },
        "node_gpu_mid": {
            "gpu_score": 25000.00,
            "vram_mb": 6144,
            "single_core_score": 300.0,
            "multi_core_score": 2500.0,
            "ram_mb": 16384,
            "is_plugged_in": True,
            "has_battery": False,
        },
        "node_gpu_low_battery": {
            "gpu_score": 70000.00,
            "vram_mb": 16384,
            "single_core_score": 400.0,
            "multi_core_score": 3500.0,
            "ram_mb": 32768,
            "is_plugged_in": False,
            "has_battery": True,
            "battery_pct": 12,  # Low battery!
        },
        "node_zero_gpu_fast_cpu": {
            "gpu_score": 0.00,
            "vram_mb": 0,
            "single_core_score": 450.0,
            "multi_core_score": 4200.0,
            "ram_mb": 32768,
            "is_plugged_in": True,
            "has_battery": False,
        },
        "node_zero_gpu_slow_cpu": {
            "gpu_score": 0.00,
            "vram_mb": 0,
            "single_core_score": 150.0,
            "multi_core_score": 900.0,
            "ram_mb": 8192,
            "is_plugged_in": True,
            "has_battery": False,
        },
    }

    # Test GPU task ranking:
    # 1. node_gpu_high (top GPU)
    # 2. node_gpu_mid (mid GPU)
    # 3. node_gpu_low_battery (penalized due to low battery despite high score)
    # 4. node_zero_gpu_fast_cpu (zero GPU -> least priority, but faster CPU fallback)
    # 5. node_zero_gpu_slow_cpu (zero GPU -> least priority, slower CPU fallback)
    gpu_ranked = rank_workers_by_affinity(nodes, "gpu", min_vram_mb=4000)
    print("GPU Ranking:", gpu_ranked)
    assert gpu_ranked[0] == "node_gpu_high", f"Expected node_gpu_high first, got {gpu_ranked[0]}"
    assert gpu_ranked[1] == "node_gpu_mid", f"Expected node_gpu_mid second, got {gpu_ranked[1]}"
    assert gpu_ranked[2] == "node_gpu_low_battery", f"Expected node_gpu_low_battery third, got {gpu_ranked[2]}"
    assert gpu_ranked[3] == "node_zero_gpu_fast_cpu", f"Expected zero GPU fast CPU next, got {gpu_ranked[3]}"
    assert gpu_ranked[4] == "node_zero_gpu_slow_cpu", f"Expected zero GPU slow CPU last, got {gpu_ranked[4]}"
    print("✓ GPU ranking correctly prioritizes GPU-capable devices and places zero-GPU devices at least priority")

    # Test Single-core task ranking:
    # 1. node_zero_gpu_fast_cpu (450 single core)
    # 2. node_gpu_high (396.27 single core)
    # 3. node_gpu_mid (300 single core)
    # 4. node_zero_gpu_slow_cpu (150 single core)
    # 5. node_gpu_low_battery (400 single core, but low battery)
    sc_ranked = rank_workers_by_affinity(nodes, "single_core")
    print("Single-core Ranking:", sc_ranked)
    assert sc_ranked[0] == "node_zero_gpu_fast_cpu"
    assert sc_ranked[1] == "node_gpu_high"
    assert sc_ranked[-1] == "node_gpu_low_battery"
    print("✓ Single-core ranking correctly sorts by single-core score and applies battery penalty")

    # Test Multi-core task ranking:
    # 1. node_zero_gpu_fast_cpu (4200 multi core)
    # 2. node_gpu_high (3083 multi core)
    # 3. node_gpu_mid (2500 multi core)
    # 4. node_zero_gpu_slow_cpu (900 multi core)
    # 5. node_gpu_low_battery (3500 multi core, but low battery)
    mc_ranked = rank_workers_by_affinity(nodes, "multi_core")
    print("Multi-core Ranking:", mc_ranked)
    assert mc_ranked[0] == "node_zero_gpu_fast_cpu"
    assert mc_ranked[1] == "node_gpu_high"
    assert mc_ranked[-1] == "node_gpu_low_battery"
    print("✓ Multi-core ranking correctly sorts by multi-core score")

def test_spare_nodes_split():
    print("\n--- Test 4: Spare Nodes Detection Scenario ---")
    # Scenario from user:
    # "there are total 3 devices. task requires atleast 3. no spare nodes left."
    devices = ["node1", "node2", "node3"]
    max_nodes = 3
    min_devices = 3

    active, spares = split_active_and_spare_addresses(devices, max_nodes)
    print(f"Active: {active}, Spares: {spares}")
    assert len(active) == 3
    assert len(spares) == 0
    assert not spares, "Should detect no spare nodes left"
    print("✓ Successfully detects 0 spare nodes when total devices == active nodes required")

if __name__ == "__main__":
    test_telemetry_schema()
    test_benchmark_caching()
    test_priority_ranking()
    test_spare_nodes_split()
    print("\nALL TESTS PASSED SUCCESSFULLY!")
