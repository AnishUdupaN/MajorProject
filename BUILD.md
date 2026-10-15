# Build & Developer Guide

This document covers compiling the native hardware benchmark tool, organizing pre-compiled binaries, running local simulations and test suites, and troubleshooting common issues.

---

## 1. Compiling the Benchmark Program

The project uses a small C program (`benchmark.c`) to measure CPU single-core, multi-core, and OpenCL GPU performance on worker machines. It uses standard POSIX threads and dynamically loads OpenCL at runtime (`dlopen`), so you only need a standard C compiler (`gcc` or `clang`).

### Build Command

First ensure the output folder exists:
```bash
mkdir -p binaries/benchmark/linux
mkdir -p binaries/benchmark/macos
```

Compile for Linux:
```bash
gcc -O3 -pthread worker/benchmark.c -ldl -o binaries/benchmark/linux/bench
chmod +x binaries/benchmark/linux/bench
```

Compile for macOS:
```bash
clang -O3 -pthread worker/benchmark.c -ldl -o binaries/benchmark/macos/bench
chmod +x binaries/benchmark/macos/bench
```

### Running and Verifying

Run the binary directly to confirm it detects your hardware and prints valid JSON:

```bash
# Run the full benchmark (single-core, multi-core, and GPU if present)
./binaries/benchmark/$(uname -s | tr '[:upper:]' '[:lower:]')/bench

# Run only single-core CPU test
./binaries/benchmark/$(uname -s | tr '[:upper:]' '[:lower:]')/bench --singlecore

# Run only multi-core CPU test
./binaries/benchmark/$(uname -s | tr '[:upper:]' '[:lower:]')/bench --multicore

# Test GPU with debug output
./binaries/benchmark/$(uname -s | tr '[:upper:]' '[:lower:]')/bench --gpu --debug
```

Expected output:
```json
{
  "ram_mb": 16384,
  "vram_mb": 4096,
  "single_core_score": 215.40,
  "multi_core_score": 1420.80,
  "gpu_score": 850.25
}
```

---

## 2. Setting Up Binary Distribution

If worker machines don't have tools like `ffmpeg` or `blender` installed locally, the master can automatically push pre-compiled portable binaries to them during initial connection.

Place binaries or helper packages directly in the `binaries/` directory by application name and operating system:

```
binaries/
├── benchmark/
│   ├── linux/
│   │   └── bench
│   └── macos/
│       └── bench
├── blender/
│   ├── linux/
│   │   └── blender
│   └── macos/
│       └── MacOS/Blender
├── ffmpeg/
│   ├── linux/
│   │   └── ffmpeg
│   └── macos/
│       └── ffmpeg
├── encode_hevc_helper/
│   ├── linux/
│   │   └── encode_hevc_helper.py
│   └── macos/
│       └── encode_hevc_helper.py
└── transcribe_helper/
    ├── linux/
    │   └── transcribe_helper.py
    └── macos/
        └── transcribe_helper.py
```

### Binary Path Resolution

The system's binary resolver resolves paths up to `./binaries/<binary_name>/<os_name>/` (on the master) and `./worker/binaries/<binary_name>/<os_name>/` (on the worker). The config specifies the rest:

- **Single-file binaries** (e.g. `ffmpeg`): The directory is automatically added to `PATH`, so your config can directly execute `ffmpeg --args` (or `encode_hevc_helper.py {input} {output_directory}/{output}`).
- **Multi-directory packages** (e.g. applications with nested binaries or helper scripts inside subdirectories): The config specifies the relative subpath inside `execute_command`, such as `MacOS/Blender -b ...` or `run/maintask/main --args`. The worker resolves it against the platform base folder.

In your pipeline `.ini` config file:
```ini
binary_name = ffmpeg
require_binary = true
```

The master computes an MD5 checksum of the binary and only transfers it if the worker's local copy is missing or has a different hash.

---

## 3. Local Multi-Node Simulation

You can test the entire distributed workflow on a single computer using loopback IP aliases (`127.0.0.1`, `127.0.0.2`, `127.0.0.3`):

### Terminal 1 — Start the Master
```bash
python3 master/master.py --config config/sample_compress_and_grayscale.ini --password testpass
```

### Terminal 2 — Start Worker 1
```bash
python3 worker/worker.py 127.0.0.1 --bind-ip 127.0.0.2 --username "worker-1" --password testpass
```

### Terminal 3 — Start Worker 2
```bash
python3 worker/worker.py 127.0.0.1 --bind-ip 127.0.0.3 --username "worker-2" --password testpass
```

### Terminal 4 — Start a Spare Worker
```bash
python3 worker/worker.py 127.0.0.1 --bind-ip 127.0.0.4 --username "spare-worker" --password testpass
```

---

## 4. Running Tests

All test and simulation scripts live in the `simulations_and_testing/` directory:

```bash
# 1. Hardware benchmark caching and affinity sorting test
python3 simulations_and_testing/test_benchmark_features.py

# 2. Automated worker failure and spare node replacement simulation
python3 simulations_and_testing/demo_failover.py
```

---

## 5. Diagnostics & Troubleshooting

### Discovery fails on mobile hotspot
- **Cause:** Mobile hotspots often do not route mDNS multicast (`224.0.0.251:5353`).
- **Fix:** UDP broadcast on port `5005` handles this automatically, but if your firewall blocks UDP traffic, specify the master's IP directly when launching the worker:
  ```bash
  python3 worker/worker.py <MASTER_IP>
  ```

### PIN pairing prompts appear during automated runs
- **Cause:** By default, connections require entering the 4-digit PIN displayed on the master's screen.
- **Fix:** Pass `--password <secret>` to both master and worker to use static authentication.

### GPU score reports 0.00
- **Cause:** OpenCL libraries are missing or no compatible GPU driver is active.
- **Fix:** The system automatically falls back to CPU multi-core execution. To enable GPU acceleration, install the appropriate OpenCL driver for your hardware.

### Dynamic CPU throttling isn't changing process priority
- **Cause:** Changing process priority via `nice` requires `psutil` if system calls fail without elevated privileges.
- **Fix:** Run `pip install psutil` so the worker can use the `psutil` priority fallback.
