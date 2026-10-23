# LAN Distributed Compute System

A lightweight, cross-platform distributed task runner designed for local networks. If you have a compute-heavy task (like video transcoding or 3D rendering) and a few machines on the same Wi-Fi or mobile hotspot, this project splits the work across them, runs the tasks directly on each machine, and merges the finished output back together.

---

## What It Does

1. **Splits Work**: Takes a large file or job and divides it into chunks.
2. **Auto-Discovers Nodes**: Workers find the master on the local network automatically using mDNS and UDP broadcast.
3. **Pairs Securely**: Workers authenticate using a 4-digit PIN displayed on the master's screen (or an optional pre-shared password).
4. **Benchmarks Hardware**: Evaluates each worker's CPU (single-core and multi-core) and OpenCL GPU to match tasks to the fastest nodes.
5. **Runs Bare-Metal**: Executes tasks directly on the host without container overhead, allowing full use of native hardware and GPU acceleration.
6. **Throttles Dynamically**: Monitors background CPU load on worker machines. If someone starts using their machine, the worker process automatically lowers its priority (`nice` 5 to 19) so their computer stays responsive.
7. **Handles Failures**: Automatically retries disconnected workers or reassigns missing chunks to idle spare nodes.
8. **Merges Results**: Collects finished chunks via a built-in HTTP file transfer daemon and joins them into the final file.

---

## Quick Start

### 1. Requirements

- Python 3.10+
- The `psutil` package:
  ```bash
  pip install -r requirements.txt
  ```
- Any tools required by your tasks (e.g. `ffmpeg`, `blender`).

To compile the native hardware benchmark tool (`worker/benchmark.c`), see [BUILD.md](BUILD.md).

### 2. Start the Master

Run `master/master.py` on the machine that holds the source file and will assemble the final output:

```bash
# Start in interactive mode (choose config on screen)
python3 master/master.py

# Or start with a specific config and enable the web dashboard
python3 master/master.py --config config/sample_compress_and_grayscale.ini --webserver
```

When started, the master displays a 4-digit pairing PIN on its terminal dashboard.

### 3. Start Workers

Run `worker/worker.py` on any other machine connected to the same Wi-Fi:

```bash
# Auto-discover master on local network
python3 worker/worker.py

# Or connect directly by IP
python3 worker/worker.py 192.168.1.50
```

The worker will ask for the 4-digit PIN shown on the master's terminal. Once verified, it benchmarks its hardware, reports telemetry back to the master, and waits for task assignments.

---

## Example Tasks & Configurations

Tasks are defined in standard `.ini` configuration files. You specify how to split the work, what command each worker should run on its chunk, and how to merge the finished chunks.

### Configuration Template Placeholders

When writing commands, use these placeholders:
- `{input_directory}`: Directory on master where the original file and split chunks live.
- `{output_directory}`: Directory where finished chunks are saved before merging.
- `{segment_times}`: Comma-separated timestamps calculated by the master for video splits.
- `{input}`: Path to the downloaded chunk on the worker machine.
- `{output}`: Expected output filename for the chunk on the worker.
- `{filelist}`: File listing all parts in order for concat tools.

---

### 1. FFmpeg: Video Transcoding (H.264 / HEVC)

Splits a video into equal parts, re-encodes each part across available nodes, and concatenates them back together:

```ini
[DEFAULT]
split_command = ffmpeg -y -ss 00:00:00 -i {input_directory}/input.mkv -to 00:30:00 -c copy -map 0 -f segment -segment_times {segment_times} -segment_start_number 1 -reset_timestamps 1 {input_directory}/part%d.mkv
merge_command = ffmpeg -y -f concat -safe 0 -i filelist.txt -c copy {output_directory}/output.mkv

max_nodes = 4
resource_type = multi_core

# Cross-platform encoding command
execute_command = ffmpeg -y -i {input} -c:v libx264 -preset slow -crf 22 -c:a aac -b:a 192k {output_directory}/{output}
```

*(Note: If a task requires different commands on different operating systems, use a helper script that inspects the OS and run that helper script in `execute_command` instead of bloating the config).*

---

### 2. FFmpeg: Video Filters, Scaling & Grayscale

Applies heavy CPU-bound video filters such as resolution downscaling and color space conversion:

```ini
[DEFAULT]
split_command = ffmpeg -y -i {input_directory}/input.mkv -c copy -f segment -segment_times {segment_times} -segment_start_number 1 -reset_timestamps 1 {input_directory}/part%d.mkv
merge_command = ffmpeg -y -f concat -safe 0 -i filelist.txt -c copy {output_directory}/output.mkv

max_nodes = 4
resource_type = multi_core

execute_command = ffmpeg -y -i {input} -vf "format=gray,scale=1280:-2" -c:v libx264 -preset medium -crf 24 -c:a copy {output_directory}/{output}
```

---

### 3. FFmpeg: Video Denoising & Cleaning

Runs intensive temporal and spatial denoising (`nlmeans` or `hqdn3d`):

```ini
[DEFAULT]
split_command = ffmpeg -y -i {input_directory}/input.mkv -c copy -f segment -segment_times {segment_times} -segment_start_number 1 -reset_timestamps 1 {input_directory}/part%d.mkv
merge_command = ffmpeg -y -f concat -safe 0 -i filelist.txt -c copy {output_directory}/output.mkv

max_nodes = 4
resource_type = multi_core

execute_command = ffmpeg -y -i {input} -vf "nlmeans=s=1.5:p=7:r=15" -c:v libx264 -preset slow -crf 20 -c:a copy {output_directory}/{output}
```

---

### 4. FFmpeg: Audio Extraction & Loudness Normalization

Splits long audio recordings and applies dual-pass EBU R128 loudness normalization:

```ini
[DEFAULT]
split_command = ffmpeg -y -i {input_directory}/input.wav -f segment -segment_times {segment_times} -segment_start_number 1 {input_directory}/part%d.wav
merge_command = ffmpeg -y -f concat -safe 0 -i filelist.txt -c copy {output_directory}/normalized_output.wav

max_nodes = 4
resource_type = single_core

execute_command = ffmpeg -y -i {input} -af "loudnorm=I=-16:TP=-1.5:LRA=11" {output_directory}/{output}
```

---

### 5. Blender: Distributed 3D Animation Frame Rendering

Distributes animation rendering across machines using Blender's command-line interface:

```ini
[DEFAULT]
# Distribute the project file to 4 worker slots
split_command = python3 -c "import shutil; [shutil.copy('{input_directory}/scene.blend', '{input_directory}/part%d.blend' % i) for i in range(1, 5)]"

# Assemble rendered frame images into a movie file
merge_command = ffmpeg -y -framerate 30 -i {output_directory}/frame_%%04d.png -c:v libx264 -pix_fmt yuv420p {output_directory}/render.mp4

max_nodes = 4
resource_type = gpu
min_vram_mb = 2048

# Each worker renders frames using Cycles
execute_command = blender -b {input} -o {output_directory}/frame_#### -E CYCLES -F PNG -a
```

---

### 6. Batch Image Processing (ImageMagick)

Processes large collections of images (e.g. resizing, sharpening, and format conversion):

```ini
[DEFAULT]
split_command = python3 scripts/split_archive.py {input_directory}/photos.tar {input_directory}
merge_command = python3 scripts/merge_archive.py {output_directory} {output_directory}/processed_photos.tar

max_nodes = 6
resource_type = multi_core

execute_command = bash -c "mkdir -p tmp && tar -xf {input} -C tmp && magick mogrify -resize 1920x1080 -quality 85 tmp/*.jpg && tar -cf {output_directory}/{output} -C tmp ."
```

---

### 7. Audio Transcription (Whisper AI)

Transcribes long interviews or recordings in parallel by segmenting audio and running speech-to-text models:

```ini
[DEFAULT]
split_command = ffmpeg -y -i {input_directory}/audio.mp3 -f segment -segment_times {segment_times} -segment_start_number 1 {input_directory}/part%d.mp3
merge_command = python3 scripts/concat_transcripts.py {output_directory} {output_directory}/full_transcript.srt

max_nodes = 4
resource_type = gpu

execute_command = whisper {input} --model medium --output_format srt --output_dir {output_directory}
```

---

### 8. Parallel Data Compression (`zstd`)

Compresses large raw files in parallel:

```ini
[DEFAULT]
split_command = split -n 4 -d {input_directory}/dump.bin {input_directory}/part
merge_command = cat {output_directory}/part*.zst > {output_directory}/dump.bin.zst

max_nodes = 4
resource_type = multi_core

execute_command = zstd -19 -T0 {input} -o {output_directory}/{output}.zst
```

---

## Configuration Settings

You can adjust how nodes are chosen and managed inside the `[DEFAULT]` block of any config:

| Key | Default | Description |
|---|---|---|
| `split_command` | *required* | Shell command to split input into numbered parts (`part1.ext`, `part2.ext`...). |
| `execute_command` | *required* | Command workers run on their assigned chunk. |
| `merge_command` | *required* | Command to merge completed chunks on the master. |
| `max_nodes` | `4` | Maximum active workers to use for the task. |
| `min_nodes` | `1` | Minimum workers needed before starting execution. |
| `resource_type` | `multi_core` | Affinity type: `multi_core`, `single_core`, or `gpu`. |
| `min_vram_mb` | `0` | Minimum GPU VRAM (MB) needed to assign GPU tasks to a node. |
| `on_high_usage` | `throttle` | Action when host CPU spikes: `throttle` or `kill`. |
| `cpu_threshold_percent`| `70.0` | Host background CPU percentage that triggers throttling. |
| `cpu_threshold_seconds`| `10.0` | Seconds above threshold before throttling starts. |
| `binary_name` | *optional* | Name of binary to push from `binaries/` (e.g. `ffmpeg`, `transcribe_helper`). |
| `require_binary` | `false` | When true, workers download the binary from the master first. |

---

## Dynamic CPU Throttling

When borrowing a friend's machine, their regular work shouldn't be interrupted by high CPU usage.

Workers monitor total background CPU usage (excluding their own task process). If non-task CPU usage exceeds `cpu_threshold_percent` for longer than `cpu_threshold_seconds`, the worker automatically degrades its own process priority (`nice` 5 up to 19). Once background activity drops back below the threshold, priority returns to normal.

Throttling defaults (`nice_initial = 5`, `nice_step = 5`, `nice_max = 19`, `nice_check_interval_seconds = 5.0`, `max_throttle_duration_seconds = 600.0`, and `unthrottle_threshold_seconds = 20.0`) are declared as global parameters at the top of `worker/worker.py`.

---

## Dashboards

### Terminal UI
Master runs an interactive ANSI terminal dashboard showing real-time progress, connected nodes, execution flags (Receiving / Executing / Sending), and cluster alerts:

```
=== MASTER DASHBOARD: [executing] ===
NODE IP         DEVICE         CONNECTED  PART FILE   STATE        FLAGS [R/E/S]  PROGRESS / RUN TIME
192.168.1.10    alex-laptop    ✓          part1.mkv   executing    [-/E/-]        54.2% [ETA: 01:12]
192.168.1.15    sam-desktop    ✓          part2.mkv   executing    [-/E/-]        62.8% [ETA: 00:54]
192.168.1.22    spare-node     ✓          -           idle         [-/-/-]        -
───────────────────────────────────────────────────────────────────────────────────────────────────
• Worker 192.168.1.15 finished uploading part2.mkv
• Worker 192.168.1.10 throttled to nice=10 (host active)
```

### Web Dashboard
Passing `--webserver` when starting `master.py` opens a browser dashboard at `http://localhost:8080`:
- Live progress bars and per-node states.
- Cluster event feed and message logs.
- Interactive configuration switcher.
- REST API endpoints (`/api/status`, `/api/input`).

---

## Project Structure

```
├── core/                                 # Shared foundation package
│   ├── __init__.py                       # Re-exports all public APIs for backwards compatibility
│   ├── constants.py                      # Network ports, status strings, schemas, and paths
│   ├── config.py                         # Config dataclass, INI auto-discovery, and parser
│   ├── devices.py                        # Device ID allocation and devices.json tracking
│   ├── binaries.py                       # Platform resolution, cache path builder, MD5 hashing
│   ├── messaging.py                      # TCP JSON protocol, authentication, PIN pairing
│   ├── discovery.py                      # mDNS auto-discovery and UDP broadcast beacons
│   ├── file_transfer.py                  # HTTP FileTransferDaemon & upload/download client
│   ├── benchmark.py                      # Hardware detection, benchmarks, affinity ranking
│   ├── execution.py                      # Process execution, CPU tracker, and nice throttling
│   ├── task_prep.py                      # Video/file chunk splitting, merging, and cleanup
│   └── dashboard.py                      # Terminal StatusDashboard UI and thread-safe logger
├── master/                               # Master orchestrator component
│   ├── __init__.py                       # Master package exports
│   ├── master.py                         # Main orchestrator script and lifecycle coordinator
│   ├── bench_and_sort.py                 # Worker ranking and active/spare partitioning
│   ├── failsafe.py                       # Failover recovery, spare reassignment, crash handling
│   ├── connection_pool.py                # WorkerConnectionPool and socket listener
│   ├── binary_distributor.py             # Pre-task binary checks and HTTP distribution
│   ├── task_distributor.py               # Part-file distribution and daemon spawning
│   ├── monitor.py                        # Execution monitoring and artifact collection
│   ├── cli.py                            # Master argument parsing and validation
│   ├── web_dashboard.py                  # Web dashboard server
│   ├── input/                            # Source input files and split chunks
│   └── output/                           # Finished merged output files
├── worker/                               # Worker execution component
│   ├── __init__.py                       # Worker package exports
│   ├── worker.py                         # Main worker loop and execution daemon
│   ├── cli.py                            # Worker argument parsing
│   ├── connection.py                     # Master connection, mDNS/UDP discovery, PIN pairing
│   ├── binary_manager.py                 # Binary cache verification and handshake
│   ├── task_fetcher.py                   # HTTP part-file downloading
│   ├── task_executor.py                  # Task runner, CPU throttling, result upload
│   ├── benchmark.c                       # Native C hardware benchmark source
│   ├── input/                            # Worker staging directory for chunks
│   └── output/                           # Processed results ready for upload
├── config/                               # Sample task configurations (.ini)
├── binaries/                             # Portable binaries (benchmark, ffmpeg, blender)
└── simulations_and_testing/              # Automated simulations and unit test suites
```

### Component Reference Guide

#### 1. Core Package (`core/`)
- **[`core/constants.py`](core/constants.py)**: Protocol constants, ports (5000 control, 5353 mDNS, 5005 UDP), progress labels, message types, and validation schemas (`MESSAGE_REQUIRED_KEYS`).
- **[`core/config.py`](core/config.py)**: Configuration dataclass (`Config`), INI file parser (`load_config`), and config file auto-discovery (`get_available_configs`).
- **[`core/devices.py`](core/devices.py)**: Active device mapping, `devices.json` persistence (`write_devices_json`), and hot-spare IP substitution (`swap_device_ip`).
- **[`core/binaries.py`](core/binaries.py)**: Target platform detection (`get_worker_platform`), cache path resolution (`resolve_worker_binary_cache_path`), and fast MD5 chunked hashing (`compute_md5`).
- **[`core/messaging.py`](core/messaging.py)**: Low-level socket JSON newline framing, HMAC secret verification, PIN challenge/response handlers, and typed control messages (`send_ready_message`, `send_finished_message`).
- **[`core/discovery.py`](core/discovery.py)**: Multi-threaded mDNS announcer & listener (`MdnsAnnouncer`, `discover_master_ip`), UDP broadcast beacon responder & listener (`discover_master_udp`).
- **[`core/file_transfer.py`](core/file_transfer.py)**: Standalone HTTP daemon (`FileTransferDaemon`), ephemeral port manager, and worker file client (`request_file_list`, `request_file`, `upload_result_file`).
- **[`core/benchmark.py`](core/benchmark.py)**: Hardware inspection (CPU brand, OpenCL GPU/VRAM, native battery status, total RAM), synthetic single/multi-core benchmarks (`benchmark_single_core`, `benchmark_multi_core`, `run_node_benchmark`), and affinity ranking (`rank_workers_by_affinity`).
- **[`core/execution.py`](core/execution.py)**: Bare-metal process execution runner (`run_execute_command`), real-time `ProcessCpuTracker`, stepped dynamic `nice` throttling, ffmpeg progress/ETA parser, and socket connection liveness watchdog.
- **[`core/task_prep.py`](core/task_prep.py)**: Task file splitting (`build_split_command`, `run_split_command`), part verification (`verify_part_files`), concat demuxer filelist generator (`write_filelist_txt`), output merging (`run_merge_command`), and cleanup routines.
- **[`core/dashboard.py`](core/dashboard.py)**: Terminal `StatusDashboard` UI, live progress indicators, interactive prompt system, and thread-safe file logging (`printlog`).

#### 2. Master Component (`master/`)
- **[`master/master.py`](master/master.py)**: Master orchestration entry point (`run_master`). Coordinates startup, config selection, worker discovery, binary check, video splitting, execution tracking, and final output merging.
- **[`master/bench_and_sort.py`](master/bench_and_sort.py)**: Worker ranking and partitioning (`split_active_and_spare_addresses`, `rank_and_partition_workers`) matching tasks to best hardware.
- **[`master/failsafe.py`](master/failsafe.py)**: Fault tolerance logic: hot-spare reassignment (`reassign_task_to_spare_node`) and no-spare recovery scenarios (`handle_no_spare_nodes_recovery`).
- **[`master/connection_pool.py`](master/connection_pool.py)**: Thread-safe `WorkerConnectionPool` managing dynamic worker socket accepts, PIN authentication challenges, and connection health.
- **[`master/binary_distributor.py`](master/binary_distributor.py)**: Phase 0 binary verification (`check_and_distribute_binaries`) ensuring workers have required binary dependencies before task dispatch.
- **[`master/task_distributor.py`](master/task_distributor.py)**: Task dispatch (`distribute_part_files_to_active_nodes`), launching per-node HTTP daemons and sending `ready` signals.
- **[`master/monitor.py`](master/monitor.py)**: Central event loop (`monitor_worker_executions_and_collect_results`) tracking execution progress, handling same-node retries, and verifying artifact uploads.
- **[`master/cli.py`](master/cli.py)**: Master CLI argument definitions and validation (`parse_master_arguments`, `validate_worker_address_count`).
- **[`master/web_dashboard.py`](master/web_dashboard.py)**: Optional browser-based dashboard server (`start_web_server`) exposing REST APIs and live web UI on port 8080.

#### 3. Worker Component (`worker/`)
- **[`worker/worker.py`](worker/worker.py)**: Worker lifecycle daemon (`run_worker`). Handles connection loops, hardware benchmarking, and continuous task execution.
- **[`worker/cli.py`](worker/cli.py)**: Worker CLI argument parsing (`parse_worker_arguments`).
- **[`worker/connection.py`](worker/connection.py)**: Auto-discovery via mDNS/UDP, TCP socket connection, and PIN/password handshake pairing (`connect_to_master`, `discover_master_address`, `perform_handshake_authentication`).
- **[`worker/binary_manager.py`](worker/binary_manager.py)**: Binary cache management, executable permission checks (`ensure_executable`), and binary transfer handshake (`do_binary_handshake`).
- **[`worker/task_fetcher.py`](worker/task_fetcher.py)**: HTTP download client fetching allocated input parts from the master daemon (`fetch_allocated_part_file`).
- **[`worker/task_executor.py`](worker/task_executor.py)**: Task execution loop (`run_worker_task`), CPU nice throttling integration, progress reporting, and multi-file artifact uploading.

*(For detailed architectural guidance, token-optimized file navigation, and debugging instructions for AI agents, see **[AGENTS.md](AGENTS.md)**).*

For compiling the benchmark tool and running tests, see **[BUILD.md](BUILD.md)**.
