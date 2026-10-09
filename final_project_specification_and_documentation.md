# LAN-Based Distributed Compute System — Definitive Final Specification

> **What this document is:** The single source of truth for what this project IS today, what it SHOULD BE at completion, and every gap between the two. Every unfinished item is specified with enough detail to implement 1-to-1 without ambiguity.
>
> **Target Audience:** The developer implementing these features (that's us), and the evaluator reading the project report (BE 4th year major project).

---

## 1. Project Identity & Design Philosophy

### 1.1 What This Project Is

A lightweight distributed task execution system for **small friend groups on local Wi-Fi** (3–10 laptops). One person (the Master) has a compute-heavy task (video encoding, 3D rendering, etc.). Friends lend their idle laptops. The system:

1. **Splits** the task into N chunks (one per worker)
2. **Benchmarks** each worker's hardware on first connection
3. **Ranks** workers by the metric relevant to the task (CPU single-core, multi-core, or GPU)
4. **Distributes** chunks to the best-matched workers
5. **Executes** bare-metal on each friend's machine
6. **Collects** results and **merges** the final output

### 1.2 Supported Platforms (v1)

| Platform | `platform.system()` | `platform.machine()` | Binary folder | Role |
|---|---|---|---|---|
| **Linux x86_64** | `"Linux"` | `"x86_64"` | `linux/` | Master or Worker |
| **macOS Apple Silicon** | `"Darwin"` | `"arm64"` | `macos/` | Worker (or Master) |

> [!NOTE]
> Windows support will be added after v1 works end-to-end on Linux + macOS. Linux aarch64 and macOS Intel are excluded from v1 scope.

### 1.3 Supported Networks

The system works identically on any flat-subnet local Wi-Fi:

| Network Type | Typical Subnet | Notes |
|---|---|---|
| TP-Link / Netgear / ASUS home routers | `192.168.0.x`, `192.168.1.x` | mDNS works natively |
| Android phone hotspot | `192.168.43.x` | mDNS broken; UDP broadcast needed |
| iPhone / iOS hotspot | `172.20.10.x` | mDNS broken; UDP broadcast needed |
| Windows laptop hotspot | `192.168.137.x` | mDNS broken; UDP broadcast needed |
| Linux `hostapd` / `nmcli` hotspot | various | mDNS usually works |

The only requirement: all devices are on the **same flat subnet** (no routing between VLANs).

### 1.4 Hard Design Constraints

| Constraint | Rationale |
|---|---|
| **No containers** | Bare-metal execution maximizes hardware access (Apple Metal/VideoToolbox, NVIDIA NVENC). Docker on macOS runs a Linux VM that caps RAM and blocks GPU. |
| **No keyboard/mouse monitoring** | Privacy-invasive. Friends trust us with their laptop; we trust them back. CPU load monitoring via `psutil` is sufficient and non-intrusive. |
| **Dynamic CPU Throttling (Mayday)** | Non-intrusive resource management. Instead of key/mouse hooks, the system monitors background CPU load via `psutil`/`procfs`. When a friend uses their laptop, worker process priority is dynamically degraded (nice 5 → 19 on POSIX, `BELOW_NORMAL` → `IDLE` on Windows) so the host stays responsive. |
| **No FastAPI / REST / WebSocket migration** | The current raw TCP + newline-delimited JSON protocol works, is debuggable with zero dependencies. Migrating to FastAPI adds `uvicorn`, `pydantic`, async complexity — unnecessary for <10 nodes. |
| **PIN-based pairing IS required** | This system enables remote code execution on friends' laptops. Without verifying that commands originate from the legitimate master, anyone on the same Wi-Fi could impersonate the master and execute arbitrary code. A 4-digit PIN proves physical proximity and consent. |
| **CLI-first** | Terminal `StatusDashboard` is the primary UI. No browser dashboard. |

### 1.5 Security Threat Model

This is a **known-friend, physically-present device lending** system. The PIN protects against:

- ✅ Strangers on the same Wi-Fi silently registering as workers
- ✅ A rogue device impersonating the master to execute arbitrary code on friends' laptops
- ✅ Replay of stale connections from devices that left the cluster

The PIN does NOT protect against:
- ❌ A compromised master pushing malicious commands (accepted risk — you own the master)
- ❌ Network traffic eavesdropping (accepted risk — plain HTTP on trusted LAN)

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      MASTER NODE (Linux)                    │
│                                                             │
│  ┌───────────┐  ┌──────────────┐  ┌───────────────────────┐ │
│  │ Splitter   │  │ Matchmaker   │  │ Merger                │ │
│  │ (ffmpeg/   │  │ (rank nodes  │  │ (ffmpeg concat/       │ │
│  │  custom)   │  │  by affinity)│  │  custom cmd)          │ │
│  └─────┬─────┘  └──────┬───────┘  └───────────────────────┘ │
│        │               │                                    │
│  ┌─────┴───────────────┴────────────────────────────────┐   │
│  │           WorkerConnectionPool                        │   │
│  │  TCP :5000 — dynamic accept, PIN pairing,             │   │
│  │  failover, kill/retry, telemetry storage              │   │
│  └───────────────────────┬──────────────────────────────┘   │
│  ┌───────────────────────┴──────────────────────────────┐   │
│  │      FileTransferDaemon (per-node ephemeral HTTP)     │   │
│  │  GET /listfiles, GET /file/<name>, POST /file/<name>  │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  StatusDashboard (live terminal UI)                    │   │
│  │  PIN display, node table, progress, messages          │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Discovery: mDNS announcer + UDP broadcast listener   │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
         │ TCP :5000              │ HTTP (ephemeral port)
         │ JSON messages          │ file chunks
         ▼                        ▼
┌─────────────────────────────────────────────────────────────┐
│              WORKER NODE (Linux or macOS) ×N                │
│                                                             │
│  1. Discover master (mDNS multicast → UDP broadcast → CLI)  │
│  2. TCP connect :5000                                       │
│  3. PIN pairing: receive challenge, user types PIN, verify  │
│  4. Run local hardware benchmark (~2s)                      │
│  5. Send benchmark scores + system info to master           │
│  6. TASK LOOP:                                              │
│     a. Receive "ready" msg (includes execute_command)       │
│     b. Download chunk via HTTP GET                          │
│     c. Execute bare-metal command                           │
│     d. Upload result via HTTP POST                          │
│     e. Send "finished" message                              │
│     f. Return to IDLE, wait for next task                   │
│  7. Background: monitor CPU load, adjust nice priority      │
│  8. Background: monitor power state (battery/AC)            │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Exhaustive Component Audit

### Legend
| Symbol | Meaning |
|---|---|
| ✅ **DONE** | Implemented, tested, working |
| 🟡 **PARTIAL** | Core logic exists but has specific gaps |
| ❌ **NOT DONE** | Not implemented at all |
| 🔧 **BUG** | Implemented but has a correctness issue |

---

### 3.1 Task Splitting Engine — ✅ DONE (95%)

**Files:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py) lines 1842–1960

**Functions:**
- `resolve_input_video()` — finds input video from CLI arg, default path, split_command regex, or directory scan
- `get_video_duration_seconds()` — probes duration via `ffprobe`
- `build_split_command()` — computes equal segment timestamps `duration × i / N` and substitutes `{segment_times}`, `{input_directory}` placeholders
- `run_split_command()` — runs ffmpeg split via subprocess; if only 1 node, copies input directly
- `verify_part_files()` — confirms all expected `partX.ext` files exist on disk
- `get_part_extension()` — infers extension (`.mkv`, `.mp4`, `.webm`) from split command regex or existing files
- `part_filename_for_node()` — generates `part1.mkv`, `part2.mkv`, etc.

**Status:** Fully functional. No changes needed.

---

### 3.2 Task Execution Engine — ✅ DONE (85%)

**Files:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py) lines 1252–1680

**Functions:**
- `build_execute_command()` — substitutes `{input}`, `{output}`, `{output_directory}`, `{input_directory}` placeholders
- `run_execute_command()` — runs subprocess with:
  - Real-time ffmpeg stderr parsing for progress %, ETA, duration
  - `ProcessCpuTracker` for background CPU monitoring
  - Stepped nice throttling (`nice_initial` → `+nice_step` → `nice_max`) or kill policy
  - Auto-recovery when CPU load drops
  - User keyboard abort (`k` key via `termios` cbreak mode)
  - Master disconnect detection via `check_socket_connection_lost()`
  - Simulated failure injection for testing

**Classes:**
- `ProcessCpuTracker` ([`core.py`](file:///home/anishudupan/claudecode/project/core.py) line 1390) — reads `/proc/stat` and `/proc/<pid>/stat` on Linux; falls back to `psutil` on macOS

🔧 **Bug: `set_process_nice()` on macOS**

[`core.py`](file:///home/anishudupan/claudecode/project/core.py) line 1462:
```python
def set_process_nice(pid: int, nice_value: int) -> bool:
    try:
        os.setpriority(os.PRIO_PROCESS, pid, nice_value)
```
On macOS, `os.setpriority()` exists but raising niceness above the current value requires `sudo`. When running as a normal user, this silently fails and falls back to psutil. The fallback works, but `psutil` must be installed.

**Fix needed:** Ensure `psutil` is a declared required dependency.

---

### 3.3 Task Merging Engine — ✅ DONE (95%)

**Files:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py) lines 1960–2050

**Functions:**
- `generate_filelist_text()` — generates `file '<abs_path>'` entries for ffmpeg concat demuxer
- `write_filelist_txt()` — writes to both `master/output/filelist.txt` AND `./filelist.txt`
- `build_merge_command()` — substitutes `{output_directory}` and `{filelist}`
- `run_merge_command()` — runs ffmpeg concat merge, verifies `output.mkv` exists

**Status:** Fully functional. No changes needed.

---

### 3.4 Worker Connection Pool — ✅ DONE (90%)

**Files:** [`master.py`](file:///home/anishudupan/claudecode/project/master.py) lines 61–268

**Class: `WorkerConnectionPool`**

**Attributes:**
- `connections: dict[str, socket.socket]` — keyed by IP or `ip:port` for multi-worker-same-IP
- `killed_ips: set[str]` — permanently blacklisted after failover
- `lock: threading.Lock` — thread-safe access
- `_shared_secret: str | None` — for authentication

**Methods:**
- `_accept_loop()` — background thread: `select()` on listening socket with 0.5s timeout, validates IP allowlist, authenticates, prunes dead connections via `MSG_PEEK`, handles multi-worker on same IP
- `get_connection(key, timeout_seconds=30)` — polls for specific worker connection
- `remove_connection(key)` — closes socket, updates dashboard to "disconnected"
- `mark_killed(key)` — permanently blacklists IP, closes socket, updates dashboard to "killed"
- `shutdown_all_workers()` — broadcasts shutdown message to all connected workers
- `stop()` — sets `running = False` to exit accept loop

**Status:** Fully functional.

🟡 **Gap:** Pool does not store worker telemetry (benchmark scores, hardware info). Currently workers are just sockets — no metadata attached. The matchmaker will need `worker_telemetry: dict[str, dict]` storage added here.

---

### 3.5 File Transfer Daemon — ✅ DONE (90%)

**Files:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py) lines 1002–1190

**Class: `FileTransferDaemon`**
- Spawns a separate Python process running `core.py --serve-directory <dir> --files <file1> <file2> ...`
- Binds `HTTPServer(("0.0.0.0", 0))` to OS-assigned ephemeral port
- Prints `LISTENING <port>` to stdout (parent reads this via `read_daemon_port()`)
- Parent watchdog thread: if master process dies, daemon auto-terminates
- `atexit` cleanup via global `_active_daemons_registry`

**Class: `FileTransferRequestHandler`** (inside `_make_handler_class()`)
- `GET /listfiles` → JSON `{"files": [...]}`
- `GET /file/<name>` → streams binary bytes (only if filename is in allocated list)
- `POST /file/<name>` → receives upload, `os.path.basename()` sanitization prevents directory traversal

**Status:** Fully functional.

---

### 3.6 Failover & Recovery — 🟡 PARTIAL (60%)

**Files:** [`master.py`](file:///home/anishudupan/claudecode/project/master.py) lines 420–1077

**Current Mechanisms:**
1. **Same-node retry** (1 attempt): waits 8s for same worker to reconnect, re-sends same chunk
2. **Spare node failover**: `spare_nodes.pop(0)`, kills failed worker, swaps IP in `devices.json`, transfers chunk to spare
3. **Interactive recovery** (no spares available): Manual prompts (`[k]`, `[w]`, `[r]`, `[b]`).

**Problem:** Manual interaction halts the pipeline, and spare nodes remain idle instead of contributing.
❌ **Missing: Automated Dynamic Task Queue & Recovery.** 
By default, failover should be fully automated using a Dynamic Task Queue. If the `--manual` flag is provided when launching the master, the system will fall back to the Interactive Recovery prompts. Specified in detail in §4.9.

---

### 3.7 Terminal Status Dashboard — ✅ DONE (85%)

**Files:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py) lines 2073–2304

**Class: `StatusDashboard`**

**What it renders:**
```
=== MASTER DASHBOARD: [executing] ===
NODE IP         DEVICE   CONNECTED  PART FILE   STATE        FLAGS [R/E/S]  PROGRESS / RUN TIME
192.168.1.10    node1    ✓          part1.mkv   executing    [-/E/-]        45.2% [ETA: 02:15]
192.168.1.11    node2    ✓          part2.mkv   executing    [-/E/-]        38.7% [ETA: 03:01]
192.168.1.12    spare    ✓          -           idle         [-/-/-]        -
───────────────────────────────────────────────────────────────────────────────────────────────
• Worker 192.168.1.10 connected (8s left)
• File distribution complete for 2 nodes (5s left)
```

**Features:**
- ANSI escape clearing (`\033[2J\033[H`) — works cross-platform without `clear`/`cls`
- Interactive config selection during startup (toggle with `c`/numbers, confirm with `y`)
- Per-node R/E/S flags (Receiving/Executing/Sending)
- Timed message banners with auto-countdown and expiry
- All frames mirrored to `logs.txt` via `printlog()`

🟡 **Gap:** Dashboard does not display PIN challenge prompts yet. Will need a "Worker X wants to join. PIN: 7042" message display.

---

### 3.8 Binary Distribution — ✅ DONE (80%)

**Files:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py) lines 143–290

**What exists:**
- `BINARY_PLATFORM_MAP`: maps `(platform.system(), platform.machine())` to `(os_folder, arch_folder)`
- `get_worker_platform()`: detects worker's OS/arch
- `get_binary_path()`: locates binary on master in `binaries/<binary_name>/<os_folder>/`
- `resolve_worker_binary_cache_path()`: local cache at `worker/binaries/<binary_name>/<os_folder>/`
- `compute_md5()`: checksum for staleness detection
- Full binary handshake protocol: `binary_info_request` → `binary_info` → file transfer → `binary_ready`

**Pre-compiled binaries on disk:**
```
binaries/
├── ffmpeg/
│   ├── linux/ffmpeg        (76 MB ELF)
│   └── macos/ffmpeg        (43 MB Mach-O)
└── blender/
    ├── linux/blender       (166 MB ELF + libs)
    └── macos/MacOS/...     (app bundle)
```

🔧 **Bug: Duplicate entry in `BINARY_PLATFORM_MAP`**

[`core.py`](file:///home/anishudupan/claudecode/project/core.py) line 149–153:
```python
BINARY_PLATFORM_MAP = {
    ("Linux",  "x86_64"):  ("linux", "x64"),
    ("Darwin", "arm64"):   ("macos", "aarch64"),
    ("Darwin", "aarch64"): ("macos", "aarch64"),  # ← DEAD CODE: Python never returns "aarch64" on macOS
}
```
Python on macOS Apple Silicon reports `platform.machine()` as `"arm64"`, never `"aarch64"`. The third entry is dead code.

**Fix:** Replace with exactly what Python returns:
```python
BINARY_PLATFORM_MAP = {
    ("Linux",  "x86_64"): ("linux", "x64"),
    ("Darwin", "arm64"):  ("macos", "arm64"),
}
```
Also rename the folder from `macos/aarch64` to just `macos/` (flat) or `macos/arm64` — since we only support one arch per OS, the arch subfolder is redundant.

---

### 3.9 OS-Specific Command Overrides — 🔧 IMPLEMENTED BUT NEVER CALLED

**Files:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py) lines 294–370

The `Config` dataclass has:
- Fields: `execute_command_windows`, `execute_command_macos`, `execute_command_linux` (and same for split/merge)
- Methods: `get_execute_command(os_type)`, `get_split_command(os_type)`, `get_merge_command(os_type)` — auto-detect platform and return the OS-specific command, falling back to the default

🔧 **Bug 1: `worker.py` line 380 uses `config.execute_command` directly**

```python
run_worker_task(connection, master_ip_address, arguments.download_directory,
                config.execute_command,  # ← bypasses OS-specific override
                ...)
```

**Fix:** This will be superseded by master-transmitted commands (§3.13). The worker will receive the resolved command from the master, not read it from local config.

🔧 **Bug 2: `master.py` uses `config.split_command` and `config.merge_command` directly**

Lines 1503, 1548: `config.split_command` and `config.merge_command` are used raw.

**Fix:** Change to `config.get_split_command()` and `config.get_merge_command()`. The master runs split/merge locally, so it should use its own platform's override.

---

### 3.10 Network Discovery — 🟡 PARTIAL (70%)

**Files:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py) lines 540–780

**What exists (mDNS):**
- `MdnsAnnouncer` — background thread sending DNS-SD A+PTR records to multicast `224.0.0.251:5353`, broadcast `255.255.255.255:5353`, and loopback `127.0.0.1:5353`
- `discover_master_ip()` — listens on multicast group for master announcements, extracts IP from A-record
- Cross-platform multicast socket handling (`SO_REUSEPORT` try/except for macOS vs Linux)

**Problem:** mDNS **does not work on most mobile hotspots** (Android, iOS, Windows). Multicast packets from one client don't reach other clients because the hotspot doesn't act as an mDNS reflector.

❌ **Missing: UDP Broadcast Discovery (port 5005)**

A simple, parallel discovery method that works on ALL network types. Specified in detail in §4.2.

---

### 3.11 Authentication — 🟡 PARTIAL (60%)

**What exists:**
- `DIST_SHARED_SECRET` environment variable ([`core.py`](file:///home/anishudupan/claudecode/project/core.py) line 103)
- `send_auth_message()` / `verify_auth_message()` — worker sends pre-shared secret string, master verifies

**Problem:** Pre-shared secrets require coordinating the same environment variable across all machines. This is friction ("hey, type `export DIST_SHARED_SECRET=mysecret` on your terminal").

❌ **Missing: PIN-Based Pairing**

A 4-digit PIN displayed on the master's screen, typed by the friend on their worker terminal. Proves physical proximity without pre-coordination. Specified in detail in §4.1.

---

### 3.12 Hardware Benchmarking — ❌ NOT DONE (0%)

No benchmarking code exists anywhere in the codebase. Workers connect and are assigned tasks without any knowledge of their hardware capability.

Specified in detail in §4.3.

---

### 3.13 Resource-Affinity Matchmaker — ❌ NOT DONE (0%)

Worker assignment is purely positional: `active_nodes = worker_ip_addresses[:max_nodes]`. Node 1 always gets Chunk 1 regardless of whether it's a 4-core i3 or a 16-core Ryzen 9.

Specified in detail in §4.4.

---

### 3.14 Power State Awareness — ❌ NOT DONE (0%)

No battery/AC detection exists. A friend's laptop on 5% battery receives the same workload as a plugged-in desktop.

Specified in detail in §4.5.

---

### 3.15 Master-to-Worker Command Transmission — ❌ NOT DONE

Currently the worker reads `execute_command` from its OWN local config file (line 380 of [`worker.py`](file:///home/anishudupan/claudecode/project/worker.py)). This means:
- Every worker machine must have a copy of the same config file
- If the master changes the config, workers don't know
- OS-specific command resolution can't happen on the master side

Specified in detail in §4.6.

---

### 3.16 Dependency Management — ❌ NOT DONE

No `requirements.txt` exists. `psutil` is silently optional, causing CPU monitoring to fail on macOS.

---

### 3.17 Resource Throttling & System Watchdog Engine (Mayday) — 🟡 PARTIAL (80%)

**Files:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py) lines 1390–1480, 1528–1646; `config/*.ini` (`mayday.ini`, `sw1.ini`, `sw2.ini`, etc.)

**Functions & Classes:**
- `ProcessCpuTracker` — samples `/proc/stat` & `/proc/<pid>/stat` (Linux) or `psutil` (macOS/Windows) to isolate background non-task CPU usage (`bg_pct`) from worker task CPU usage (`proc_pct`).
- `set_process_nice(pid, nice_value)` — dynamic OS process priority adjustment (`os.setpriority` / `psutil`).
- `run_execute_command()` polling loop — samples CPU load every 1.0s and executes throttling policy.

**Throttling & Recovery Workflow:**
1. **Trigger:** If background CPU usage `bg_pct > cpu_threshold_percent` (e.g. 70%) for `> cpu_threshold_seconds` (e.g. 10s):
   - If `on_high_usage == "kill"`: terminates task immediately (`force_stop_process()`).
   - If `on_high_usage == "throttle"`: sets `is_throttled = True`, degrades process priority to `nice_initial` (e.g. 5).
2. **Stepped Escalation:** If background CPU usage continues to rise over subsequent `nice_check_interval_seconds` (5s), niceness steps up by `nice_step` (+5) up to `nice_max` (19).
3. **Timeout Safeguard:** If throttled for `> max_throttle_duration_seconds` (e.g. 600s) and `long_runnable` is `False`, worker kills the task to prevent infinite resource starvation.
4. **Auto-Recovery (Unthrottling):** When background CPU usage stays below threshold for `> unthrottle_threshold_seconds` (e.g. 20s), priority is restored to default (nice 0 / Normal Priority), resetting `is_throttled = False`.
5. **Dashboard Status Telemetry:** Dashboard displays `[throttled nice=X]` in worker state during throttling.

🔧 **Configuration Bug (`mayday.ini` & `sw*.ini`):**
In existing `.ini` configs, `on_high_usage = throttle` is set, but `nice_initial = 0`, `nice_step = 0`, `nice_max = 0`. Throttling logic triggers and prints logs, but niceness remains at priority 0. Fixed in Task 1.4 (`nice_initial=5`, `nice_step=5`, `nice_max=19`).

---

## 4. Detailed Specification of Every Unfinished Feature

### 4.1 PIN-Based Pairing (TCP, No FastAPI)

**Why this matters:** This system executes arbitrary shell commands on friends' laptops. Without verifying the master's identity, any device on the same Wi-Fi could send `rm -rf /` to a worker. A 4-digit PIN proves the worker operator is physically present with the master operator and consents to join.

**Authentication Modes:**

| Mode | When Used | How It Works |
|---|---|---|
| **PIN pairing** | `DIST_SHARED_SECRET` env var is NOT set (default) | Interactive 4-digit PIN on master dashboard |
| **Pre-shared secret** | `DIST_SHARED_SECRET` env var IS set | Automatic, no user interaction (for dev/testing) |

**PIN Pairing Flow:**

```
Worker                                          Master
  │                                                │
  │──── TCP connect to :5000 ─────────────────────▶│
  │                                                │
  │                                     Master generates random 4-digit PIN
  │                                     (rejects 0000, 1111, 1234, 4321, 9876)
  │                                     Dashboard shows:
  │                                     "Worker 192.168.1.10 wants to join. PIN: 7042"
  │                                                │
  │◀─── {"type":"pin_challenge",       ◄───────────│
  │      "master_name":"anish-laptop"}             │
  │                                                │
  │  Worker terminal prompts:                      │
  │  "Master 'anish-laptop' requests pairing.      │
  │   Enter PIN shown on master: "                 │
  │  User types: 7042                              │
  │                                                │
  │──── {"type":"pin_response",        ───────────▶│
  │      "pin":"7042"}                             │
  │                                                │
  │                                     Master verifies PIN
  │                                                │
  │◀─── {"type":"pin_accepted"}        ◄───────────│  (success)
  │                                                │
  │  ──── proceeds to binary handshake ──────────▶ │
  │  ──── proceeds to benchmark ─────────────────▶ │
  │  ──── proceeds to task loop ─────────────────▶ │
```

**Failure handling:**
- Wrong PIN: master sends `{"type": "pin_rejected", "reason": "wrong_pin", "attempts_left": 2}`
- Max 3 attempts per connection. After 3 failures, master closes the socket.
- Worker can reconnect and try again (new PIN is generated each time).

**Session persistence:**
- Once paired, the master stores `(worker_ip, paired_at_timestamp)` in memory.
- If the same worker IP reconnects (e.g., after a network glitch), the master skips PIN pairing for the duration of the current master session.
- Pairing state is NOT persisted to disk — every time `master.py` restarts, all workers must re-pair.

**New message types to add to `MESSAGE_REQUIRED_KEYS`:**

```python
MESSAGE_TYPE_PIN_CHALLENGE = "pin_challenge"
MESSAGE_TYPE_PIN_RESPONSE = "pin_response"
MESSAGE_TYPE_PIN_ACCEPTED = "pin_accepted"
MESSAGE_TYPE_PIN_REJECTED = "pin_rejected"

MESSAGE_REQUIRED_KEYS.update({
    MESSAGE_TYPE_PIN_CHALLENGE: ['type', 'master_name'],
    MESSAGE_TYPE_PIN_RESPONSE: ['type', 'pin'],
    MESSAGE_TYPE_PIN_ACCEPTED: ['type'],
    MESSAGE_TYPE_PIN_REJECTED: ['type', 'reason', 'attempts_left'],
})
```

**Implementation locations:**
- `core.py`: Add message type constants, `generate_pin()`, `send_pin_challenge()`, `receive_pin_challenge()`, `send_pin_response()`, `receive_pin_response()`, `send_pin_accepted()`, `send_pin_rejected()`
- `master.py` → `WorkerConnectionPool._accept_loop()`: After TCP accept, before storing connection — run PIN challenge flow. Display PIN on dashboard.
- `worker.py` → after `connect_to_master()`: Receive `pin_challenge`, prompt user for PIN via `input()`, send `pin_response`, wait for `pin_accepted`/`pin_rejected`.

---

### 4.2 UDP Broadcast Discovery (Port 5005)

**Why this matters:** mDNS multicast doesn't work on mobile hotspots. UDP broadcast to `255.255.255.255` works on any flat subnet because it's handled at the link layer.

**Run alongside existing mDNS**, not replacing it. Both discovery methods operate in parallel.

**Master side — new background thread:**

```python
# Constants
UDP_DISCOVERY_PORT = 5005
DISCOVERY_BEACON_TYPE = "DISCOVERY_BEACON"
MASTER_OFFER_TYPE = "MASTER_OFFER"

def start_udp_discovery_listener(master_ip: str, control_port: int = FIXED_PORT) -> threading.Thread:
    """Background thread: listen for worker UDP beacons on port 5005, reply with master location."""
    def _listen():
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except (AttributeError, OSError):
            pass
        sock.bind(("0.0.0.0", UDP_DISCOVERY_PORT))
        sock.settimeout(1.0)
        
        while True:
            try:
                data, addr = sock.recvfrom(4096)
                msg = json.loads(data.decode())
                if msg.get("type") == DISCOVERY_BEACON_TYPE:
                    reply = json.dumps({
                        "type": MASTER_OFFER_TYPE,
                        "master_ip": master_ip,
                        "control_port": control_port,
                    }).encode()
                    sock.sendto(reply, addr)
            except socket.timeout:
                continue
            except (json.JSONDecodeError, UnicodeDecodeError, OSError):
                continue
    
    t = threading.Thread(target=_listen, daemon=True)
    t.start()
    return t
```

**Worker side — fallback when mDNS times out:**

```python
def discover_master_udp(timeout_seconds: float = 30.0) -> tuple[str, int]:
    """Broadcast UDP beacon every 2s, wait for master reply."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(2.0)
    
    local_ip = get_local_ip()
    beacon = json.dumps({
        "type": DISCOVERY_BEACON_TYPE,
        "worker_ip": local_ip,
    }).encode()
    
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        # Broadcast to both generic broadcast and common subnet broadcasts
        for target in ["255.255.255.255"]:
            try:
                sock.sendto(beacon, (target, UDP_DISCOVERY_PORT))
            except OSError:
                pass
        try:
            data, addr = sock.recvfrom(4096)
            msg = json.loads(data.decode())
            if msg.get("type") == MASTER_OFFER_TYPE:
                sock.close()
                return msg["master_ip"], msg.get("control_port", FIXED_PORT)
        except socket.timeout:
            continue
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
    
    sock.close()
    raise TimeoutError(f"No master found via UDP broadcast within {timeout_seconds}s")
```

**Worker discovery flow (updated):**
```python
# In worker.py, if master_ip_address is not specified:
print("Auto-discovering master...")
try:
    # Try mDNS first (works on home routers)
    master_ip_address = discover_master_ip(timeout_seconds=10.0)
except TimeoutError:
    print("mDNS discovery timed out. Trying UDP broadcast...")
    try:
        # Fallback to UDP broadcast (works on mobile hotspots)
        master_ip_address, _ = discover_master_udp(timeout_seconds=30.0)
    except TimeoutError:
        print("Error: Could not discover master on the network.", file=sys.stderr)
        sys.exit(1)
```

---

### 4.3 Hardware Benchmarking Suite

**Where:** New functions in [`core.py`](file:///home/anishudupan/claudecode/project/core.py). Called from [`worker.py`](file:///home/anishudupan/claudecode/project/worker.py) at startup.

**When:** After PIN pairing succeeds, before entering the task loop. Takes ~2 seconds total.

#### A. Single-Core CPU Score

Measures **single-thread execution speed**. Critical for single-threaded tasks (Node.js, Python scripts, sequential data processing).

```python
def benchmark_single_core(duration_seconds: float = 0.5) -> float:
    """Tight single-threaded SHA-256 hashing loop.
    
    Returns: operations per second (higher = faster single-core).
    
    Why SHA-256: It's a pure CPU-bound operation available in Python's stdlib
    (hashlib). No disk I/O, no memory allocation variance. Consistent across
    platforms. ~1KB input block ensures the entire operation fits in L1 cache.
    """
    import hashlib
    data = b"benchmark_payload_block_" * 43  # ~1KB block (43 * 24 = 1032 bytes)
    count = 0
    start = time.time()
    while time.time() - start < duration_seconds:
        hashlib.sha256(data).digest()
        count += 1
    elapsed = time.time() - start
    return count / elapsed if elapsed > 0 else 0.0
```

#### B. Multi-Core CPU Score

Measures **total parallel throughput across all CPU cores**. Critical for multi-threaded tasks (FFmpeg libx264, compilation, parallel compression).

```python
def benchmark_multi_core(duration_seconds: float = 1.0) -> float:
    """Spawn os.cpu_count() worker processes, each running single-core bench.
    
    Returns: total aggregate ops/second across all cores (higher = more parallel power).
    
    Why multiprocessing.Pool: Measures actual OS-level process parallelism,
    not just thread count. GIL doesn't affect this because each process has
    its own Python interpreter. Each worker runs for duration/num_cores seconds.
    """
    from multiprocessing import Pool
    num_cores = os.cpu_count() or 1
    per_core_duration = max(0.2, duration_seconds / 2)  # each core runs for half total time
    
    with Pool(num_cores) as pool:
        scores = pool.starmap(benchmark_single_core, [(per_core_duration,)] * num_cores)
    
    return sum(scores)
```

#### C. GPU Metadata Detection

GPU compute benchmarking via kernels requires CUDA/Metal SDKs — too heavy. Instead, collect **metadata** the matchmaker can use for routing decisions.

```python
def detect_gpu_info() -> dict:
    """Detect GPU name, VRAM, and hardware encoder availability.
    
    Returns dict with keys: gpu_name (str), vram_mb (int), hw_encoders (list[str]).
    
    Detection methods by platform:
    - Linux: nvidia-smi (NVIDIA GPUs)
    - macOS: system_profiler SPDisplaysDataType (Apple Silicon GPU)
    - Both: ffmpeg -encoders (available hardware encoders)
    """
    info = {"gpu_name": "", "vram_mb": 0, "hw_encoders": []}
    
    # NVIDIA (Linux): query via nvidia-smi
    if platform.system() == "Linux":
        try:
            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=name,memory.total",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5
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
    
    # macOS: query via system_profiler
    if platform.system() == "Darwin":
        try:
            result = subprocess.run(
                ["system_profiler", "SPDisplaysDataType", "-json"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                data = json.loads(result.stdout)
                displays = data.get("SPDisplaysDataType", [])
                if displays:
                    gpu = displays[0]
                    info["gpu_name"] = gpu.get("sppci_model", "Apple Silicon GPU")
                    # Apple Silicon uses unified memory — GPU shares system RAM
                    # Report 0 for dedicated VRAM; matchmaker uses hw_encoders instead
                    info["hw_encoders"].extend(["h264_videotoolbox", "hevc_videotoolbox"])
        except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError):
            pass
    
    # Cross-platform: check ffmpeg for available hardware encoders
    try:
        result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            known_hw_encoders = [
                "h264_nvenc", "hevc_nvenc",                  # NVIDIA
                "h264_videotoolbox", "hevc_videotoolbox",    # macOS
                "h264_qsv", "hevc_qsv",                     # Intel QuickSync
                "h264_vaapi", "hevc_vaapi",                  # Linux VAAPI
            ]
            for enc in known_hw_encoders:
                if enc in result.stdout and enc not in info["hw_encoders"]:
                    info["hw_encoders"].append(enc)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    
    return info
```

#### D. Power State Detection

```python
def detect_power_state() -> dict:
    """Detect AC/battery status. No keyboard/mouse monitoring.
    
    Uses psutil.sensors_battery() which works on Linux and macOS without
    any special permissions or accessibility hooks.
    
    Returns dict: is_plugged_in (bool), battery_pct (int 0-100), has_battery (bool).
    """
    info = {"is_plugged_in": True, "battery_pct": 100, "has_battery": False}
    try:
        import psutil
        battery = psutil.sensors_battery()
        if battery is not None:
            info["has_battery"] = True
            info["is_plugged_in"] = bool(battery.power_plugged)
            info["battery_pct"] = int(battery.percent)
    except (ImportError, AttributeError, RuntimeError):
        pass  # Desktop without battery, or psutil not installed — assume plugged in
    return info
```

#### E. Combined Benchmark Runner

```python
def run_node_benchmark() -> dict:
    """Load or run all benchmarks and collect system info.
    
    Checks config/benchmarks.json. If absent, runs a dual-profile (plugged/unplugged)
    benchmark interactively. Then appends real-time current telemetry.
    """
    import json, os
    benchmark_file = "config/benchmarks.json"
    
    if os.path.exists(benchmark_file):
        print("Loaded cached hardware benchmark profiles.")
        with open(benchmark_file, "r") as f:
            profiles = json.load(f)
    else:
        print("Running first-time hardware benchmark (requires manual charger toggle)...")
        # Ensure plugged in first
        power = detect_power_state()
        while not power.get("is_plugged_in", True):
            input("Please PLUG IN your charger, then press Enter...")
            power = detect_power_state()
        
        plugged_profile = {
            "single_core_score": round(benchmark_single_core(0.5), 1),
            "multi_core_score": round(benchmark_multi_core(1.0), 1),
            "gpu": detect_gpu_info()
        }
        
        # Ensure unplugged second
        if power.get("has_battery", False):
            while detect_power_state().get("is_plugged_in", True):
                input("Please UNPLUG your charger to bench battery performance, then press Enter...")
                
            unplugged_profile = {
                "single_core_score": round(benchmark_single_core(0.5), 1),
                "multi_core_score": round(benchmark_multi_core(1.0), 1),
                "gpu": detect_gpu_info()
            }
            print("Benchmarks complete! You may plug the charger back in.")
        else:
            # Desktop without battery
            unplugged_profile = plugged_profile
            
        profiles = {"plugged": plugged_profile, "unplugged": unplugged_profile}
        os.makedirs("config", exist_ok=True)
        with open(benchmark_file, "w") as f:
            json.dump(profiles, f, indent=4)
            
    # Append current live telemetry
    power = detect_power_state()
    current_profile = profiles["plugged"] if power.get("is_plugged_in", True) else profiles["unplugged"]
    gpu = current_profile["gpu"]
    
    result = {
        "single_core_score": current_profile["single_core_score"],
        "multi_core_score": current_profile["multi_core_score"],
        "gpu_name": gpu["gpu_name"],
        "vram_mb": gpu["vram_mb"],
        "gpu_shared_ram_mb": gpu.get("gpu_shared_ram_mb", 0),
        "hw_encoders": gpu["hw_encoders"],
        "os_type": platform.system().lower(),
        "cpu_count": os.cpu_count() or 1,
        "total_ram_mb": _get_total_ram_mb(),
        "is_plugged_in": power.get("is_plugged_in", True),
        "battery_pct": power.get("battery_pct", 100),
        "has_battery": power.get("has_battery", False),
        "current_system_cpu_usage": _get_current_system_cpu_usage(),
        "current_system_gpu_usage": _get_current_system_gpu_usage(),
        "current_system_ram_usage": _get_current_system_ram_usage(),
    }
    
    print(f"  Single-core: {result['single_core_score']:.0f} ops/s")
    print(f"  Multi-core:  {result['multi_core_score']:.0f} ops/s ({result['cpu_count']} cores)")
    if result["gpu_name"]:
        print(f"  GPU: {result['gpu_name']} ({result['vram_mb']} MB VRAM)")
    print(f"  RAM: {result['total_ram_mb']} MB | Power: {'AC' if result['is_plugged_in'] else 'Battery'} ({result['battery_pct']}%)")
    
    return result

def _get_current_system_cpu_usage() -> float:
    try:
        import psutil
        return psutil.cpu_percent(interval=0.5)
    except:
        return 0.0

def _get_current_system_ram_usage() -> float:
    try:
        import psutil
        return psutil.virtual_memory().percent
    except:
        return 0.0

def _get_current_system_gpu_usage() -> float:
    import platform
    if platform.system() == "Linux":
        try:
            import subprocess
            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=utilization.gpu", "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=2
            )
            if result.returncode == 0:
                return float(result.stdout.strip().split('\n')[0])
        except:
            pass
    return 0.0


def _get_total_ram_mb() -> int:
    """Get total system RAM in MB."""
    try:
        import psutil
        return psutil.virtual_memory().total // (1024 * 1024)
    except ImportError:
        # Fallback for Linux: read /proc/meminfo
        try:
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        return int(line.split()[1]) // 1024  # kB -> MB
        except (FileNotFoundError, ValueError):
            pass
    return 0
```

#### F. New Message Type

```python
MESSAGE_TYPE_WORKER_TELEMETRY = "worker_telemetry"

MESSAGE_REQUIRED_KEYS[MESSAGE_TYPE_WORKER_TELEMETRY] = [
    'type', 'single_core_score', 'multi_core_score', 'os_type', 'cpu_count',
]
```

Worker sends this immediately after PIN pairing:
```python
telemetry = run_node_benchmark()
send_json_message(connection, {"type": "worker_telemetry", **telemetry})
```

Master receives and stores it in `WorkerConnectionPool`:
```python
# In _accept_loop or a new method:
telemetry_msg = receive_json_message(conn)
validate_message(telemetry_msg)
self.worker_telemetry[node_key] = telemetry_msg
```

---

### 4.4 Resource-Affinity Matchmaker

**Where:** New function in [`core.py`](file:///home/anishudupan/claudecode/project/core.py) or [`master.py`](file:///home/anishudupan/claudecode/project/master.py). Called from `run_master()` after all workers have connected and sent telemetry.

#### A. Config Field: `resource_type`

Add to `Config` dataclass and `load_config()`:

```python
@dataclass(frozen=True)
class Config:
    # ... existing fields ...
    resource_type: str = "auto"    # single_core | multi_core | gpu | auto
    min_vram_mb: int = 0           # minimum VRAM for gpu tasks (hard filter)
```

#### B. Config `.ini` usage:

```ini
[DEFAULT]
# What hardware resource does this task depend on?
resource_type = multi_core

# Example: a Node.js task
# resource_type = single_core

# Example: a Blender GPU render
# resource_type = gpu
# min_vram_mb = 2048
```

#### C. Ranking Function

```python
def rank_workers_by_affinity(
    worker_telemetry: dict[str, dict],  # {ip: telemetry_dict}
    resource_type: str,
    min_vram_mb: int = 0,
) -> list[str]:
    """Sort worker IPs by the benchmark metric relevant to the task.
    
    Returns ordered list of IPs, best-suited first.
    
    Ranking strategy by resource_type:
    - single_core: Sort ONLY by single_core_score. Multi-core and GPU scores IGNORED.
      Use case: Node.js scripts, single-threaded Python, sequential processing.
    
    - multi_core (or auto): Sort ONLY by multi_core_score. Single-core and GPU IGNORED.
      Use case: FFmpeg libx264 -threads 0, gcc -j, parallel compression.
    
    - gpu: Filter by min_vram_mb, then sort by vram_mb and hw_encoder count.
      CPU scores IGNORED. Use case: NVENC/Videotoolbox encoding, Blender, Ollama.
    """
    workers = list(worker_telemetry.items())
    
    # Hard filter: exclude workers on battery below 20%
    workers = [
        (ip, t) for ip, t in workers
        if t.get("battery_pct", 100) >= 20 or t.get("is_plugged_in", True)
    ]
    
    # Priority penalty: Deprioritize nodes already running high background loads
    # Instead of sending a task and immediately throttling it, rank busy nodes lower
    # to give priority to completely idle nodes.
    def compute_penalty(telemetry: dict) -> float:
        cpu_usage = telemetry.get("current_system_cpu_usage", 0.0)
        gpu_usage = telemetry.get("current_system_gpu_usage", 0.0)
        ram_usage = telemetry.get("current_system_ram_usage", 0.0)
        
        penalty = 1.0
        if cpu_usage > 70.0: penalty *= 0.1
        if gpu_usage > 70.0: penalty *= 0.1
        if ram_usage > 85.0: penalty *= 0.1
        return penalty

    if resource_type == "single_core":
        workers.sort(key=lambda x: x[1].get("single_core_score", 0) * compute_penalty(x[1]), reverse=True)
    
    elif resource_type in ("multi_core", "auto"):
        workers.sort(key=lambda x: x[1].get("multi_core_score", 0) * compute_penalty(x[1]), reverse=True)
    
    elif resource_type == "gpu":
        # Hard filter: must meet minimum VRAM
        if min_vram_mb > 0:
            workers = [
                (ip, t) for ip, t in workers
                if t.get("vram_mb", 0) >= min_vram_mb
            ]
        # Sort by: number of hw encoders (primary), VRAM (secondary)
        workers.sort(
            key=lambda x: (
                len(x[1].get("hw_encoders", [])),
                x[1].get("vram_mb", 0),
            ),
            reverse=True,
        )
    
    return [ip for ip, _ in workers]
```

#### D. Integration in `master.py`

Replace the current sequential slicing in `run_master()`:

```python
# CURRENT (line ~1455):
active_nodes = worker_ip_addresses[:config.max_nodes]
spare_nodes = worker_ip_addresses[config.max_nodes:]

# REPLACEMENT:
ranked_ips = rank_workers_by_affinity(
    worker_pool.worker_telemetry,
    config.resource_type,
    config.min_vram_mb,
)
active_nodes = ranked_ips[:config.max_nodes]
spare_nodes = ranked_ips[config.max_nodes:]
```

---

### 4.5 Master-to-Worker Command Transmission

**Problem:** Worker currently reads `execute_command` from its own local config file. This creates two issues:
1. Workers need config files (friction)
2. OS-specific command resolution can't happen on the master side (the master knows the worker's OS from telemetry but can't act on it)

**Solution:** Include the resolved `execute_command` in the `ready` message.

**Master side (when dispatching a chunk):**
```python
worker_os = worker_pool.worker_telemetry.get(worker_ip, {}).get("os_type", "linux")
resolved_cmd = config.get_execute_command(worker_os)
send_ready_message(connection, master_ip, daemon.port, execute_command=resolved_cmd)
```

**Modified `ready` message format:**
```json
{
    "type": "ready",
    "master_ip_address": "192.168.1.50",
    "file_transfer_port": 54321,
    "execute_command": "ffmpeg -y -i {input} -c:v h264_videotoolbox -preset fast {output_directory}/{output}"
}
```

**Worker side:**
```python
ready_msg = receive_ready_message(connection)
execute_cmd = ready_msg.get("execute_command", config.execute_command)
# Use execute_cmd instead of config.execute_command
```

The `execute_command` field is optional in `ready` for backward compatibility. If absent, worker falls back to its local config (existing behavior).

---

### 4.7 System Watchdog & Resource Throttling Mechanism Specification

**Why this matters:** In a friend-to-friend device lending setup, the friend must be able to use their laptop for browsing, coding, or watching videos without the background compute task freezing their system. Monitoring CPU load and dynamically throttling worker process priority guarantees zero UI lag for the donor node.

#### A. Architecture & Sampling Math

The watchdog thread monitors total system CPU usage (`sys_pct`) and isolates worker process usage (`proc_pct`) to compute background non-task CPU load (`bg_pct`):

$$\text{bg\_pct} = \max(0.0, \text{sys\_pct} - \text{proc\_pct})$$

- **Sampling Rate:** Every 1.0s in `run_execute_command()` loop.
- **Linux Implementation:** Parses `/proc/stat` for aggregate system CPU ticks and `/proc/<pid>/stat` for process ticks (user space `utime` + kernel space `stime`).
- **macOS / Windows Implementation:** Fallback to `psutil.cpu_percent(interval=None)` and `psutil.Process(pid).cpu_percent(interval=None)`.

#### B. Dynamic Priority Adjustment (Niceness Escalation)

When $\text{bg\_pct} > \text{cpu\_threshold\_percent}$ for continuous $\text{elapsed} \ge \text{cpu\_threshold\_seconds}$:

1. **Policy `kill`:** Worker immediately sends `SIGKILL` / `terminate()` to process, raises `RuntimeError`, and notifies master with `{"type": "failed", "reason": "cpu_high_usage"}`.
2. **Policy `throttle`:**
   - On first trigger: `is_throttled = True`, process niceness set to `nice_initial` (default 5).
   - If $\text{bg\_pct}$ continues to escalate during `nice_check_interval_seconds` (5s), niceness increases stepwise:

$$\text{nice}_{\text{next}} = \min(\text{nice}_{\text{max}}, \text{nice}_{\text{current}} + \text{nice}_{\text{step}})$$

- **OS Niceness Mappings:**
  - **Linux / macOS (POSIX):** `nice` values 0 (default priority) to 19 (idle priority). `set_process_nice()` invokes `os.setpriority(os.PRIO_PROCESS, pid, val)` with fallback to `psutil.Process(pid).nice(val)`.
  - **Windows:** Niceness maps to Windows Process Priority Classes:
    - $\text{nice} = 0$: `psutil.NORMAL_PRIORITY_CLASS`
    - $1 \le \text{nice} \le 10$: `psutil.BELOW_NORMAL_PRIORITY_CLASS`
    - $\text{nice} > 10$: `psutil.IDLE_PRIORITY_CLASS`

#### C. Max Throttling Safeguard & Auto-Recovery

- **Max Duration Timeout:** If task remains throttled for $\ge \text{max\_throttle\_duration\_seconds}$ (default 600s / 10 min) AND `long_runnable == False`, the watchdog terminates the process to prevent deadlocks or unacceptably slow progression.
- **Auto-Recovery (Unthrottling):** When $\text{bg\_pct} \le \text{cpu\_threshold\_percent}$ for continuous $\ge \text{unthrottle\_threshold\_seconds}$ (default 20s):
  - Worker restores niceness to 0 (`NORMAL_PRIORITY_CLASS`).
  - Sets `is_throttled = False`, `throttled_start_time = None`.
  - Emits `[RESOURCE RECOVERY]` notification to console and master telemetry.

#### D. CPU vs. GPU Throttling Strategy

- **CPU Tasks (ffmpeg CPU libx264, blender CPU cycles):** Fully throttlable via OS process priority (`nice` / priority class).
- **GPU Tasks (ffmpeg Apple VideoToolbox, NVIDIA NVENC, Ollama GPU inference):** GPU hardware queues cannot be directly deprioritized via CPU `nice`.
  - **GPU Throttling Mechanism:** When background activity is detected during a GPU task, the worker sends a `{"type": "throttle_alert", "worker_ip": ip, "reason": "gpu_load"}` message to the Master.
  - The Master holds task dispatch for 30s. If background load does not subside, the Master gracefully pauses or migrates the chunk to a spare worker.

---

### 4.8 Master-Assigned UIDs & Persistent Tracking

**Why this matters:** When a worker disconnects and reconnects (e.g., due to a network glitch or a laptop sleeping), the master needs to reliably identify it to preserve task progress and avoid re-doing the initial hardware benchmark. IP addresses can change via DHCP.

**Mechanism:**
1. **Worker Connects:** The worker reads `config/state.json`. If it lacks a `uid`, it sends `{"type": "uid_request"}` or includes `uid: null` in its handshake.
2. **Master Generates UID:** The master creates a persistent identifier (`uuid4`) and replies `{"type": "uid_assign", "uid": "a1b2c3..."}`.
3. **Worker Persists:** Worker saves the UID locally. On future connections, it transmits the UID in its handshake/telemetry payload, ensuring the master maps it to the same logical node.

---

### 4.9 Automated Dynamic Task Queue

**Why this matters:** The previous failover mechanism (v1) assigned Chunk 1 to Node 1, Chunk 2 to Node 2. If Node 1 failed and no spare nodes were available, the pipeline halted entirely pending manual user interaction.

**Mechanism (Queue-Based Orchestration):**
1. **Task Pool:** All task chunks are placed into a central queue `[part1, part2, part3...]`.
2. **Dynamic Dispatch:** When any worker finishes a task, it immediately requests the next chunk from the queue.
3. **Seamless Failover:** If a worker disconnects or drops a task midway, the chunk is simply returned to the front of the queue.
4. **No Spare Nodes Needed:** All connected nodes actively process chunks. A fast 16-core desktop might process 5 chunks in the time a 4-core laptop processes 1. If the laptop fails, the desktop will naturally dequeue and finish the laptop's abandoned chunk without any manual prompts.
5. **Manual Override (`--manual`):** If the master is launched with the `--manual` flag, this automated queue logic is disabled for recovery. In the event of a failure with no spare nodes, the master pauses and presents the interactive `[k]`, `[w]`, `[r]`, `[b]` prompts to the user.

---

## 5. Complete Message Protocol Reference

All messages travel over persistent TCP on port 5000, serialized as JSON terminated by `\n`.

### Complete Worker-Master Lifecycle

```
Worker                                          Master
  │                                                │
  │──── TCP connect to :5000 ─────────────────────▶│
  │                                                │
  │  ┌──── PIN PAIRING (if no shared secret) ───┐  │
  │◀─│── {"type":"pin_challenge", ...}          │──│
  │──│── {"type":"pin_response","pin":"7042"}    │─▶│
  │◀─│── {"type":"pin_accepted"}                │──│
  │  └─────────────────────────────────────────┘   │
  │                                                │
  │  ┌── OR: SHARED SECRET (if env var set) ────┐  │
  │──│── {"type":"auth","secret":"xxx"}         │─▶│
  │  └─────────────────────────────────────────┘   │
  │                                                │
  │  ┌── BINARY HANDSHAKE (if require_binary) ──┐  │
  │◀─│── {"type":"binary_info_request", ...}    │──│
  │──│── {"type":"binary_info", os, arch, md5}  │─▶│
  │◀─│── {"type":"binary_ready"}                │──│  (or file transfer first)
  │  └─────────────────────────────────────────┘   │
  │                                                │
  │──── {"type":"worker_telemetry", scores...} ──▶│  ← NEW: benchmark results
  │                                                │
  │     ┌─── TASK LOOP (repeats per chunk) ────┐   │
  │◀────│── {"type":"ready", ip, port, cmd}    │───│  ← MODIFIED: +execute_command
  │     │   HTTP GET /listfiles                │   │
  │     │   HTTP GET /file/part1.mkv           │   │
  │────▶│── {"type":"file_received"}           │──▶│
  │     │   [execute command locally]          │   │
  │     │   HTTP POST /file/part1.mkv (result) │   │
  │────▶│── {"type":"finished", time, file}    │──▶│
  │     └──────────────────────────────────────┘   │
  │                                                │
  │◀──── {"type":"shutdown"} ─────────────────────│  (all tasks done)
```

### All Message Types

| Type | Direction | Required Fields | Purpose |
|---|---|---|---|
| `pin_challenge` | Master → Worker | `type`, `master_name` | Initiate PIN pairing |
| `pin_response` | Worker → Master | `type`, `pin` | Worker submits typed PIN |
| `pin_accepted` | Master → Worker | `type` | PIN verified, pairing successful |
| `pin_rejected` | Master → Worker | `type`, `reason`, `attempts_left` | Wrong PIN |
| `auth` | Worker → Master | `type`, `secret` | Pre-shared secret (alternative to PIN) |
| `binary_info_request` | Master → Worker | `type`, `binary_name` | Ask worker about its binary cache |
| `binary_info` | Worker → Master | `type`, `os_name`, `arch`, `binary_name`, `has_binary`, `md5` | Worker reports binary status |
| `binary_ready` | Master → Worker | `type` | Binary check done, proceed |
| `worker_telemetry` | Worker → Master | `type`, `single_core_score`, `multi_core_score`, `os_type`, `cpu_count` | Hardware benchmark results |
| `ready` | Master → Worker | `type`, `master_ip_address`, `file_transfer_port` | Task assignment (optional: `execute_command`) |
| `file_received` | Worker → Master | `type` | Chunk download confirmed |
| `finished` | Worker → Master | `type`, `execution_time`, `part_filename` | Task completed |
| `throttle_alert` | Worker → Master | `type`, `worker_ip`, `nice_level`, `reason` | Worker background CPU load high, task priority degraded |
| `failed` | Worker → Master | `type`, `reason` | Task failed |
| `shutdown` | Master → Worker | `type` | Master done, worker should exit |

---

## 6. File Structure at Project Completion

```
project/
├── core.py                    # Shared library: constants, config, networking,
│                              #   benchmarks, matchmaker, discovery (mDNS + UDP),
│                              #   PIN pairing, file transfer, execution, split/merge,
│                              #   dashboard, cleanup
├── master.py                  # Master orchestrator
├── worker.py                  # Worker agent
├── requirements.txt           # psutil
├── config/
│   ├── sw1.ini                # Fast ultrafast encode (resource_type = multi_core)
│   ├── sw2.ini
│   ├── sw3.ini
│   ├── sw4.ini
│   └── mayday.ini             # Complex filter chain (resource_type = multi_core)
├── binaries/
│   ├── ffmpeg/
│   │   ├── linux/ffmpeg       (76 MB)
│   │   └── macos/ffmpeg       (43 MB)
│   └── blender/
│       ├── linux/blender      (166 MB + libs)
│       └── macos/...          (app bundle)
├── master/
│   ├── input/                 # Source video + split parts (temp)
│   └── output/                # Collected results + merged output
├── worker/
│   ├── input/                 # Downloaded chunks (temp, auto-cleaned)
│   └── output/                # Processed results (temp, auto-cleaned)
├── devices.json               # Active node IP → device ID mapping
├── demo_failover.py           # Automated failover test
└── .env                       # DIST_SHARED_SECRET (optional, for dev/testing)
```
