# Implementation Task List — LAN Distributed Compute System

> Each task specifies **exactly** what file to edit, what function to add/modify, and why.
> Phases are ordered by dependency: each phase can be tested independently before starting the next.

---

## Phase 1: Fix Broken & Unused Existing Code

These are bugs and dead code in the current codebase that must be fixed before building new features.

---

### `[ ]` 1.1 — Fix `BINARY_PLATFORM_MAP` dead entry

**File:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py) lines 149–153

**Problem:** The map has `("Darwin", "aarch64")` which is dead code. Python on macOS Apple Silicon returns `platform.machine()` as `"arm64"`, never `"aarch64"`. Also the mapped folder name `"aarch64"` is inconsistent — should match what Python reports.

**Current code:**
```python
BINARY_PLATFORM_MAP = {
    ("Linux",  "x86_64"):  ("linux", "x64"),
    ("Darwin", "arm64"):   ("macos", "aarch64"),
    ("Darwin", "aarch64"): ("macos", "aarch64"),  # ← never matched
}
```

**Replace with:**
```python
BINARY_PLATFORM_MAP = {
    ("Linux",  "x86_64"): ("linux", "x64"),
    ("Darwin", "arm64"):  ("macos", "arm64"),
}
```

**Also:** Rename the physical folder `binaries/ffmpeg/macos/` and `binaries/blender/macos/` (if they use `aarch64` subfolders) to match. Check `get_binary_path()` to ensure it resolves correctly with the new naming.

**Why this matters:** Without this fix, binary distribution to macOS workers may look in the wrong directory path.

---

### `[ ]` 1.2 — Wire up `config.get_split_command()` and `config.get_merge_command()` in `master.py`

**File:** [`master.py`](file:///home/anishudupan/claudecode/project/master.py) lines 1503, 1511, 1548, 1551

**Problem:** Master uses `config.split_command` and `config.merge_command` directly, bypassing the OS-specific override methods we added to the `Config` dataclass. If a macOS user runs the master with `split_command_macos` set in config, it gets ignored.

**Change:**
```python
# Line 1503: change config.split_command → config.get_split_command()
# Line 1511: change split_command=config.split_command → split_command=config.get_split_command()
# Line 1548: change config.merge_command → config.get_merge_command()
# Line 1551: change split_command=config.split_command → split_command=config.get_split_command()
```

**Also:** Find any other `config.split_command` references in master.py (grep for them) and update similarly.

**Why this matters:** The Config override methods exist but are never called — dead code until we wire them up.

---

### `[ ]` 1.3 — Create `requirements.txt`

**File:** New file at project root: `requirements.txt`

**Content:**
```
psutil>=5.9.0
```

**Why this matters:** `ProcessCpuTracker` on macOS falls back to `psutil`. `detect_power_state()` uses `psutil.sensors_battery()`. Without declaring this dependency, CPU monitoring and battery detection silently return zeros on macOS.

---

### `[ ]` 1.4 — Fix `mayday.ini` throttle config (nice values are all zero)

**File:** [`config/mayday.ini`](file:///home/anishudupan/claudecode/project/config/mayday.ini) lines 11–13

**Problem:** `on_high_usage = throttle` is set, but `nice_initial = 0`, `nice_step = 0`, `nice_max = 0`. This means throttling "activates" but niceness never changes — the process stays at default priority. This is a configuration error, not a code bug.

**Change:**
```ini
nice_initial = 5
nice_step = 5
nice_max = 19
```

**Why this matters:** Without this fix, the throttle feature appears broken during demos ("it says throttled but nothing actually changes").

---

### `[ ]` 1.5 — Ensure `psutil` fallback works correctly for macOS niceness

**File:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py) — `set_process_nice()` around line 1462

**Problem:** On macOS, `os.setpriority()` exists but raising niceness on another user's process may fail silently. The `psutil` fallback is fine on macOS (psutil maps nice values correctly on POSIX). On future Windows support, psutil expects `psutil.IDLE_PRIORITY_CLASS` constants instead of numeric values — but since we're deferring Windows, just ensure the psutil fallback path is correctly used on macOS.

**Action:** Add a comment documenting the macOS behavior. Ensure the `except Exception: pass` in the `os.setpriority` path falls through to psutil correctly. Test on macOS by verifying `psutil.Process(pid).nice()` returns the expected value after calling `set_process_nice(pid, 19)`.

---

## Phase 2: Benchmarking Suite (core.py)

All new functions in [`core.py`](file:///home/anishudupan/claudecode/project/core.py). Add a new section header:
```python
# ==============================================================================
# SECTION X: HARDWARE BENCHMARKING & TELEMETRY
# ==============================================================================
```

---

### `[ ]` 2.1 — Implement `benchmark_single_core()`

**File:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py)

**Signature:** `def benchmark_single_core(duration_seconds: float = 0.5) -> float`

**Implementation:** Tight single-threaded SHA-256 hashing loop on a ~1KB block. Returns operations/second. See spec §4.3-A for complete code.

**Test:** Run on current machine, verify it returns a positive float (should be ~10,000–50,000 ops/s depending on CPU).

---

### `[ ]` 2.2 — Implement `benchmark_multi_core()`

**File:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py)

**Signature:** `def benchmark_multi_core(duration_seconds: float = 1.0) -> float`

**Implementation:** Spawns `os.cpu_count()` processes via `multiprocessing.Pool`, each running `benchmark_single_core()`. Returns total aggregate ops/second. See spec §4.3-B for complete code.

**Test:** Result should be roughly `single_core_score × cpu_count` (with some overhead).

---

### `[ ]` 2.3 — Implement `detect_gpu_info()`

**File:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py)

**Signature:** `def detect_gpu_info() -> dict`

**Implementation:** 
- Linux: Run `nvidia-smi --query-gpu=name,memory.total --format=csv,noheader,nounits`, parse output
- macOS: Run `system_profiler SPDisplaysDataType -json`, parse JSON
- Both: Run `ffmpeg -hide_banner -encoders`, grep for known hardware encoder names

Returns `{"gpu_name": str, "vram_mb": int, "hw_encoders": list[str]}`. See spec §4.3-C for complete code.

**Test:** On Linux without NVIDIA GPU, should return empty strings and empty list. On macOS, should detect "Apple Silicon GPU" and `h264_videotoolbox`.

---

### `[ ]` 2.4 — Implement `detect_power_state()`

**File:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py)

**Signature:** `def detect_power_state() -> dict`

**Implementation:** Uses `psutil.sensors_battery()`. Returns `{"is_plugged_in": bool, "battery_pct": int, "has_battery": bool}`. See spec §4.5 for complete code.

**Test:** On desktop (no battery), should return `is_plugged_in=True, battery_pct=100, has_battery=False`.

---

### `[ ]` 2.5 — Implement `_get_total_ram_mb()` helper

**File:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py)

**Signature:** `def _get_total_ram_mb() -> int`

**Implementation:** `psutil.virtual_memory().total // (1024*1024)` with fallback to `/proc/meminfo`. See spec §4.3-E.

---

### `[ ]` 2.6 — Implement `run_node_benchmark()` combining all above

**File:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py)

**Signature:** `def run_node_benchmark() -> dict`

**Implementation:** Calls `benchmark_single_core()`, `benchmark_multi_core()`, `detect_gpu_info()`, `detect_power_state()`, `_get_total_ram_mb()`. Prints formatted summary to stdout. Returns combined dict. See spec §4.3-E for complete code.

**Test:** Run standalone — should print benchmark summary and return valid dict.

---

### `[ ]` 2.7 — Add `worker_telemetry` message type

**File:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py) — near `MESSAGE_REQUIRED_KEYS` (around line 73)

**Add:**
```python
MESSAGE_TYPE_WORKER_TELEMETRY = "worker_telemetry"

# In MESSAGE_REQUIRED_KEYS dict:
MESSAGE_TYPE_WORKER_TELEMETRY: ['type', 'single_core_score', 'multi_core_score', 'os_type', 'cpu_count'],
```

**Also add helper functions:**
```python
def send_worker_telemetry(connection: socket.socket, telemetry: dict) -> None:
    send_json_message(connection, {"type": MESSAGE_TYPE_WORKER_TELEMETRY, **telemetry})

def receive_worker_telemetry(connection: socket.socket) -> dict:
    msg = receive_json_message(connection)
    validate_message(msg)
    if msg["type"] != MESSAGE_TYPE_WORKER_TELEMETRY:
        raise ValueError(f"Expected worker_telemetry, got {msg['type']}")
    return msg
```

---

## Phase 3: PIN-Based Pairing (core.py + master.py + worker.py)

---

### `[ ]` 3.1 — Add PIN message types and helper functions to `core.py`

**File:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py)

**Add constants:**
```python
MESSAGE_TYPE_PIN_CHALLENGE = "pin_challenge"
MESSAGE_TYPE_PIN_RESPONSE = "pin_response"
MESSAGE_TYPE_PIN_ACCEPTED = "pin_accepted"
MESSAGE_TYPE_PIN_REJECTED = "pin_rejected"
```

**Add to `MESSAGE_REQUIRED_KEYS`:**
```python
MESSAGE_TYPE_PIN_CHALLENGE: ['type', 'master_name'],
MESSAGE_TYPE_PIN_RESPONSE: ['type', 'pin'],
MESSAGE_TYPE_PIN_ACCEPTED: ['type'],
MESSAGE_TYPE_PIN_REJECTED: ['type', 'reason', 'attempts_left'],
```

**Add functions:**
- `generate_pin() -> str` — generates random 4-digit PIN, rejects trivial patterns (0000, 1111, 1234, 4321, 9876, 0123)
- `send_pin_challenge(connection, master_name)`
- `receive_pin_challenge(connection) -> dict`
- `send_pin_response(connection, pin: str)`
- `receive_pin_response(connection) -> dict`
- `send_pin_accepted(connection)`
- `send_pin_rejected(connection, reason: str, attempts_left: int)`

---

### `[ ]` 3.2 — Implement PIN challenge in `WorkerConnectionPool._accept_loop()` (master.py)

**File:** [`master.py`](file:///home/anishudupan/claudecode/project/master.py) — `_accept_loop()` method (around line 107)

**Current flow:** accept → check allowlist → verify shared secret → store connection

**New flow:** accept → check allowlist → **if no shared secret: run PIN pairing; else: verify shared secret** → store connection

**Implementation:**
1. After TCP accept, if `self._shared_secret` is None (no pre-shared secret configured):
   a. Generate PIN via `generate_pin()`
   b. Add dashboard message: `f"Worker {ip} wants to join. PIN: {pin}"`
   c. Send `pin_challenge` with `master_name = socket.gethostname()`
   d. Receive `pin_response` (with 10s timeout)
   e. If PIN matches: send `pin_accepted`, store `ip` in `self.paired_ips` set
   f. If wrong: send `pin_rejected`, allow 2 more attempts, then close socket
2. If the worker IP is already in `self.paired_ips` (reconnection within same session): skip PIN, go straight to storing connection
3. Add `self.paired_ips: set[str] = set()` to `__init__`

---

### `[ ]` 3.3 — Implement PIN response in `worker.py`

**File:** [`worker.py`](file:///home/anishudupan/claudecode/project/worker.py) — after `connect_to_master()` succeeds (around line 360)

**Current flow:** connect → send auth message → do binary handshake

**New flow:** connect → **receive first message: if `pin_challenge` → prompt user and respond; if nothing (shared secret mode) → send auth message** → do binary handshake

**Implementation:**
1. After connecting, if shared_secret is None:
   a. Receive `pin_challenge` message
   b. Print: `f"Master '{master_name}' requests pairing. Enter PIN shown on master's screen: "`
   c. Read PIN from `input()` (standard blocking input — worker terminal is interactive)
   d. Send `pin_response`
   e. Receive response: if `pin_accepted` → continue; if `pin_rejected` → retry up to 3 times or exit
2. If shared_secret is set: send `auth` message (existing behavior)

---

## Phase 4: Worker Telemetry & Matchmaker (worker.py + master.py)

---

### `[ ]` 4.1 — Run benchmark and send telemetry at worker startup

**File:** [`worker.py`](file:///home/anishudupan/claudecode/project/worker.py) — after pairing/auth, before task loop (around line 365)

**Add after authentication succeeds:**
```python
# Run hardware benchmark (~2 seconds)
from core import run_node_benchmark, send_worker_telemetry
telemetry = run_node_benchmark()
send_worker_telemetry(connection, telemetry)
```

**Also:** Export `run_node_benchmark`, `send_worker_telemetry` from `core.py` imports at top of worker.py.

---

### `[ ]` 4.2 — Store worker telemetry in `WorkerConnectionPool`

**File:** [`master.py`](file:///home/anishudupan/claudecode/project/master.py) — `WorkerConnectionPool` class

**Add to `__init__`:**
```python
self.worker_telemetry: dict[str, dict] = {}  # {ip_or_key: telemetry_dict}
```

**Add method:**
```python
def receive_and_store_telemetry(self, key: str, connection: socket.socket) -> dict:
    """Receive worker_telemetry message and store it."""
    msg = receive_worker_telemetry(connection)
    with self.lock:
        self.worker_telemetry[key] = msg
    return msg
```

**Call this** in `_accept_loop()` after PIN/auth succeeds and before storing the connection, or in a new post-connection phase in `run_master()` after `get_connection()` returns.

**Decision point:** The telemetry receive should happen in the `_accept_loop` thread since it's part of the connection handshake. After `pin_accepted`/`auth` → receive `worker_telemetry` → store in `self.worker_telemetry[node_key]` → store socket in `self.connections[node_key]`.

---

### `[ ]` 4.3 — Add `resource_type` and `min_vram_mb` to `Config` dataclass

**File:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py) — `Config` dataclass (around line 294)

**Add fields:**
```python
resource_type: str = "auto"    # single_core | multi_core | gpu | auto
min_vram_mb: int = 0           # minimum VRAM for gpu tasks
```

**Update `load_config()`:** (around line 459)
```python
resource_type=section.get("resource_type", "auto").strip().lower(),
min_vram_mb=section.getint("min_vram_mb", fallback=0),
```

**Validate:** `resource_type` must be one of `("single_core", "multi_core", "gpu", "auto")`. Default to `"auto"` if invalid.

---

### `[ ]` 4.4 — Implement `rank_workers_by_affinity()` 

**File:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py) (new function, place near matchmaker section)

**Implementation:** See spec §4.4-C for complete code. Key logic:
1. Hard filter: exclude battery < 20% workers
2. Sort by the ONE metric that matters for the task's `resource_type`
3. Return ordered list of IPs (best first)

---

### `[ ]` 4.5 — Replace sequential node assignment with ranked assignment in `master.py`

**File:** [`master.py`](file:///home/anishudupan/claudecode/project/master.py) — `run_master()` (around line 1455)

**Current:**
```python
active_nodes = worker_ip_addresses[:config.max_nodes]
spare_nodes = worker_ip_addresses[config.max_nodes:]
```

**Replace with:**
```python
ranked_ips = rank_workers_by_affinity(
    worker_pool.worker_telemetry,
    config.resource_type,
    config.min_vram_mb,
)
# If matchmaker returned fewer workers than available (filtered out), fall back
if not ranked_ips:
    ranked_ips = list(worker_pool.connections.keys())
active_nodes = ranked_ips[:config.max_nodes]
spare_nodes = ranked_ips[config.max_nodes:]
```

**Also:** Print matchmaker results to dashboard: `"Matchmaker ranked N workers by {resource_type}. Top: {active_nodes}"`

---

### `[ ]` 4.6 — Transmit resolved `execute_command` in `ready` message

**File:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py) — `send_ready_message()` (around line 840)

**Current signature:**
```python
def send_ready_message(connection, master_ip_address, file_transfer_port):
```

**Add optional `execute_command` parameter:**
```python
def send_ready_message(connection, master_ip_address, file_transfer_port, execute_command: str | None = None):
    msg = {"type": MESSAGE_TYPE_READY, "master_ip_address": master_ip_address, "file_transfer_port": file_transfer_port}
    if execute_command:
        msg["execute_command"] = execute_command
    send_json_message(connection, msg)
```

**In `master.py`** — where `send_ready_message()` is called for each worker:
```python
worker_os = worker_pool.worker_telemetry.get(worker_ip, {}).get("os_type", "")
resolved_cmd = config.get_execute_command(worker_os)
send_ready_message(conn, master_ip, daemon.port, execute_command=resolved_cmd)
```

**In `worker.py`** — where `run_worker_task()` is called:
```python
# In run_worker_task, after receiving ready_message:
execute_cmd = ready_message.get("execute_command", execute_command_template)
# Use execute_cmd instead of execute_command_template for run_execute_command()
```

---

## Phase 5: UDP Broadcast Discovery (core.py + master.py + worker.py)

---

### `[ ]` 5.1 — Add UDP discovery constants and `start_udp_discovery_listener()` to `core.py`

**File:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py)

**Add constants:**
```python
UDP_DISCOVERY_PORT = 5005
DISCOVERY_BEACON_TYPE = "DISCOVERY_BEACON"
MASTER_OFFER_TYPE = "MASTER_OFFER"
```

**Add function:** `start_udp_discovery_listener(master_ip, control_port) -> threading.Thread`

See spec §4.2 for complete implementation. Background daemon thread listening on `0.0.0.0:5005`, replying with master IP and control port.

---

### `[ ]` 5.2 — Start UDP listener thread in `master.py`

**File:** [`master.py`](file:///home/anishudupan/claudecode/project/master.py) — in `run_master()`, after network setup (around line 1340)

**Add after mDNS announcer start:**
```python
# Start UDP broadcast discovery listener (works on mobile hotspots where mDNS fails)
master_ip_for_discovery = get_local_ip_for_peer("8.8.8.8") if not arguments.master_ip else arguments.master_ip
udp_discovery_thread = start_udp_discovery_listener(master_ip_for_discovery, FIXED_PORT)
```

---

### `[ ]` 5.3 — Add `discover_master_udp()` to `core.py`

**File:** [`core.py`](file:///home/anishudupan/claudecode/project/core.py)

**Signature:** `def discover_master_udp(timeout_seconds: float = 30.0) -> tuple[str, int]`

See spec §4.2 for complete implementation. Broadcasts `DISCOVERY_BEACON` to `255.255.255.255:5005` every 2 seconds, waits for `MASTER_OFFER` reply.

---

### `[ ]` 5.4 — Add UDP fallback to `worker.py` discovery flow

**File:** [`worker.py`](file:///home/anishudupan/claudecode/project/worker.py) — around lines 308–314

**Current:**
```python
if not master_ip_address:
    master_ip_address = discover_master_ip(timeout_seconds=60.0)
```

**Replace with:**
```python
if not master_ip_address:
    print("Auto-discovering master...")
    try:
        # Try mDNS first (works on home routers with mDNS reflectors)
        master_ip_address = discover_master_ip(timeout_seconds=10.0)
        print(f"Found master via mDNS: {master_ip_address}")
    except TimeoutError:
        print("mDNS timed out. Trying UDP broadcast (works on mobile hotspots)...")
        try:
            master_ip_address, _ = discover_master_udp(timeout_seconds=30.0)
            print(f"Found master via UDP broadcast: {master_ip_address}")
        except TimeoutError:
            print("Error: Could not discover master on the network.", file=sys.stderr)
            sys.exit(1)
```

**Also:** Add `discover_master_udp` to the imports from `core`.

---

## Phase 6: Update Config Files & Dependencies

---

### `[ ]` 6.1 — Add `resource_type` to all config `.ini` files

**Files:** All files in [`config/`](file:///home/anishudupan/claudecode/project/config/)

Add to each `.ini` file's `[DEFAULT]` section:
```ini
# What hardware resource does this task depend on?
# Options: single_core | multi_core | gpu | auto
resource_type = multi_core
```

Also add `min_vram_mb = 0` to each file for completeness.

---

### `[ ]` 6.2 — Add `execute_command_macos` to config files where relevant

**Files:** Config files in [`config/`](file:///home/anishudupan/claudecode/project/config/) that use ffmpeg encoding

For configs that encode with `libx264`, add a macOS override using Apple's VideoToolbox:
```ini
# macOS Apple Silicon hardware encoding (much faster than libx264 on M-series)
execute_command_macos = ffmpeg -y -i {input} -c:v h264_videotoolbox -q:v 65 {output_directory}/{output}
```

---

## Phase 7: End-to-End Testing & Verification

---

### `[ ]` 7.1 — Unit test: benchmark suite returns valid scores

**Action:** Run `python3 -c "from core import run_node_benchmark; print(run_node_benchmark())"` on the development machine. Verify:
- `single_core_score` > 0
- `multi_core_score` > `single_core_score`
- `os_type` is `"linux"` or `"darwin"`
- `cpu_count` matches `os.cpu_count()`
- `total_ram_mb` > 0

---

### `[ ]` 7.2 — Integration test: PIN pairing on localhost

**Action:** Run master and worker on the same machine using loopback:
```bash
# Terminal 1 (Master):
python master.py --config config/sw1.ini 127.0.0.1

# Terminal 2 (Worker):
python worker.py 127.0.0.1
```

**Verify:**
- Master displays PIN on dashboard
- Worker prompts for PIN input
- After typing correct PIN, worker proceeds to benchmark and task loop
- After typing wrong PIN 3 times, worker is disconnected

---

### `[ ]` 7.3 — Integration test: matchmaker ranking

**Action:** Start master and 2 workers (on loopback or LAN) with `resource_type = multi_core`. Verify in logs/dashboard that the worker with the higher `multi_core_score` is assigned to `node1` (first active slot).

---

### `[ ]` 7.4 — Integration test: UDP discovery on real Wi-Fi

**Action:** Connect master and worker to the same Wi-Fi network (or phone hotspot). Start worker without specifying master IP. Verify:
- Worker first tries mDNS (may or may not work depending on network)
- If mDNS fails, falls back to UDP broadcast
- Worker connects successfully to master

---

### `[ ]` 7.5 — End-to-end test: cross-platform execution

**Action:** Run master on Linux, worker on macOS (Apple Silicon). Use a config with both `execute_command` (Linux default) and `execute_command_macos`. Verify:
- Master resolves correct command for the macOS worker based on `os_type` from telemetry
- macOS worker executes with `h264_videotoolbox` (or whatever is in `execute_command_macos`)
- Result uploads successfully, merge completes

---

### `[ ]` 7.6 — Failover test with benchmarked spare selection

**Action:** Start 3 workers (2 active, 1 spare). Kill worker 1 mid-task. Verify:
- Spare worker takes over the failed chunk
- Spare worker was selected based on its benchmark score (logged in dashboard messages)
- Final merge completes successfully
