# Bug Fixes: Failsafe, Reconnection, Benchmarks, Throttling & Device Sorting

Comprehensive fix for multiple interrelated bugs across the master-worker distributed system.

## Identified Bugs (Grouped by Area)

### Area 1: Worker Reconnection — Exits Instead of Reconnecting

**Bug**: In [worker.py](file:///home/anishudupan/claudecode/project/worker.py#L463-L470), when a `ConnectionError`/`OSError` occurs during task execution (e.g. master drops connection mid-task, or the worker's task gets killed by 'k'), the worker **exits with `sys.exit(0)`** instead of reconnecting:

```python
except (ConnectionError, OSError) as exc:
    print(f"\nControl socket disconnected after pairing ({exc}). Exiting safely without retrying.", file=sys.stderr)
    sys.exit(0)  # BUG: Should reconnect, not exit!
```

The worker should set `connection = None` and loop back to the reconnect logic, just like the `RuntimeError`/`ValueError` handler does on line 471-480.

---

### Area 2: PIN/Password Not Shown on Master During Reconnection/Failsafe

**Bug**: In [master.py `_accept_loop`](file:///home/anishudupan/claudecode/project/master.py#L118-L263), when a new worker connects while the master is in the middle of executing tasks (failsafe/reconnection scenario), the PIN challenge and display logic runs correctly in the `_accept_loop` thread (line 138-165). However, the `dashboard.add_message` for the PIN uses a short `timeout_seconds=15` and `timeout_seconds=60`. When a node disconnects during execution and reconnects, the accept loop runs its PIN flow, but:

1. The dashboard message showing the PIN can get overwritten by higher-priority status messages from `monitor_worker_executions_and_collect_results` which update every 0.5s.
2. The PIN message timeout is too short — if the user isn't watching when the node reconnects, they miss the PIN entirely.

**Fix**: Increase PIN message priority/timeout and ensure it persists until the user acknowledges or until pairing completes.

---

### Area 3: Master Not Waiting for Reconnecting Nodes (No Dashboard Indication)

**Bug**: When a worker disconnects during execution, the master enters the failover path in [`monitor_worker_executions_and_collect_results`](file:///home/anishudupan/claudecode/project/master.py#L958-L1134). The retry logic at line 1058-1091 tries `worker_pool.get_connection(ip, timeout_seconds=8.0)` for the same-node retry. But:

1. The master doesn't display a "waiting for node to reconnect" message on the dashboard before calling `get_connection`.
2. The 8-second timeout for retry is very short — the worker may need more time to reconnect (especially if it was in the `sys.exit(0)` bug from Area 1).
3. If the retry fails, it goes straight to failover without ever telling the user it's waiting.

**Fix**: Add explicit dashboard messages during the reconnection wait, and increase the same-node retry timeout.

---

### Area 4: Master Not Reallocating Work After Second 'k' Press

**Bug**: When a node fails a task and the user presses 'k' to kill/restart, the system attempts one retry on the same node (line 1058-1091). If the node fails again (second failure), the `node_retries` counter hits >= 1 and it falls through to `reassign_task_to_spare_node`. But:

1. The worker that was killed by 'k' gets a `KeyboardInterrupt` exception (line 443-454 in worker.py) which closes the connection and sets `connection = None`. The worker then reconnects.
2. On the master side, the reconnected worker's old IP key still has `node_retries[ip] >= 1`, so it skips retry and goes to failover.
3. If there are no spare nodes, it enters `handle_no_spare_nodes_recovery` which prompts the user — but this only works if the failed node was disconnected. If the node successfully reconnected and is now idle in the pool, it isn't recognized as available because `active_tasks` still references it.

> [!IMPORTANT]
> The worker should NOT make any decisions about whether to retry a failed config. The worker should just execute whatever the master sends it. Currently, the worker doesn't have this issue (it doesn't track failures), but the master's retry tracking is tied to IP rather than task, causing confusion when IPs reconnect.

---

### Area 5: Dummy Benchmarks — Need Predefined Scores by CPU Name

**Bug**: The current [`run_node_benchmark()`](file:///home/anishudupan/claudecode/project/core.py#L2521-L2553) runs real SHA-256 benchmarks. The user wants predefined scores for demo/testing using a `scores.json` file on the worker side, keyed by CPU chip name:

- **i5 13500H**
- **Apple M4**
- **i5 11400U**

The worker should read `devices.json` (which has `cpu_name`), look up the CPU in a `scores.json` file, and return predefined benchmark numbers instead of running live benchmarks.

---

### Area 6: Device Sorting — Wrong Field and No Re-sort on Reconnect

**Bug**: In [`rank_workers_by_affinity`](file:///home/anishudupan/claudecode/project/core.py#L2605-L2621):

1. The configs use `resource_type = multi_core` (or `single_core`, `gpu`), and the ranking function sorts by `single_core_score`, `multi_core_score`, or `vram_mb`. This is correct for the current config setup.
2. **However**, when a device reconnects during failover, the master does NOT re-sort the worker list. The `rank_workers_by_affinity` is only called once at line 1496 of master.py before execution starts. When a spare node or reconnecting node becomes available during recovery, it's picked FIFO from the pool rather than being ranked.
3. In `reassign_task_to_spare_node` (line 904-912), spare nodes are selected by `spare_nodes.pop(0)` (FIFO) or by finding any unassigned IP in the pool — no sorting by capability.

**Fix**: When selecting a spare/reconnected node for failover, re-rank available nodes using `rank_workers_by_affinity` and pick the best one.

---

### Area 7: `printlog` Not Imported in worker.py

**Bug**: [`worker.py` lines 274, 282](file:///home/anishudupan/claudecode/project/worker.py#L274-L282) call `printlog()` but it's not imported from `core.py`. This would cause a `NameError` at runtime during cleanup.

---

## Proposed Changes

### Worker Module

#### [MODIFY] [worker.py](file:///home/anishudupan/claudecode/project/worker.py)

1. **Import `printlog`** from core (fix Area 7)
2. **Fix `ConnectionError`/`OSError` handler** (lines 463-470): Change from `sys.exit(0)` to reconnect loop — set `connection = None` and continue, matching the `RuntimeError`/`ValueError` handler pattern (fix Area 1)

---

### Core Module

#### [MODIFY] [core.py](file:///home/anishudupan/claudecode/project/core.py)

1. **Add predefined scores lookup** in `run_node_benchmark()`:
   - Read `devices.json` from worker directory for `cpu_name`
   - Read a new `scores.json` file keyed by CPU name
   - If the CPU name matches a known chip, return predefined scores instead of running live benchmarks
   - Fall back to live benchmarks if no match found
2. **Fix `rank_workers_by_affinity`** to handle `resource_type = "auto"` by defaulting to `multi_core_score` sort (currently returns unsorted if `resource_type` is `auto`)

---

#### [NEW] [scores.json](file:///home/anishudupan/claudecode/project/scores.json)

Predefined benchmark scores for known CPUs:
```json
{
  "i5 13500H": {
    "single_core_score": 28500.0,
    "multi_core_score": 310000.0,
    "total_ram_mb": 16384
  },
  "Apple M4": {
    "single_core_score": 35200.0,
    "multi_core_score": 285000.0,
    "total_ram_mb": 16384
  },
  "i5 11400U": {
    "single_core_score": 19800.0,
    "multi_core_score": 105000.0,
    "total_ram_mb": 8192
  }
}
```

The `cpu_name` field from `devices.json` will be used as the lookup key. You'll manually set it on each worker node.

---

### Master Module

#### [MODIFY] [master.py](file:///home/anishudupan/claudecode/project/master.py)

1. **Fix PIN display during reconnection** (Area 2):
   - Increase PIN message `timeout_seconds` to 120 (from 15/60) on the dashboard
   - Add a persistent "session PIN" display that persists in the dashboard header or footer

2. **Add "waiting for reconnection" dashboard message** (Area 3):
   - Before calling `worker_pool.get_connection()` in the retry path (line 1068), show a dashboard message
   - Increase same-node retry timeout from 8s to 30s

3. **Fix task reallocation after repeated failures** (Area 4):
   - After a node fails and the master decides to failover, properly check if the reconnected node is now idle in the pool as a potential spare
   - Clean up `active_tasks` tracking when a node is removed

4. **Re-sort nodes on reconnection for failover** (Area 6):
   - In `reassign_task_to_spare_node` and `handle_no_spare_nodes_recovery`, re-rank available nodes using `rank_workers_by_affinity` before selecting the best spare

---

## Open Questions

> [!IMPORTANT]
> **Benchmark scores values**: The predefined scores I've listed are approximate reasonable values for those chips. Should I adjust them, or will you provide exact numbers?

> [!IMPORTANT]
> **`devices.json` on worker side**: Currently `devices.json` contains `{"cpu_name": "Apple M4", "num_cores": 10, "ram_gb": 16}`. Should the `scores.json` be a separate file (my current plan), or should the scores be embedded into `devices.json`?

> [!IMPORTANT]
> **`resource_type` vs `process_type`**: You mentioned "fetching appropriate scores of the process type" — the configs use `resource_type` (values: `multi_core`, `single_core`, `gpu`, `auto`). There is no `process_type` field. Should I add a `process_type` field to configs, or should the sorting continue using `resource_type`?

---

## Verification Plan

### Manual Verification

1. **Reconnection test**: Start master + 2 workers, begin a task, kill one worker (Ctrl+C), verify it reconnects and gets work reallocated
2. **PIN display test**: During reconnection, verify the master dashboard shows the PIN for the reconnecting worker
3. **Double failure test**: Kill a task twice ('k' key), verify master properly reports the double failure and reallocates to spare/healthy node
4. **Benchmark scores test**: Set `cpu_name` to "i5 13500H" in worker's `devices.json`, verify predefined scores are sent in telemetry
5. **Device sorting test**: Connect 3 workers with different CPU names, verify they're sorted by `multi_core_score` (highest first) for task assignment
