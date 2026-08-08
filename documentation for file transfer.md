# sender.py — File Transfer Node

One script, deployed identically on the Master and on every Worker. Every
node runs an HTTP listener on its configured port, and all transfers are
**pull-based** in both directions — nobody ever POSTs a file to anybody;
the receiving side always initiates a `GET`.

```
Worker  --GET /files-------------> Master   (what's staged for me?)
Worker  <--GET /files/<name>------ Master   (worker pulls each chunk into input/)

Master  --GET /files-------------> Worker   (what's ready to collect?)
Master  <--GET /files/<name>------ Worker   (master pulls each result into output/<node_id>/)
```

## Running it

**Master:**
```bash
python sender.py --role master \
  --base-dir /path/to/cluster-share \
  --port 8080 \
  --nodes-map nodes_map.json \
  --max-concurrent 16 \
  --token <shared-secret> \
  --log-file /var/log/lan-compute/master-sender.log
```

**Worker:**
```bash
python sender.py --role worker \
  --base-dir /path/to/worker-share \
  --port 8080 \
  --master-ip 192.168.43.1 \
  --max-concurrent 16 \
  --token <shared-secret> \
  --log-file ~/.lan-compute/worker-sender.log
```

`nodes_map.json` (Master only — see `nodes_map.example.json`) maps each
worker's LAN IP to the `node_id` used in the Master's `input/<node_id>/`
and `output/<node_id>/` folders. It's hot-reloaded on file-mtime change, so
the orchestrator can update it as workers pair/unpair without restarting
the file server.

## Endpoints

| Method | Path                  | Who calls it                     | What it does |
|---|---|---|---|
| GET  | `/files`              | The peer this node currently trusts | Lists files staged for that peer, with size + MD5 |
| GET  | `/files/<name>`       | Same                               | Streams the file; supports `Range` for resume |
| GET  | `/health`             | anyone                             | Liveness + current concurrency usage |
| POST | `/admin/cleanup`      | trusted peer only                  | Wipes per-node working files |
| POST | `/admin/concurrency`  | trusted peer only                  | `{"limit": N}` — rescale concurrency without restart |

Identity is resolved per-request from the peer's source IP (`identify_requester`):
- **Master** looks the IP up in the nodes map → serves from `input/<node_id>/`.
- **Worker** only accepts requests from `--master-ip` → serves from `output/`.
- If `--token` is set on both ends, every request must also carry a matching
  `X-Cluster-Token` header.

## Resumability

Downloads are served with `aiohttp.web.FileResponse`, which natively
implements `Range` / `If-Range` and returns `206 Partial Content`. The
included `resumable_download()` helper is the client-side half: it checks
how many bytes already exist on disk, resumes with `Range: bytes=<n>-`,
retries with exponential backoff on failure, and verifies the MD5 returned
by `/files` once the download completes. Import it from an orchestrator or
worker agent — it isn't wired to any route itself.

## Concurrency (scalable / descalable)

`--max-concurrent` (default 16) is enforced by `DynamicSemaphore`, not a
plain `asyncio.Semaphore` — its limit can be changed at runtime via
`POST /admin/concurrency {"limit": N}`. Raising the limit immediately wakes
queued requests; lowering it just stops admitting new ones until in-flight
transfers drain below the new ceiling, so nothing in progress gets killed.
Requests beyond the current limit get `503` with `Retry-After: 1` rather
than queuing indefinitely inside the server.

## Cleanup semantics

- **On startup**, and whenever `POST /admin/cleanup` is called, per-node
  working files are wiped.
- **On the Master**, `input/combined/` and `output/combined/` (the original
  unsplit input and the final combined output) are preserved by default.
  Pass `--no-preserve-combined` to wipe those too.
- **On a Worker**, `input/` and `output/` are wiped entirely — a worker
  holds no cross-task state.
- Call `/admin/cleanup` at both the start of a task (to clear the previous
  run's stale chunks) and after a successful completion, on every node.

## Logging

Every request logs method, path, caller IP, resolved status, latency, and
current concurrency (`in_use/limit`) through the middleware. Downloads log
whether they were a fresh pull or a resume (presence of `Range`) and the
file size. Rejections (unknown IP, wrong master, bad token, 503s) log at
`WARNING`. Unhandled exceptions log at `ERROR` with a full traceback.
Pass `--log-file` for a rotating file handler (25 MB × 5 backups) in
addition to console output; `--log-level DEBUG` for more detail.

## Reconnect / timeout / reallocation (`matchmaker.py`)

Node lifecycle:

```
IDLE --assign--> RUNNING --finish--> IDLE
                    |
              miss heartbeats
                    v
              UNREACHABLE --reconnects in time--> RUNNING/IDLE
                    |
        reallocation grace period expires
        (subtask reassigned to another node)
                    v
                 STALE --reconnects--> CLEANING --> IDLE
```

Every subtask assignment is a **lease**: `(subtask_id, lease_id, node_id)`.
`lease_id` increments every time a subtask is (re)assigned. This is the
fencing token that answers your question precisely:

- A node reconnecting reports the `(subtask_id, lease_id)` it's holding.
- `ClusterState.reconcile_reconnect()` compares that against the
  Master's current authoritative lease for that `subtask_id`.
  - **Same lease still authoritative** → `{"action": "resume"}`. The node
    (or orchestrator on its behalf) then calls `sync_files()`, which
    hash-checks every file the Master's `/files` listing reports against
    what's already on disk and only re-fetches what's missing or
    mismatched — nothing gets needlessly resent.
  - **Lease bumped / owned by someone else** → `{"action": "cleanup"}`.
    The node calls `POST /admin/cleanup`, wipes its working files, and
    reports itself `IDLE` (`ClusterState.mark_cleanup_done`).
- `check_timeouts()` is the periodic sweep (call it once per heartbeat
  interval): a node that misses heartbeats becomes `UNREACHABLE`; if it
  doesn't come back within `reallocation_grace_ms`, its subtask is handed
  to someone else via `plan_placement()` and the node is marked `STALE`.
  Only *then* is its old lease invalidated — a brief network blip on an
  Android hotspot won't trigger a reallocation, only a sustained one will.

## Scheduling algorithm

Both of the scenarios you described are the same underlying question —
*"given current node availability and speed, which placement finishes
soonest?"* — so there's one function, `plan_placement()`, used for both
initial job placement and mid-task reallocation. It's greedy
Earliest-Completion-Time list scheduling (LPT): sort fragments
largest-first, and for each one, assign it to whichever *eligible* node
(hard constraints already applied) has the earliest projected finish time
given what's already queued on it.

- **"4 nodes vs. pack 2 onto node1"**: this falls out automatically. If
  node1 is fast enough that running a second fragment on it still finishes
  before node4 (the slowest node) would finish its first, the algorithm
  packs node1 and skips node4 entirely — no special-casing needed. See
  `test_matchmaker.py`, Scenario 1: with scores `node1=100, node2=node3=40,
  node4=8`, the planner produces `node1: [2 frags], node2: [1], node3: [1],
  node4: []`.
- **"node4 times out — node1 (idle, finished) vs. node5 (fresh)"**: same
  function, called with the one orphaned fragment and the current set of
  idle/available nodes as candidates. `busy_until_estimate_ms` and a fixed
  provisioning overhead (binary not yet cached on a never-used node) are
  both factored into the projected finish time, so a fast-but-busy node1
  correctly loses to a slower-but-immediately-available node5 when that's
  actually faster overall (Scenario 2b in the test file).

Hard constraints (`min_ram_gb`, `min_vram_gb`, `min_battery_pct`) are an
**eligibility filter**, not a scoring penalty — a node that fails them is
never a candidate at all, regardless of how fast it is.

Node busy/idle state feeds into this in two ways: `NodeInfo.status` (from
heartbeats / `POST /admin/status`) determines eligibility, and
`busy_until_estimate_ms` (built from the node's current queue) is what
lets the planner correctly favor "pack onto a fast busy node anyway" or
correctly avoid it, per scenario above.

Run `python3 test_matchmaker.py` to see all four scenarios (including the
lease reconnect/stale case) executed and asserted against actual output.

## Task config: hard resource limits + per-architecture commands

See `task_config.example.json`. Two additions:

- `constraints.min_ram_gb` / `constraints.min_vram_gb` are **hard**
  limits — `matchmaker.is_eligible()` filters out any node below them
  before scoring even happens, they never just lower a node's rank.
- `execution.by_arch` maps an architecture key (matching the leaf folder
  names under `binaries/<app_id>/` — `x64_linux`, `x64_win`,
  `aarch64_linux`, `aarch64_macos`) to its own `binary_ref` + `args`.
  A node without an entry in `by_arch` falls back to `execution.default`.

## Anything else worth thinking about

A few things the spec doesn't cover yet that are worth deciding on before
this goes further:

- **Compute isn't checkpointed, only file transfer is.** `sync_files()`
  means a resumed node won't re-download what it already has, but if its
  *process* died mid-encode (not just the network), it has no way to
  resume the actual computation — it has to restart the fragment from
  scratch once it has the input again. Worth being explicit about this
  distinction in the worker agent's design, or accepting per-app
  checkpointing (e.g. ffmpeg's own resume support) where available.
- **Split-brain on results.** If a node reconnects and races the Master's
  reallocation — e.g. it finishes and calls `POST .../results` right as
  the grace period expires — the Master must reject a result whose
  `lease_id` doesn't match the current authoritative lease, or you'll get
  two nodes' output for one fragment. `SubtaskLease.status` plus checking
  the reported lease on every result submission covers this, but it needs
  to actually be enforced at that endpoint.
- **Worker-side self-abort.** Right now only the Master decides when a
  node is stale. A worker that can't reach the Master for a while (e.g.
  it, not the Master, is on the flaky link) is burning battery on work
  that's probably already been reassigned. A worker-side watchdog that
  self-aborts and goes idle after its own timeout — independent of
  waiting for a Master instruction — avoids that waste.
- **Lease persistence across a full process restart**, not just a network
  blip. If the worker agent itself crashes and restarts (not just loses
  connectivity), it needs to load its last-known `(subtask_id, lease_id)`
  from disk to go through the same reconcile flow, rather than assuming
  it's idle.
- **Tune the grace periods for the actual network.** Android hotspots
  reassign DHCP leases and drop briefly on lock-screen; a 60s reallocation
  grace period is a reasonable starting point but should be measured
  against real disconnect/reconnect timing on the target hardware rather
  than guessed.
- **Log every scheduling decision, not just failures.** `plan_placement()`
  and `_reallocate()` already log the outcome — consider also logging the
  *rejected* candidates and their projected finish times when debugging,
  since "why didn't it use node3" is the first question you'll ask when
  something looks wrong.

## Notes vs. the earlier draft

- Previously the server only implemented `GET`, but the API doc called for
  the Master to `POST` results uploads to itself — an inconsistent, one-
  directional design. This version is symmetric and pull-only in both
  directions, which also resolves the earlier question of how a Worker
  could receive a Master-initiated request without running its own HTTP
  server: now it does, by design.
- The URL prefix is always `/files`, independent of what the base directory
  is named on disk — no more mismatch between the doc's example paths and
  the server's actual routing.
- Connection accounting no longer monkey-patches `write_eof`; it's a
  straightforward acquire/release around the handler via `DynamicSemaphore`.
