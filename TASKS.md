# Distributed Video Processing Prototype — Task List

Source spec: `target.txt` (master/worker distributed video split-execute-merge system).
This file is meant to be handed to individual agents/contributors as standalone
task packets — each task lists its own context, so no prior chat history is needed.

Legend: `[ ]` not started · `[~]` in progress · `[x]` done

---

## 0. Shared Context (read this first, applies to all tasks)

- System has one **master node** and multiple **worker nodes**, over a fixed TCP port: **`5000`** (used everywhere in this doc and code — do not invent alternate ports).
- Master is started with the IP addresses of all individual worker nodes.
- Each worker is started with the master node's address only.
- Config file (4 parameters only, for this prototype):
  - `split_command`
  - `execute_command`
  - `merge_command`
  - `max_nodes`
- Nodes are allocated tasks in the order the IP addresses were entered (no load-based sorting in this prototype).
- The number of IP addresses given to master must be **at least `max_nodes + 1`** (not exactly). The first `max_nodes` addresses in entry order are the active task nodes; every address after that is a spare/extra node, used in the order given, one at a time, as failures occur.
- This is a prototype build: keep code literal, minimal, and directly traceable line-by-line back to the English spec (see "Documentation & code style" below) so multiple agents working in parallel sessions don't diverge in interpretation.
- Throttling/failure detection is done by either:
  - monitoring **system-level** (not app-level) CPU usage on Linux, or
  - simply disconnecting a node from the network (for demo purposes).
- On detected failure: task reassigns to the extra node. **No task continuation or reconnection support** in this prototype — a failed task restarts fresh on the spare node.
- Progress states to expose (in this exact order of appearance), each tied to a node or the master:
  1. `splitting file` (master)
  2. `sending file` (master → worker, per worker)
  3. `executing` (per worker, with live running time)
  4. `receiving files` (master, per worker as it completes)
  5. `finished` (per worker, after its file is received)
  6. `merging files` (master, after all workers finished)
  7. `finished` (master, overall completion)

---

## Documentation & Code Style (applies to every task below)

To keep agents from conflicting when working in parallel sessions on this
prototype, all code and comments should be a **direct, literal English→Python
conversion** wherever possible:

- Language: **Python 3**, standard library only unless a task explicitly says otherwise (`socket`, `subprocess`, `threading`/`multiprocessing`, `time`, `configparser`/`json`, `psutil` only if Task 3.2 needs it).
- One function = one sentence/step from the spec. Function and variable names should echo the spec's own wording (e.g. `send_ready_message()`, `request_file()`, `run_execute_command()`, `mark_finished()`) rather than clever abstractions.
- No speculative generalization — don't add config options, sorting logic, retry/reconnect logic, or extra ports beyond what's listed in Phase 0 and each task. If the spec doesn't say it, don't build it.
- Every function gets a one-line docstring that is basically the spec sentence it implements, so any agent (or reviewer) can diff code against `target.txt` directly.
- Fixed values (port `5000`, the 4 config keys, the progress-state strings) should be defined once as constants in a shared module (e.g. `constants.py`) and imported everywhere — never re-typed/re-invented per file, to avoid drift between parallel sessions.
- Progress-state strings must be used **verbatim** as given in Phase 0's list (`"splitting file"`, `"sending file"`, `"executing"`, `"receiving files"`, `"finished"`, `"merging files"`) — no rewording.

---

## Phase 1 — Config & Bootstrapping

### Task 1.1: Config file loader
- [x] Define config file format (suggest: simple `key=value` or YAML/JSON) with exactly 4 fields: `split_command`, `execute_command`, `merge_command`, `max_nodes`.
- [x] Write parser/loader used by both master and worker startup.
- [x] Validate: error clearly if any of the 4 fields is missing or `max_nodes` isn't a positive integer.
- **Deliverable:** config module + sample `config.ini`/`config.json`.

### Task 1.2: Master node startup
- [x] CLI/entry point: master started with a list of IP addresses (one per line/arg) — this list must contain **at least `max_nodes + 1`** addresses. Reject startup with a clear error if fewer are given.
- [x] First `max_nodes` addresses (in the order entered) = active nodes; remaining addresses = spares, used one at a time in order as needed.
- [x] Master opens a listening socket on the fixed port `5000`.
- [x] Master loads config (Task 1.1).
- **Deliverable:** `master.py` + `constants.py` (port, config keys, progress-state strings) + arg parsing.

### Task 1.3: Worker node startup
- [x] CLI/entry point: worker started with only the master's IP address.
- [x] Worker connects to master on the fixed port `5000` (from shared `constants.py`) and idles until contacted.
- **Deliverable:** `worker.py` startup + arg parsing, importing `constants.py`.

---

## Phase 2 — File Splitting & Distribution

### Task 2.1: Video splitting
- [ ] On start, master runs `split_command` from config against the input video.
- [ ] Output must be named `part1.mkv`, `part2.mkv`, ... (one part per active/allocated node, NOT counting the spare unless it's activated).
- [ ] Progress state `splitting file` shown while this runs.
- **Deliverable:** split step wired into master's main flow.

### Task 2.2: Ready / file-request handshake
- [ ] Master sends a "ready" message to each of the `max_nodes` active nodes (in IP-list order) once its part is ready.
- [ ] Worker, on receiving "ready", requests the file from master.
- [ ] Master sends the file over the fixed-port (`5000`) file-transfer connection.
- [ ] Progress state `sending file` shown on master (per node) during transfer.
- **Deliverable:** ready/request/send protocol + basic file transfer function (reusable for Phase 4 too).

---

## Phase 3 — Execution

### Task 3.1: Worker execution
- [ ] On receiving its part file, worker runs `execute_command` (from config) against it.
- [ ] Progress state `executing` shown for that worker, along with **live/running elapsed time** for that node's execution.
- [ ] On completion, worker sends a "finished" message to master.
- **Deliverable:** execution runner + timer + finished-signal.

### Task 3.2: Failure / throttle detection (demo mechanism)
- [ ] Implement Linux system-level (not per-app) CPU usage monitor on worker or master-side polling.
- [ ] Implement manual "disconnect node from network" demo path (e.g., a script/toggle to simulate a node drop).
- [ ] On detecting throttle/disconnect for a task-node: mark that node's task as failed, and reassign the task to the **next unused spare address** in the list (in order) using the same ready→request→send→execute flow (Tasks 2.2, 3.1). No resume/reconnect logic needed — spare starts the task from scratch.
- [ ] If more failures occur than there are remaining spares, this is out of scope for the prototype — just log/fail clearly, no further handling needed.
- **Deliverable:** monitor module + failover trigger + reassignment logic.

---

## Phase 4 — Collecting Results & Merge

### Task 4.1: Result collection
- [ ] After a worker's "finished" message, master starts a file-transfer listener for that worker to send its output file back.
- [ ] Progress state `receiving files` shown per worker while its result is incoming.
- [ ] Once a worker's file is fully received, progress state for that worker becomes `finished`.
- **Deliverable:** receive-side file transfer + per-node status tracking.

### Task 4.2: Merge
- [ ] Once **all** worker nodes show `finished`, master runs `merge_command` from config to combine all output parts into one final file.
- [ ] Progress state `merging files` shown during this step.
- [ ] After merge completes, overall progress state becomes `finished`.
- **Deliverable:** merge step wired into master's main flow; end-of-run signal.

---

## Phase 5 — Progress Display / UI

### Task 5.1: Progress reporting surface
- [ ] Design a simple live status view (CLI table, log lines, or minimal web/dashboard — pick one) showing, per node: current state (`sending file` / `executing` / `receiving files` / `finished`) and running time where applicable.
- [ ] Show master-level states (`splitting file`, `merging files`, `finished`) separately from per-node rows.
- **Deliverable:** status display module consuming events from master.

---

## Phase 6 — Integration & Demo Run

### Task 6.1: End-to-end wiring
- [ ] Connect all phases into one runnable flow: config load → split → distribute → execute → collect → merge → done.
- [ ] Confirm task allocation strictly follows IP-address entry order, and confirm startup rejects address lists shorter than `max_nodes + 1`.
- **Deliverable:** working end-to-end script/entry point.

### Task 6.2: Failover demo script
- [ ] Prepare a repeatable demo: start master + at least `max_nodes + 1` workers (all listening on port `5000`), kick off a run, and mid-execution trigger either a CPU-throttle or disconnect on one active node.
- [ ] Confirm the next available spare node picks up that task and the run still reaches overall `finished`.
- **Deliverable:** demo runbook/script for the live prototype walkthrough.

### Task 6.3: Dry-run / test pass
- [ ] Run full pipeline on a sample video with `max_nodes = 2` and 4 total addresses given (2 active + 2 spares, to confirm "at least" is honored, not just "exactly") with no failure — confirm every progress state appears in the correct order.
- [ ] Run again forcing a failure on one node — confirm failover to the next spare + correct final merged output.
- **Deliverable:** test notes / checklist confirming both scenarios pass.

---

## Open Items / Not In Scope (per spec — do not implement unless asked)
- No task sorting/benchmarking-based allocation.
- No task continuation or reconnection after disconnection.
- No config parameters beyond the 4 listed.
