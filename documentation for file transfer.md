# filetransfer.py — Design Document

Consolidates the previous `sender.py` (server-only) and the client-side
helpers (`resumable_download`, `sync_files`) into a single script that runs
on both the Master and every Worker. Direction is chosen per-invocation
via `--send` / `--receive`, not baked into a persistent daemon's role.

This document is scope-limited to file transfer. Scheduling, leases, and
timeout/reallocation stay in `matchmaker.py` as-is; this doc only defines
how the two systems hand off to each other (§6).

---

## 1. Core idea: direction, not role, picks the code path

Previously each node ran one long-lived process in a fixed role (`master`
serves, `worker` serves-and-pulls). Now **whichever side originates the
data listens; whichever side needs the data connects out and pulls it.**
That's true regardless of whether the listener happens to be the Master or
a Worker:

| Direction | Who listens (`--send`) | Who pulls (`--receive`) |
|---|---|---|
| Input chunk: Master → Worker | Master | Worker |
| Result: Worker → Master | Worker | Master |

So both roles need both flags — the CLI surface is the same shape, only
the argument list differs:

```
Master:
  filetransfer.py --send
  filetransfer.py --receive <nodenum>

Worker:
  filetransfer.py --receive
  filetransfer.py --send
```

The `<nodenum>` positional argument is what disambiguates a Master
invocation from a Worker invocation for `--receive` (a Worker only ever
has one counterpart — the Master — so it never needs to name who to pull
from). `--send` is disambiguated by which directory set gets exposed,
which is resolved from the existing `--role` / `--nodes-map` config each
node already carries (see §3) — not re-derived from the flags themselves.

## 2. Process lifecycle

- **`--send` is long-running.** It starts an HTTP listener, serves
  `/files` and `/files/<name>` (same semantics as the old `sender.py`:
  hash-listing, `Range`-based resume, concurrency-limited via
  `DynamicSemaphore`), and keeps running until:
  - an idle timeout elapses with no connections (`--idle-timeout`,
    default e.g. 120s) — the transfer window is expected to be short-lived
    per task, not indefinite, so this bounds how long a stale listener can
    sit open if the other side never shows up;
  - it receives an explicit `POST /admin/stop` from the peer it's serving
    (sent once that peer's `--receive` has confirmed completion); or
  - it's killed by the invoking orchestrator (worker agent / master
    process) as part of task cleanup.
- **`--receive` is one-shot.** It connects to a given `host:port`, lists
  what's available, hash-checks against anything already on disk (§4),
  pulls what's missing/mismatched (resumably), verifies, and exits with a
  status code (§7). It does not stay running.

This means a full input-then-output cycle for one subtask involves **four
separate `filetransfer.py` invocations** across the two nodes — two
listeners (started and later torn down) and two one-shot pulls — not two
persistent daemons.

## 3. Role and identity configuration (unchanged inputs, new usage)

Each node still needs to know its own role and, if it's the Master, its
node map — these are no longer used to pick *which* CLI flags exist (both
roles get both flags now), only to resolve **directories and peer
identity** once a `--send` or `--receive` actually runs:

- `--role {master,worker}` — required, as before.
- `--base-dir` — required, as before (contains `input/`, `output/`).
- `--nodes-map <path>` — required only for Master, only consulted during
  `--send` (to resolve an inbound requester's IP → `node_id`, exactly as
  in the old `sender.py`) and during `--receive <nodenum>` (to resolve
  `nodenum` → the target worker's IP, since the Master needs an address to
  connect *to*, not just an address to expect connections *from*).
- `--master-ip` — required only for Worker, used the same way it was
  before: to authenticate that an inbound `--send` connection, or the
  target of a `--receive`, is actually the Master.
- `--token` — unchanged, optional shared secret, checked on every request
  in both directions.

Directory resolution is unchanged from the previous design:

| Invocation | Serves / writes to |
|---|---|
| Master `--send` | serves `input/<node_id>/` (identity from nodes-map lookup on the inbound connection) |
| Worker `--receive` | writes into `input/` |
| Worker `--send` | serves `output/` |
| Master `--receive <nodenum>` | writes into `output/<nodenum>/` |

## 4. Resume / no-resend behavior (unchanged semantics)

`--receive` always does a hash-check-first sync before transferring
anything, same as the previous `sync_files()`:

1. `GET /files` from the target → listing with `name`, `size_bytes`, `md5`.
2. For each entry, if a local file of the same name already matches on
   size + MD5, skip it entirely — no request made.
3. Anything missing or mismatched is fetched via a resumable download
   (`Range: bytes=<local-size>-` if a partial file exists, verified against
   the listed MD5 once complete, retried with backoff on failure).

This is why a `--receive` that gets interrupted and re-run later doesn't
re-pull anything it already finished — it's a property of `--receive`
itself, independent of which side is Master or Worker.

## 5. Port negotiation over the control-plane channel

There is no more static, pre-shared port. Every `--send` binds an
ephemeral or configured port, and that port has to reach the other side
through the existing Control Plane API (the FastAPI/WebSocket channel from
the API doc), not through `filetransfer.py` itself. Two new small control
messages are needed — one per direction:

### 5a. Master → Worker (input chunk delivery)

1. Master starts `filetransfer.py --send --port <p>` (or `--port 0` for an
   OS-assigned ephemeral port, which it then reads back).
2. Master sends a control-plane message to the target worker — extends
   the existing Task Dispatch payload (§2.7 of the API doc) rather than
   inventing a new RPC:
   ```json
   {
     "subtask_id": "sub-1",
     "transfer": {
       "host": "192.168.43.1",
       "port": 51234,
       "token": "…",
       "expires_at_ms": 1754643300000
     },
     "...": "...rest of existing TaskDispatch fields unchanged..."
   }
   ```
3. Worker runs `filetransfer.py --receive --host 192.168.43.1 --port 51234
   --token …`.
4. On completion, Worker's `--receive` process (or the worker agent
   wrapping it) sends `POST /admin/stop` to the Master's listener, then
   reports dispatch-ack / transfer-complete over the control plane as
   already specified.

### 5b. Worker → Master (result collection)

1. Once a Worker finishes a subtask, it starts `filetransfer.py --send
   --port <p>` (again, ephemeral-by-default).
2. Worker reports readiness over the control plane — extends the existing
   Task Result Reporting call (§2.8) with the same `transfer` block instead
   of a bare `output_url`:
   ```json
   {
     "subtask_id": "sub-1",
     "success": true,
     "transfer": {
       "host": "192.168.43.102",
       "port": 51890,
       "token": "…",
       "expires_at_ms": 1754643900000
     },
     "...": "...rest of existing SubTaskResult fields unchanged..."
   }
   ```
3. Master runs `filetransfer.py --receive <nodenum> --host 192.168.43.102
   --port 51890 --token …` (resolving `<nodenum>` via the nodes-map is what
   tells it *which* `output/<nodenum>/` to write into — the `--host`/`--port`
   here come from the control-plane message, not from the nodes-map, since
   they're ephemeral per-transfer).
4. Master's `--receive` calls `POST /admin/stop` on the Worker's listener
   once done, and only then does the existing MD5 integrity check /
   `ReportSubTaskResult` acknowledgment flow proceed as already specified.

### Why ephemeral ports instead of a fixed one per node

A fixed, pre-shared port (the old model) meant the listener had to be
always-on, which is exactly what this redesign moves away from. Binding
a fresh ephemeral port per `--send` invocation, and pushing it over a
channel that's authenticated and already open (the control plane), avoids
needing any change to firewall/port-forwarding assumptions and avoids the
class of bug where a long-dead listener's port is still advertised as
live. The cost is that every transfer now has one extra control-plane
round trip before it can start — worth calling out as a latency trade-off,
though on a LAN it's negligible next to actual file-transfer time.

## 6. Interaction with lease/timeout state (`matchmaker.py`)

File transfer must not blindly proceed if the underlying subtask lease is
no longer valid — this is where the two modules meet:

- Before Master starts a `--send` (input) or issues the `--receive
  <nodenum>` (result pull), it should confirm the relevant lease is still
  `ASSIGNED`/`RUNNING` and owned by the node in question
  (`ClusterState.leases[subtask_id]`). If a reallocation happened in the
  window between "worker reported ready" and "master got around to
  pulling," the pull should be aborted rather than writing a stale node's
  output into `output/<nodenum>/`.
- Symmetrically, when a Worker reconnects after a gap and
  `reconcile_reconnect()` returns `{"action": "cleanup"}`, the Worker
  should *not* invoke `--receive` at all — it runs the existing
  `POST /admin/cleanup` cleanup path instead, then goes back to `IDLE`.
  `--receive` should only ever be invoked once reconnection has been
  reconciled as `"resume"`.
- This means the control-plane messages in §5 should each carry the
  `lease_id` alongside `subtask_id`, and both `--send` and `--receive`
  should refuse to proceed (log + non-zero exit, see §7) if asked to act
  on a `lease_id` that doesn't match what the corresponding heartbeat/task
  dispatch most recently confirmed. This is a cheap, local double-check on
  top of the Master's authoritative lease bookkeeping — belt and braces
  against a stale message arriving late.

## 7. Exit codes for `--receive` (needed since it's now a one-shot process an orchestrator scripts around)

| Code | Meaning |
|---|---|
| 0 | Completed — at least one file transferred |
| 1 | Completed — nothing to do, everything already hash-valid on disk |
| 2 | Retryable failure (connection error, listener not up yet, timeout) — caller should retry with backoff |
| 3 | Auth failure (bad token / untrusted peer IP) — not retryable without operator intervention |
| 4 | Lease mismatch — the orchestrator should treat this as "stale, go clean up," not "retry" |

`--send` doesn't need an analogous table since it's long-running; its
outcomes are all reflected in its log output and in whether a completion
was acknowledged via `/admin/stop` before its idle timeout.

## 8. What stays the same

- `/files`, `/files/<name>`, `/admin/cleanup`, `/admin/concurrency`,
  `/admin/status` endpoint semantics, request/response shapes, and the
  `DynamicSemaphore` concurrency model are unchanged from the previous
  `sender.py` — this refactor changes *when and how the listener is
  started*, not what it does once it's up.
- Directory layout, hash-verification, and cleanup-preserves-`combined`
  semantics are unchanged.
- `matchmaker.py` is unchanged; §6 only adds a couple of fields
  (`lease_id`) to messages it already informs.

## 9. Open questions to settle before implementation

- **Idle-timeout value for `--send`.** Too short risks the listener dying
  before a slow peer connects (especially over a flaky hotspot); too long
  leaves an open port around longer than needed. Worth tuning against the
  same real-world reconnect timing mentioned in the matchmaker doc, not
  guessed.
- **What happens if `--send`'s idle timeout fires while a transfer is
  genuinely still in progress** (e.g. a very large file on a slow link) —
  the idle timer should reset on any active connection, not just on
  process start, but this needs to be explicit so it isn't a corner case
  that surfaces only under load.
- **Multiple concurrent `--receive` targets on the Master.** If several
  workers finish around the same time, the Master will want to run several
  `--receive <nodenum>` invocations concurrently rather than serially —
  worth deciding whether that's the orchestrator's job (spawn N processes)
  or whether `filetransfer.py` should grow a batch mode later.
