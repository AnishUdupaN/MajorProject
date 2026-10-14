"""Web dashboard for the master node — serves a live status page and accepts user input."""

import json
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from core import StatusDashboard


class DashboardHTTPRequestHandler(BaseHTTPRequestHandler):
    dashboard: StatusDashboard = None  # Class variable to be injected

    # ------------------------------------------------------------------ POST --
    def do_POST(self):
        if self.path == "/api/input":
            content_length = int(self.headers.get("Content-Length", 0))
            post_data = self.rfile.read(content_length)
            try:
                data = json.loads(post_data)
                answer = data.get("answer")
                if answer and self.dashboard:
                    self.dashboard.submit_input(answer)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "ok"}).encode("utf-8"))
            except Exception:
                self.send_error(400, "Bad Request")
        else:
            self.send_error(404, "Not Found")

    # ------------------------------------------------------------------- GET --
    def do_GET(self):
        if self.path == "/api/status":
            self._serve_status()
        elif self.path == "/" or self.path == "/index.html":
            self._serve_html()
        else:
            self.send_error(404, "Not Found")

    # --------------------------------------------------------- API: /api/status
    def _serve_status(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()

        now = time.time()
        with self.dashboard.lock:
            uptime = None
            if self.dashboard.task_start_time is not None:
                uptime = now - self.dashboard.task_start_time

            # Serialize messages, computing time_left from expire_at
            messages_out = []
            for m in self.dashboard.messages:
                if "expire_at" in m:
                    tl = max(0.0, m["expire_at"] - now)
                else:
                    tl = max(0.0, m.get("time_left", 0.0))
                messages_out.append({"text": m["text"], "time_left": tl})

            data = {
                "master_state": self.dashboard.master_state,
                "session_pin": getattr(self.dashboard, "session_pin", None),
                "node_states": self.dashboard.node_states,
                "messages": messages_out[-20:],
                "configs": self.dashboard.available_configs,
                "selected_config": self.dashboard.get_selected_config(),
                "selected_config_index": self.dashboard.selected_config_index,
                "active_prompt": self.dashboard.active_prompt,
                "uptime": uptime,
                "server_time": now,
            }

        self.wfile.write(json.dumps(data).encode("utf-8"))

    # --------------------------------------------------------- HTML dashboard --
    def _serve_html(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(HTML_PAGE.encode("utf-8"))

    def log_message(self, format, *args):
        pass  # Suppress HTTP access logging


# ---------------------------------------------------------------------------
# Self-contained HTML/JS/CSS page
# ---------------------------------------------------------------------------
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Master Dashboard</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  :root {
    --bg:         #1e1e2e;
    --surface:    #24273a;
    --surface2:   #2a2d3e;
    --border:     #3b3f51;
    --text:       #cdd6f4;
    --subtext:    #a6adc8;
    --blue:       #89b4fa;
    --green:      #a6e3a1;
    --yellow:     #f9e2af;
    --red:        #f38ba8;
    --orange:     #fab387;
    --mauve:      #cba6f7;
    --teal:       #94e2d5;
    --accent:     #89b4fa;
  }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: "JetBrains Mono", "Cascadia Code", "Fira Code", ui-monospace, monospace;
    font-size: 13px;
    line-height: 1.5;
    padding: 16px;
  }

  h1 {
    font-size: 18px;
    font-weight: 700;
    color: var(--blue);
    margin-bottom: 4px;
  }

  .header-row {
    display: flex;
    align-items: baseline;
    gap: 12px;
    margin-bottom: 16px;
    flex-wrap: wrap;
  }
  .header-meta {
    color: var(--subtext);
    font-size: 12px;
  }
  .state-badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 20px;
    background: var(--surface2);
    border: 1px solid var(--border);
    font-weight: 600;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  /* ---- connection indicator ---- */
  .conn-pill {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 2px 10px;
    border-radius: 20px;
    background: var(--surface2);
    border: 1px solid var(--border);
    font-size: 11px;
    color: var(--subtext);
  }
  .conn-dot {
    width: 7px; height: 7px;
    border-radius: 50%;
    background: var(--red);
    transition: background 0.3s;
  }
  .conn-dot.ok { background: var(--green); }

  /* ---- prompt box ---- */
  .prompt-box {
    background: var(--surface);
    border: 1px solid var(--blue);
    border-radius: 6px;
    padding: 14px 16px;
    margin-bottom: 14px;
  }
  .prompt-text {
    white-space: pre-wrap;
    font-weight: 600;
    color: var(--text);
    margin-bottom: 12px;
  }
  .btn-row { display: flex; flex-wrap: wrap; gap: 8px; }

  button {
    font-family: inherit;
    font-size: 12px;
    font-weight: 600;
    padding: 6px 16px;
    border: 1px solid var(--border);
    border-radius: 4px;
    cursor: pointer;
    transition: opacity 0.15s;
  }
  button:hover { opacity: 0.85; }
  button:active { opacity: 0.7; }
  .btn-primary   { background: var(--blue);   color: var(--bg); border-color: var(--blue); }
  .btn-secondary { background: var(--surface2); color: var(--text); }
  .btn-danger    { background: var(--red);    color: var(--bg); border-color: var(--red); }

  /* ---- configs ---- */
  .configs-row {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 10px 14px;
    margin-bottom: 14px;
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    align-items: center;
  }
  .configs-label { color: var(--subtext); font-size: 11px; margin-right: 4px; }
  .cfg-chip {
    padding: 2px 10px;
    border-radius: 20px;
    border: 1px solid var(--border);
    background: var(--surface2);
    cursor: pointer;
    font-size: 12px;
    color: var(--subtext);
    transition: border-color 0.15s, color 0.15s;
  }
  .cfg-chip:hover { border-color: var(--accent); color: var(--text); }
  .cfg-chip.selected { border-color: var(--orange); color: var(--orange); font-weight: 700; }

  /* ---- nodes table ---- */
  .table-wrap {
    overflow-x: auto;
    margin-bottom: 14px;
    border-radius: 6px;
    border: 1px solid var(--border);
  }
  table {
    width: 100%;
    border-collapse: collapse;
    min-width: 640px;
  }
  thead th {
    background: var(--surface2);
    color: var(--blue);
    font-weight: 600;
    text-align: left;
    padding: 8px 12px;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  tbody tr:nth-child(even) { background: var(--surface); }
  tbody tr:nth-child(odd)  { background: var(--bg); }
  tbody tr:hover { background: var(--surface2); }
  tbody td {
    padding: 7px 12px;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  tbody tr:last-child td { border-bottom: none; }

  /* status colours */
  .s-disconnected, .s-failed, .s-killed { color: var(--red);    font-weight: 600; }
  .s-finished                            { color: var(--green);  font-weight: 600; }
  .s-executing                           { color: var(--yellow); font-weight: 600; }
  .s-sending, .s-receiving              { color: var(--orange); font-weight: 600; }
  .s-splitting, .s-merging              { color: var(--mauve);  font-weight: 600; }
  .s-idle                               { color: var(--subtext);}
  .s-reassigned                         { color: var(--teal);   font-weight: 600; }

  /* flag badges */
  .flag {
    display: inline-block;
    width: 20px; height: 20px;
    line-height: 20px;
    text-align: center;
    border-radius: 3px;
    font-size: 10px;
    font-weight: 700;
    margin-right: 2px;
    background: var(--surface2);
    color: var(--subtext);
  }
  .flag.active { background: var(--blue); color: var(--bg); }

  /* ---- messages ---- */
  .messages-wrap {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 10px 14px;
  }
  .messages-title {
    color: var(--blue);
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 6px;
  }
  .messages-list {
    max-height: 180px;
    overflow-y: auto;
  }
  .msg-item {
    display: flex;
    gap: 8px;
    align-items: flex-start;
    padding: 3px 0;
    border-bottom: 1px solid var(--border);
    font-size: 12px;
  }
  .msg-item:last-child { border-bottom: none; }
  .msg-time { color: var(--subtext); flex-shrink: 0; }
  .msg-text { color: var(--text); flex: 1; }
  .msg-ttl  { color: var(--subtext); flex-shrink: 0; font-size: 11px; }
  .no-messages { color: var(--subtext); font-size: 12px; font-style: italic; }

  /* ---- footer ---- */
  .footer {
    margin-top: 10px;
    color: var(--subtext);
    font-size: 11px;
    text-align: right;
  }

  /* scrollbar */
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: var(--surface); }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
</style>
</head>
<body>

<!-- ===== HEADER ===== -->
<div class="header-row">
  <h1>Master Dashboard</h1>
  <span class="state-badge" id="state-badge">—</span>
  <span class="state-badge" id="pin-badge" style="display:none; color:var(--yellow); border-color:var(--yellow)">PIN: <span id="pin-val">—</span></span>
  <span class="header-meta">Uptime: <span id="uptime">—</span></span>
  <span class="conn-pill" id="conn-pill">
    <span class="conn-dot" id="conn-dot"></span>
    <span id="conn-label">Connecting…</span>
  </span>
</div>

<!-- ===== PROMPT BOX ===== -->
<div id="prompt-box" class="prompt-box" style="display:none">
  <div id="prompt-text" class="prompt-text"></div>
  <div id="btn-row" class="btn-row"></div>
</div>

<!-- ===== CONFIG SELECTION ===== -->
<div id="configs-row" class="configs-row" style="display:none">
  <span class="configs-label">CONFIGS:</span>
  <span id="configs-list"></span>
</div>

<!-- ===== NODE TABLE ===== -->
<div class="table-wrap">
  <table>
    <thead>
      <tr>
        <th>Node / IP</th>
        <th>Device</th>
        <th>State</th>
        <th>File</th>
        <th>Flags</th>
        <th>Progress</th>
      </tr>
    </thead>
    <tbody id="nodes-tbody"></tbody>
  </table>
</div>

<!-- ===== MESSAGES ===== -->
<div class="messages-wrap">
  <div class="messages-title">Messages &amp; Alerts</div>
  <div class="messages-list" id="messages-list">
    <div class="no-messages">No messages yet.</div>
  </div>
</div>

<div class="footer">Last updated: <span id="last-updated">—</span></div>

<script>
'use strict';

// ── state ──────────────────────────────────────────────────────────────────
let lastData = null;
let fetchFailed = 0;

// ── helpers ────────────────────────────────────────────────────────────────
function fmtUptime(seconds) {
  if (seconds == null) return '—';
  const s = Math.floor(seconds);
  const m = Math.floor(s / 60), rem = s % 60;
  const h = Math.floor(m / 60), remM = m % 60;
  if (h > 0) return `${h}h ${remM}m ${rem}s`;
  if (m > 0) return `${m}m ${rem}s`;
  return `${rem}s`;
}

function stateCls(state) {
  const s = (state || '').toLowerCase();
  if (s === 'disconnected' || s === 'failed') return 's-disconnected';
  if (s === 'killed')                         return 's-killed';
  if (s === 'finished')                       return 's-finished';
  if (s === 'executing')                      return 's-executing';
  if (s === 'sending file' || s === 'sending')  return 's-sending';
  if (s === 'receiving files' || s === 'receiving') return 's-receiving';
  if (s === 'splitting file')                 return 's-splitting';
  if (s === 'merging files')                  return 's-merging';
  if (s === 'idle')                           return 's-idle';
  if (s === 'reassigned')                     return 's-reassigned';
  return '';
}

function esc(str) {
  return String(str ?? '—')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function flag(letter, active) {
  return `<span class="flag${active ? ' active' : ''}" title="${letter}">${letter}</span>`;
}

// ── send user input ────────────────────────────────────────────────────────
async function sendInput(answer) {
  try {
    const r = await fetch('/api/input', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ answer })
    });
    if (!r.ok) console.error('Input rejected:', r.status);
  } catch (e) {
    console.error('sendInput failed:', e);
  }
}

// ── render ─────────────────────────────────────────────────────────────────
function render(data) {
  lastData = data;
  const state = data.master_state || '—';

  // -- header
  const badge = document.getElementById('state-badge');
  badge.textContent = state.toUpperCase();
  badge.style.color = stateCls(state).startsWith('s-') ? '' : '';  // keep neutral for now
  document.getElementById('uptime').textContent = fmtUptime(data.uptime);

  const pinBadge = document.getElementById('pin-badge');
  if (data.session_pin) {
    pinBadge.style.display = '';
    document.getElementById('pin-val').textContent = data.session_pin;
  } else {
    pinBadge.style.display = 'none';
  }

  // -- prompt box
  const promptBox  = document.getElementById('prompt-box');
  const promptText = document.getElementById('prompt-text');
  const btnRow     = document.getElementById('btn-row');

  if (data.active_prompt) {
    promptBox.style.display = '';
    promptText.textContent = data.active_prompt;
    btnRow.innerHTML = buildButtons(state);
  } else {
    promptBox.style.display = 'none';
    promptText.textContent = '';
    btnRow.innerHTML = '';
  }

  // -- configs
  const cfgRow  = document.getElementById('configs-row');
  const cfgList = document.getElementById('configs-list');
  if (data.configs && data.configs.length > 0) {
    cfgRow.style.display = '';
    cfgList.innerHTML = data.configs.map((c, i) => {
      const sel = (i === data.selected_config_index);
      return `<span class="cfg-chip${sel ? ' selected' : ''}" onclick="selectConfig(${i})">${esc(c)}</span>`;
    }).join('');
  } else {
    cfgRow.style.display = 'none';
    cfgList.innerHTML = '';
  }

  // -- nodes table
  const tbody = document.getElementById('nodes-tbody');
  const entries = Object.entries(data.node_states || {});
  if (entries.length === 0) {
    tbody.innerHTML = `<tr><td colspan="6" style="color:var(--subtext);text-align:center;padding:14px">Waiting for worker connections…</td></tr>`;
  } else {
    tbody.innerHTML = entries.map(([ip, s]) => {
      const displayIp = (s.hostname && s.username)
        ? `${ip}<br><small style="color:var(--subtext)">${esc(s.username)}@${esc(s.hostname)}</small>`
        : esc(ip);

      let progress = '—';
      if (s.pct != null) {
        progress = `${Number(s.pct).toFixed(1)}%`;
        if (s.eta) progress += ` ETA ${esc(s.eta)}`;
      } else if (s.elapsed != null) {
        progress = fmtUptime(s.elapsed);
      }

      const cls = stateCls(s.state);
      const connIcon = s.connected === false
        ? '<span style="color:var(--red)">✗</span>'
        : '<span style="color:var(--green)">✓</span>';

      return `<tr>
        <td>${displayIp} ${connIcon}</td>
        <td>${esc(s.node_id)}</td>
        <td class="${cls}">${esc(s.state)}</td>
        <td>${esc(s.filename)}</td>
        <td>${flag('R', s.receiving)}${flag('E', s.executing)}${flag('S', s.sending)}</td>
        <td>${progress}</td>
      </tr>`;
    }).join('');
  }

  // -- messages
  const msgList = document.getElementById('messages-list');
  const msgs = data.messages || [];
  if (msgs.length === 0) {
    msgList.innerHTML = '<div class="no-messages">No messages yet.</div>';
  } else {
    const wasAtBottom = msgList.scrollHeight - msgList.clientHeight <= msgList.scrollTop + 4;
    msgList.innerHTML = msgs.map(m => {
      const tl = Math.ceil(m.time_left || 0);
      const ts = new Date((data.server_time - (m.time_left || 0)) * 1000)
        .toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
      return `<div class="msg-item">
        <span class="msg-time">${ts}</span>
        <span class="msg-text">${esc(m.text)}</span>
        <span class="msg-ttl">${tl}s</span>
      </div>`;
    }).join('');
    // Auto-scroll to bottom if we were already at bottom
    if (wasAtBottom) msgList.scrollTop = msgList.scrollHeight;
  }

  // -- last updated
  document.getElementById('last-updated').textContent = new Date().toLocaleTimeString();
}

function buildButtons(state) {
  const s = (state || '').toLowerCase();
  if (s === 'config selection') {
    return `
      <button class="btn-secondary" onclick="sendInput('up')">↑ Up (W)</button>
      <button class="btn-secondary" onclick="sendInput('down')">↓ Down (S)</button>
      <button class="btn-primary"   onclick="sendInput('confirm')">✓ Select</button>
      <button class="btn-danger"    onclick="sendInput('quit')">✕ Quit</button>`;
  }
  if (s === 'device discovery') {
    return `
      <button class="btn-primary"   onclick="sendInput('confirm')">▶ Continue</button>
      <button class="btn-danger"    onclick="sendInput('quit')">✕ Quit</button>`;
  }
  if (s === 'finished') {
    return `
      <button class="btn-primary"   onclick="sendInput('confirm')">↺ Process Another</button>
      <button class="btn-danger"    onclick="sendInput('decline')">✕ Exit</button>`;
  }
  // Generic yes/no for any other prompt
  return `
    <button class="btn-primary"   onclick="sendInput('confirm')">✓ Yes</button>
    <button class="btn-secondary" onclick="sendInput('decline')">✕ No</button>
    <button class="btn-danger"    onclick="sendInput('quit')">⏻ Quit</button>`;
}

// Allow clicking config chips to jump selection directly
async function selectConfig(index) {
  if (!lastData) return;
  const current = lastData.selected_config_index ?? 0;
  const total   = (lastData.configs || []).length;
  if (total === 0) return;
  // Navigate with up/down deltas to land on desired index
  const delta = ((index - current) % total + total) % total;
  for (let i = 0; i < delta; i++) {
    await sendInput('down');
    await new Promise(r => setTimeout(r, 60));
  }
}

// ── poll loop ──────────────────────────────────────────────────────────────
async function poll() {
  const dot   = document.getElementById('conn-dot');
  const label = document.getElementById('conn-label');
  try {
    const res = await fetch('/api/status');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    fetchFailed = 0;
    dot.classList.add('ok');
    label.textContent = 'Connected';
    render(data);
  } catch (e) {
    fetchFailed++;
    dot.classList.remove('ok');
    label.textContent = `Connection lost (${fetchFailed})`;
    console.warn('Poll failed:', e);
  }
}

// Initial poll + interval
poll();
setInterval(poll, 1000);
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Server startup
# ---------------------------------------------------------------------------

def start_web_server(dashboard: StatusDashboard, port: int = 8080) -> HTTPServer:
    """Start the HTTP dashboard server in a background daemon thread."""
    DashboardHTTPRequestHandler.dashboard = dashboard
    server = HTTPServer(("0.0.0.0", port), DashboardHTTPRequestHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
