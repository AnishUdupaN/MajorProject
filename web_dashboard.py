import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from core import StatusDashboard

class DashboardHTTPRequestHandler(BaseHTTPRequestHandler):
    dashboard: StatusDashboard = None  # Class variable to be injected

    def do_POST(self):
        if self.path == "/api/input":
            content_length = int(self.headers.get('Content-Length', 0))
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

    def do_GET(self):
        if self.path == "/api/status":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            
            with self.dashboard.lock:
                import time
                uptime = "-"
                if self.dashboard.task_start_time is not None:
                    uptime = f"{time.time() - self.dashboard.task_start_time:.1f}s"
                data = {
                    "master_state": self.dashboard.master_state,
                    "node_states": self.dashboard.node_states,
                    "messages": self.dashboard.messages[-10:],
                    "configs": self.dashboard.available_configs,
                    "selected_config": self.dashboard.get_selected_config(),
                    "active_prompt": self.dashboard.active_prompt,
                    "uptime": uptime,
                }
            self.wfile.write(json.dumps(data).encode("utf-8"))
        elif self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            
            html = """<!DOCTYPE html>
<html>
<head>
    <title>Master Dashboard</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background: #1e1e1e; color: #d4d4d4; padding: 20px; }
        h1 { color: #569cd6; }
        .state { font-weight: bold; color: #4ec9b0; }
        table { border-collapse: collapse; width: 100%; margin-top: 20px; }
        th, td { border: 1px solid #333; padding: 10px; text-align: left; }
        th { background: #2d2d2d; color: #9cdcfe; }
        .messages { margin-top: 20px; background: #252526; padding: 10px; border: 1px solid #333; height: 150px; overflow-y: scroll; }
        .disconnected { color: #f44747; }
        .finished { color: #6a9955; }
        .executing { color: #ce9178; }
        .prompt-box { background: #252526; border: 1px solid #569cd6; padding: 15px; margin-bottom: 20px; border-radius: 4px; }
        .btn { background: #0e639c; color: white; border: none; padding: 8px 16px; cursor: pointer; border-radius: 2px; margin-right: 10px; }
        .btn:hover { background: #1177bb; }
        .btn-secondary { background: #3c3c3c; }
        .btn-secondary:hover { background: #4d4d4d; }
    </style>
</head>
<body>
    <h1>Master Dashboard <span style="font-size: 14px; font-weight: normal; color: #808080;">(State: <span id="master-state" class="state">-</span> | Uptime: <span id="uptime">-</span>)</span></h1>
    
    <div id="prompt-container" class="prompt-box" style="display: none;">
        <div id="prompt-text" style="margin-bottom: 15px; font-weight: bold; white-space: pre-wrap;"></div>
        <div id="prompt-buttons"></div>
    </div>

    <div id="configs-container" style="display: none; margin-bottom: 20px;">
        <strong>Available Configs:</strong> <span id="configs-list"></span>
    </div>

    <table>
        <thead>
            <tr>
                <th>Node IP</th>
                <th>Device ID</th>
                <th>File</th>
                <th>Status</th>
                <th>Progress</th>
                <th>Flags</th>
            </tr>
        </thead>
        <tbody id="nodes-tbody">
        </tbody>
    </table>

    <div class="messages" id="messages-container"></div>

    <script>
        async function sendInput(answer) {
            try {
                await fetch('/api/input', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({answer: answer})
                });
                document.getElementById('prompt-container').style.display = 'none';
            } catch (err) {
                console.error("Failed to send input", err);
            }
        }

        async function fetchStatus() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();
                
                document.getElementById('master-state').textContent = data.master_state;
                document.getElementById('uptime').textContent = data.uptime;
                
                if (data.active_prompt) {
                    document.getElementById('prompt-container').style.display = 'block';
                    document.getElementById('prompt-text').textContent = data.active_prompt;
                    
                    const buttonsDiv = document.getElementById('prompt-buttons');
                    if (data.master_state === 'config selection') {
                        buttonsDiv.innerHTML = `
                            <button class="btn btn-secondary" onclick="sendInput('up')">↑ Move Up (w)</button>
                            <button class="btn btn-secondary" onclick="sendInput('down')">↓ Move Down (s)</button>
                            <button class="btn" onclick="sendInput('confirm')">✓ Select Config (y)</button>
                            <button class="btn btn-secondary" onclick="sendInput('quit')">Quit (q)</button>
                        `;
                    } else {
                        buttonsDiv.innerHTML = `
                            <button class="btn" onclick="sendInput('confirm')">Yes (y)</button>
                            <button class="btn btn-secondary" onclick="sendInput('decline')">No (n)</button>
                            <button class="btn btn-secondary" onclick="sendInput('quit')">Quit (q)</button>
                        `;
                    }
                } else {
                    document.getElementById('prompt-container').style.display = 'none';
                }
                
                if (data.configs && data.configs.length > 0) {
                    document.getElementById('configs-container').style.display = 'block';
                    document.getElementById('configs-list').innerHTML = data.configs.map(c => 
                        c === data.selected_config ? `<span style="color:#ce9178"><b>${c}</b></span>` : c
                    ).join(' | ');
                } else {
                    document.getElementById('configs-container').style.display = 'none';
                }
                
                const tbody = document.getElementById('nodes-tbody');
                tbody.innerHTML = '';
                
                for (const [ip, state] of Object.entries(data.node_states)) {
                    const tr = document.createElement('tr');
                    
                    let statusColor = '';
                    if (state.state === 'disconnected' || state.state === 'failed') statusColor = 'disconnected';
                    else if (state.state === 'finished' || state.state === 'Finished') statusColor = 'finished';
                    else if (state.executing || state.receiving || state.sending) statusColor = 'executing';
                    
                    let flags = [];
                    if (state.receiving) flags.push('RCV');
                    if (state.executing) flags.push('EXE');
                    if (state.sending) flags.push('SND');
                    
                    let progress = '-';
                    if (state.pct !== null) {
                        progress = `${state.pct.toFixed(1)}% (ETA: ${state.eta || '-'})`;
                    } else if (state.elapsed !== null) {
                        progress = `${state.elapsed.toFixed(1)}s`;
                    }
                    
                    tr.innerHTML = `
                        <td>${ip}</td>
                        <td>${state.node_id}</td>
                        <td>${state.filename}</td>
                        <td class="${statusColor}">${state.state}</td>
                        <td>${progress}</td>
                        <td>${flags.join(', ') || '-'}</td>
                    `;
                    tbody.appendChild(tr);
                }
                
                const msgs = document.getElementById('messages-container');
                msgs.innerHTML = data.messages.map(m => `<div>[${new Date(m.timestamp * 1000).toLocaleTimeString()}] ${m.text}</div>`).join('');
                
            } catch (err) {
                console.error("Failed to fetch status", err);
            }
        }
        
        setInterval(fetchStatus, 1000);
        fetchStatus();
    </script>
</body>
</html>"""
            self.wfile.write(html.encode("utf-8"))
        else:
            self.send_error(404, "Not Found")

    def log_message(self, format, *args):
        pass  # Suppress HTTP logging

def start_web_server(dashboard: StatusDashboard, port: int = 8080):
    DashboardHTTPRequestHandler.dashboard = dashboard
    server = HTTPServer(("0.0.0.0", port), DashboardHTTPRequestHandler)
    
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
