import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from core import StatusDashboard

class DashboardHTTPRequestHandler(BaseHTTPRequestHandler):
    dashboard: StatusDashboard = None  # Class variable to be injected

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
    </style>
</head>
<body>
    <h1>Master Dashboard <span style="font-size: 14px; font-weight: normal; color: #808080;">(State: <span id="master-state" class="state">-</span> | Uptime: <span id="uptime">-</span>)</span></h1>
    
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
        async function fetchStatus() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();
                
                document.getElementById('master-state').textContent = data.master_state;
                document.getElementById('uptime').textContent = data.uptime;
                
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
