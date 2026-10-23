"""Live CLI and Web Status Dashboard for the master orchestrator."""

from __future__ import annotations

import sys
import threading
import time
from typing import Any, Dict

from core.constants import (
    PROGRESS_MERGING_FILES,
    PROGRESS_SPLITTING_FILE,
)

_LOG_LOCK = threading.Lock()


def printlog(st: str) -> None:
    """Thread-safe append-only logging to logs.txt."""
    with _LOG_LOCK:
        with open("logs.txt", "a+") as f:
            f.write(st + "\n")


class StatusDashboard:
    """Live CLI status view tracking master and per-node states."""

    def __init__(self, master_state: str = "initializing") -> None:
        self.lock = threading.RLock()
        self.master_state = master_state
        self.splitting_flag = False
        self.merging_flag = False
        self.node_states: Dict[str, Dict[str, Any]] = {}
        self.messages: list[dict] = []
        self.available_configs: list[str] = []
        self.selected_config_index: int = 0
        self.task_start_time: float | None = None
        self.active_prompt: str | None = None

        self.webserver_mode = False
        self.pending_input_event = threading.Event()
        self.pending_input_answer: str | None = None
        self.session_pin: str | None = None

    def submit_input(self, answer: str) -> None:
        """Called by web API to submit an answer to a pending prompt or config selection."""
        self.pending_input_answer = answer
        self.pending_input_event.set()

    def set_available_configs(
        self, configs: list[str], selected: str | None = None
    ) -> None:
        """Set available config paths and initial selection."""
        with self.lock:
            self.available_configs = list(configs)
            if selected and selected in self.available_configs:
                self.selected_config_index = self.available_configs.index(selected)
            else:
                self.selected_config_index = 0
            self._render_unlocked()

    def move_config_selection(self, delta: int) -> str:
        """Move the config selection by delta."""
        with self.lock:
            if self.available_configs:
                self.selected_config_index = (
                    self.selected_config_index + delta
                ) % len(self.available_configs)
            self._render_unlocked()
            if self.available_configs and 0 <= self.selected_config_index < len(
                self.available_configs
            ):
                return self.available_configs[self.selected_config_index]
            return ""

    def select_config_by_index(self, index: int) -> str:
        """Select a config file by its 0-based index."""
        with self.lock:
            if self.available_configs and 0 <= index < len(self.available_configs):
                self.selected_config_index = index
            self._render_unlocked()
            if self.available_configs and 0 <= self.selected_config_index < len(
                self.available_configs
            ):
                return self.available_configs[self.selected_config_index]
            return ""

    def get_selected_config(self) -> str:
        """Return the currently selected config file path."""
        with self.lock:
            if self.available_configs and 0 <= self.selected_config_index < len(
                self.available_configs
            ):
                return self.available_configs[self.selected_config_index]
            return "config.ini"

    def reset_nodes(self) -> None:
        """Clear the current node states."""
        with self.lock:
            self.node_states.clear()
            self._render_unlocked()

    def set_prompt(self, text: str | None) -> None:
        """Set an active prompt to display at the bottom of the dashboard."""
        with self.lock:
            self.active_prompt = text
            self._render_unlocked()

    def add_message(self, text: str, timeout_seconds: int = 10) -> None:
        """Add an event/alert message with a countdown period to the dashboard."""
        with self.lock:
            self.messages.append(
                {
                    "text": text,
                    "expire_at": time.time() + float(timeout_seconds),
                }
            )
            self._render_unlocked()

    def set_master_state(self, state: str) -> None:
        """Update overall master state (e.g. splitting file, merging files, finished)."""
        with self.lock:
            if state == PROGRESS_SPLITTING_FILE:
                if self.task_start_time is None:
                    self.task_start_time = time.time()
                self.splitting_flag = True
            elif state in ("config selection", "initializing"):
                self.task_start_time = None
            elif state == PROGRESS_MERGING_FILES:
                self.merging_flag = True

            self.master_state = state
            self._render_unlocked()

    def update_node_flags(
        self,
        node_ip: str,
        receiving: bool = False,
        executing: bool = False,
        sending: bool = False,
    ) -> None:
        """Update 3-phase node state flags."""
        with self.lock:
            if node_ip in self.node_states:
                self.node_states[node_ip]["receiving"] = receiving
                self.node_states[node_ip]["executing"] = executing
                self.node_states[node_ip]["sending"] = sending
                self._render_unlocked()

    def update_node(
        self,
        node_ip: str,
        node_id: str,
        filename: str,
        state: str,
        elapsed_seconds: float | None = None,
        pct: float | None = None,
        eta: str | None = None,
        receiving: bool | None = None,
        executing: bool | None = None,
        sending: bool | None = None,
        connected: bool | None = None,
        hostname: str | None = None,
        username: str | None = None,
    ) -> None:
        """Update individual worker node state and flags."""
        with self.lock:
            existing = self.node_states.get(node_ip, {})
            rcv = (
                receiving if receiving is not None else existing.get("receiving", False)
            )
            exc = (
                executing if executing is not None else existing.get("executing", False)
            )
            snd = sending if sending is not None else existing.get("sending", False)
            conn = (
                connected if connected is not None else existing.get("connected", True)
            )

            self.node_states[node_ip] = {
                "node_id": node_id,
                "filename": filename,
                "state": state,
                "elapsed": elapsed_seconds,
                "pct": pct,
                "eta": eta,
                "receiving": rcv,
                "executing": exc,
                "sending": snd,
                "connected": conn,
                "hostname": (
                    hostname if hostname is not None else existing.get("hostname")
                ),
                "username": (
                    username if username is not None else existing.get("username")
                ),
            }
            self._render_unlocked()

    def set_node_connected(
        self, node_ip: str, connected: bool, state: str | None = None
    ) -> None:
        """Update connection status of a node."""
        with self.lock:
            if node_ip in self.node_states:
                self.node_states[node_ip]["connected"] = connected
                if not connected:
                    st = state or "disconnected"
                    if st in ("disconnected", "killed") and ":" in node_ip:
                        del self.node_states[node_ip]
                    else:
                        self.node_states[node_ip]["state"] = st
                        self.node_states[node_ip]["receiving"] = False
                        self.node_states[node_ip]["executing"] = False
                        self.node_states[node_ip]["sending"] = False
                elif state:
                    self.node_states[node_ip]["state"] = state
                self._render_unlocked()

    def remove_node(self, node_ip: str) -> None:
        """Remove a node from the active dashboard view."""
        with self.lock:
            self.node_states.pop(node_ip, None)
            self._render_unlocked()

    def render(self) -> None:
        with self.lock:
            self._render_unlocked()

    def _render_unlocked(self) -> None:
        """Render formatted CLI dashboard view, or update internal state only in webserver mode."""
        now = time.time()

        # Filter expired messages using wall-clock expire_at
        self.messages = [
            m
            for m in self.messages
            if now < m.get("expire_at", now + m.get("time_left", 0))
        ]

        uptime_str = ""
        if self.task_start_time is not None:
            uptime = int(now - self.task_start_time)
            um, us = uptime // 60, uptime % 60
            uptime_str = f" [UPTIME: {um:02d}:{us:02d}]"

        pin_str = (
            f" [PIN: {self.session_pin}]"
            if getattr(self, "session_pin", None)
            else ""
        )
        header = f"=== MASTER DASHBOARD: [{self.master_state.upper()}]{pin_str}{uptime_str} ==="
        divider = "=" * (len(header) + 12)

        lines = ["", divider, header, divider]

        if self.available_configs:
            lines.append("CONFIG FILE SELECTION:")
            for idx, cfg in enumerate(self.available_configs):
                marker = (
                    "► [SELECTED]" if idx == self.selected_config_index else "  "
                )
                lines.append(f"  [{idx + 1}] {cfg:<30} {marker}")
            lines.append("-" * (len(header) + 12))

        if self.master_state != "config selection":
            lines.append(
                f"{'NODE IP (USER@HOST)':<32} {'DEVICE':<10} {'CONNECTED':<11} {'PART FILE':<12} {'STATE':<20} {'FLAGS [R/E/S]':<14} {'PROGRESS / RUN TIME'}"
            )
            lines.append("-" * (len(header) + 12 + 16))

            if not self.node_states:
                lines.append("  (Waiting for worker connections...)")
            else:
                for ip, info in self.node_states.items():
                    state_str = info["state"]
                    elapsed = info["elapsed"]
                    pct = info["pct"]
                    eta = info["eta"]
                    conn_str = "YES" if info.get("connected", True) else "NO"
                    rcv = "R" if info.get("receiving") else "-"
                    exc = "E" if info.get("executing") else "-"
                    snd = "S" if info.get("sending") else "-"
                    flags_str = f"[{rcv}/{exc}/{snd}]"

                    prog_parts = []
                    if elapsed is not None:
                        m, s = int(elapsed) // 60, int(elapsed) % 60
                        prog_parts.append(f"{m:02d}:{s:02d}")
                    if pct is not None:
                        prog_parts.append(f"{pct:.1f}%")
                    if eta is not None:
                        prog_parts.append(f"[ETA: {eta}]")

                    prog_str = " ".join(prog_parts)

                    display_ip = ip
                    if info.get("hostname") and info.get("username"):
                        display_ip = f"{ip}({info['username']}@{info['hostname']})"

                    lines.append(
                        f"{display_ip:<32} {info['node_id']:<10} {conn_str:<11} {info['filename']:<12} {state_str:<20} {flags_str:<14} {prog_str}"
                    )

        if self.messages:
            lines.append(divider)
            lines.append("MESSAGES / ALERTS:")
            for m in self.messages:
                secs_left = (
                    int(m["expire_at"] - now)
                    if "expire_at" in m
                    else int(m.get("time_left", 0))
                )
                lines.append(f"  • {m['text']} ({max(0, secs_left)}s left)")

        lines.append(divider)

        if self.active_prompt:
            for line in self.active_prompt.split("\n"):
                lines.append(line)

        out_str = "\n".join(lines)

        # Always log to file
        printlog(out_str + "\n")

        # Only write to stdout in terminal (non-webserver) mode
        if not getattr(self, "webserver_mode", False):
            # Clear cursor to home, clear screen, and clear scrollback buffer
            sys.stdout.write("\033[H\033[2J\033[3J")
            sys.stdout.write(out_str)
            if not out_str.endswith("\n"):
                sys.stdout.write("\n")
            sys.stdout.flush()
