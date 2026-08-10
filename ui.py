"""Progress Display / UI Module for Master Node status dashboard."""

import sys
import time
from typing import Any, Dict


class StatusDashboard:
    """Live CLI status view tracking master and per-node states."""

    def __init__(self, master_state: str = "initializing") -> None:
        self.master_state = master_state
        self.node_states: Dict[str, Dict[str, Any]] = {}

    def set_master_state(self, state: str) -> None:
        """Update overall master state (e.g. splitting file, merging files, finished)."""
        self.master_state = state
        self.render()

    def update_node(
        self,
        node_ip: str,
        node_id: str,
        filename: str,
        state: str,
        elapsed_seconds: float | None = None,
        pct: float | None = None,
        eta: str | None = None,
    ) -> None:
        """Update individual worker node state."""
        self.node_states[node_ip] = {
            "node_id": node_id,
            "filename": filename,
            "state": state,
            "elapsed": elapsed_seconds,
            "pct": pct,
            "eta": eta,
        }
        self.render()

    def remove_node(self, node_ip: str) -> None:
        """Remove a node from the active dashboard view."""
        self.node_states.pop(node_ip, None)
        self.render()


    def render(self) -> None:
        """Render formatted CLI dashboard view."""
        header = f"=== MASTER DASHBOARD: [{self.master_state.upper()}] ==="
        divider = "=" * len(header)

        lines = ["", divider, header, divider]
        lines.append(
            f"{'NODE IP':<16} {'DEVICE':<10} {'PART FILE':<12} {'STATE':<16} {'PROGRESS / RUN TIME'}"
        )
        lines.append("-" * len(header))

        if not self.node_states:
            lines.append("  (Waiting for worker connections...)")
        else:
            for ip, info in self.node_states.items():
                state_str = info["state"]
                elapsed = info["elapsed"]
                pct = info["pct"]
                eta = info["eta"]

                prog_parts = []
                if elapsed is not None:
                    m, s = int(elapsed) // 60, int(elapsed) % 60
                    prog_parts.append(f"{m:02d}:{s:02d}")
                if pct is not None:
                    prog_parts.append(f"{pct:.1f}%")
                if eta is not None:
                    prog_parts.append(f"[ETA: {eta}]")

                prog_str = " ".join(prog_parts)
                lines.append(
                    f"{ip:<16} {info['node_id']:<10} {info['filename']:<12} {state_str:<16} {prog_str}"
                )

        lines.append(divider)
        print("\n".join(lines))
