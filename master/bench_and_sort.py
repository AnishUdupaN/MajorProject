"""Worker benchmarking evaluation, affinity sorting, and active/spare partitioning."""

from __future__ import annotations

from core.benchmark import rank_workers_by_affinity


def split_active_and_spare_addresses(
    worker_ip_addresses: list[str], max_nodes: int
) -> tuple[list[str], list[str]]:
    """First max_nodes addresses are active nodes; remaining addresses are spares."""
    active_nodes = worker_ip_addresses[:max_nodes]
    spare_nodes = worker_ip_addresses[max_nodes:]
    return active_nodes, spare_nodes


def rank_and_partition_workers(
    worker_ip_addresses: list[str],
    telemetry_store: dict[str, dict],
    resource_type: str = "auto",
    min_vram_mb: int = 0,
    max_nodes: int = 1,
) -> tuple[list[str], list[str], list[str]]:
    """Rank connected workers by affinity and split them into active and spare nodes.

    Returns: (ranked_addresses, active_nodes, spare_nodes)
    """
    ranked = rank_workers_by_affinity(
        telemetry_map=telemetry_store,
        resource_type=resource_type,
        min_vram_mb=min_vram_mb,
        worker_addresses=worker_ip_addresses,
    )
    active, spares = split_active_and_spare_addresses(ranked, max_nodes)
    return ranked, active, spares
