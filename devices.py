"""devices.json writer: map active node IP addresses to device IDs."""

import json
from pathlib import Path

from constants import DEVICE_ID_PREFIX, DEVICES_JSON_FILE


def assign_device_id(position: int) -> str:
    """Assign a device ID in IP-list order (node1, node2, ...)."""
    return f"{DEVICE_ID_PREFIX}{position}"


def build_active_device_mapping(active_ip_addresses: list[str]) -> dict[str, str]:
    """Map each active node's IP address to a device ID; spares are excluded."""
    return {
        ip_address: assign_device_id(index)
        for index, ip_address in enumerate(active_ip_addresses, start=1)
    }


def write_devices_json(
    active_ip_addresses: list[str],
    devices_json_path: str = DEVICES_JSON_FILE,
) -> dict[str, str]:
    """Write devices.json for active nodes only, before splitting starts."""
    device_mapping = build_active_device_mapping(active_ip_addresses)
    path = Path(devices_json_path)
    path.write_text(json.dumps(device_mapping, indent=2) + "\n")
    return device_mapping


def swap_device_ip(
    old_ip: str,
    new_ip: str,
    devices_json_path: str = DEVICES_JSON_FILE,
) -> dict[str, str]:
    """Replace a killed node's IP with the spare node's IP, keeping the device ID."""
    path = Path(devices_json_path)
    if not path.is_file():
        raise FileNotFoundError(f"{devices_json_path} does not exist")

    mapping: dict[str, str] = json.loads(path.read_text())
    if old_ip not in mapping:
        raise KeyError(f"IP address {old_ip} not found in {devices_json_path}")

    device_id = mapping.pop(old_ip)
    mapping[new_ip] = device_id
    path.write_text(json.dumps(mapping, indent=2) + "\n")
    return mapping

