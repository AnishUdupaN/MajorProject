"""Worker-side HTTP calls to a node's file-transfer daemon."""

import json
import urllib.error
import urllib.request
from pathlib import Path


def request_file_list(master_ip_address: str, file_transfer_port: int) -> list[str]:
    """Worker calls GET /listfiles to see what's allocated to it."""
    url = f"http://{master_ip_address}:{file_transfer_port}/listfiles"
    with urllib.request.urlopen(url) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload["files"]


def request_file(
    master_ip_address: str,
    file_transfer_port: int,
    filename: str,
    destination_directory: str = ".",
) -> str:
    """Worker calls GET /file/<filename> to fetch its allocated part file."""
    url = f"http://{master_ip_address}:{file_transfer_port}/file/{filename}"
    destination_path = Path(destination_directory) / filename
    try:
        with urllib.request.urlopen(url) as response:
            destination_path.write_bytes(response.read())
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Failed to fetch {filename}: HTTP {exc.code}") from exc
    return str(destination_path)
