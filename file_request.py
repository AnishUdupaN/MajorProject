"""Worker-side HTTP calls to a node's file-transfer daemon."""

import json
import urllib.error
import urllib.request
from pathlib import Path


def request_file_list(master_ip_address: str, file_transfer_port: int) -> list[str]:
    """Worker calls GET /listfiles to see what's allocated to it."""
    url = f"http://{master_ip_address}:{file_transfer_port}/listfiles"
    with urllib.request.urlopen(url, timeout=10.0) as response:
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
        with urllib.request.urlopen(url, timeout=10.0) as response:
            destination_path.write_bytes(response.read())
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Failed to fetch {filename}: HTTP {exc.code}") from exc
    return str(destination_path)



def upload_result_file(
    master_ip_address: str,
    file_transfer_port: int,
    source_filepath: str,
) -> None:
    """Worker calls POST /file/<filename> to send its output file back to master's daemon."""
    path = Path(source_filepath)
    if not path.is_file():
        raise FileNotFoundError(f"Result file to upload does not exist: {source_filepath}")

    filename = path.name
    file_bytes = path.read_bytes()
    url = f"http://{master_ip_address}:{file_transfer_port}/file/{filename}"

    request = urllib.request.Request(
        url,
        data=file_bytes,
        headers={
            "Content-Type": "application/octet-stream",
            "Content-Length": str(len(file_bytes)),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10.0) as response:
            if response.status != 200:
                raise RuntimeError(f"Failed to upload {filename}: HTTP {response.status}")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Failed to upload {filename}: HTTP {exc.code}") from exc


