"""Download allocated part files from master HTTP daemon to worker node."""

from __future__ import annotations

from core.file_transfer import request_file, request_file_list


def fetch_allocated_part_file(
    master_ip_address: str,
    file_transfer_port: int,
    download_directory: str,
) -> str:
    """On receiving ready, list and fetch this worker's allocated part file."""
    allocated_files = request_file_list(master_ip_address, file_transfer_port)
    if not allocated_files:
        raise RuntimeError("No files listed for this worker")

    downloaded_paths = [
        request_file(
            master_ip_address,
            file_transfer_port,
            filename,
            download_directory,
        )
        for filename in allocated_files
    ]
    return downloaded_paths[0]
