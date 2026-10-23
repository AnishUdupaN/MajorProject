"""Platform target resolution and binary cache path helpers."""

from __future__ import annotations

import hashlib
import platform
from pathlib import Path

from core.constants import WORKER_BINARY_DIRECTORY

# Mapping from (platform.system(), platform.machine()) → (os_folder, arch_folder)
# supported targets: linux (x64) and macos (aarch64)
BINARY_PLATFORM_MAP: dict[tuple[str, str], tuple[str, str]] = {
    ("Linux", "x86_64"): ("linux", "x64"),
    ("Darwin", "arm64"): ("macos", "arm64"),
}


def get_worker_platform() -> tuple[str, str]:
    """Return (os_folder, arch_folder) for supported worker platform targets."""
    sys_name = platform.system()
    machine = platform.machine()
    key = (sys_name, machine)
    if key in BINARY_PLATFORM_MAP:
        return BINARY_PLATFORM_MAP[key]
    os_folder = "macos" if sys_name == "Darwin" else sys_name.lower()
    arch_folder = (
        "aarch64"
        if machine in ("arm64", "aarch64")
        else ("x64" if machine in ("x86_64", "AMD64") else machine.lower())
    )
    return os_folder, arch_folder


def compute_md5(filepath: str | Path) -> str:
    """Compute the MD5 hex digest of a file using chunked reads (memory-efficient).

    MD5 is chosen because it is natively available in Python's hashlib on all
    platforms, and hashing a typical binary (~50 MB) takes well under 100 ms.
    """
    h = hashlib.md5()
    with open(filepath, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def get_binary_base_dir(
    binaries_directory: str = "binaries",
    binary_name: str = "",
    os_folder: str = "",
) -> Path:
    """Resolve base directory for a binary on the master node: ./binaries/<binary_name>/<os_name>/"""
    base_dirs = [
        Path(binaries_directory),
        Path(__file__).resolve().parent.parent / binaries_directory,
    ]
    for b_dir in base_dirs:
        candidate = b_dir / binary_name / os_folder
        if candidate.is_dir():
            return candidate
    return Path(binaries_directory) / binary_name / os_folder


def get_binary_path(
    binaries_directory: str,
    os_folder: str,
    arch_folder: str,
    binary_name: str,
) -> Path | None:
    """Return the Path to the binary for the given OS/arch, or None if not found.

    Resolves to ./binaries/<binary_name>/<os_folder>/
    """
    base_dirs = [
        Path(binaries_directory),
        Path(__file__).resolve().parent.parent / binaries_directory,
    ]
    for b_dir in base_dirs:
        target_dir = b_dir / binary_name / os_folder
        if not target_dir.is_dir():
            continue

        # 1. Primary layout: ./binaries/<binary_name>/<os_folder>/<binary_name>
        primary = target_dir / binary_name
        if primary.is_file():
            return primary

        # 2. Python script layout: ./binaries/<binary_name>/<os_folder>/<binary_name>.py
        script = target_dir / f"{binary_name}.py"
        if script.is_file():
            return script

        # 3. Check for any non-hidden file in target_dir
        for item in sorted(target_dir.iterdir()):
            if item.is_file() and not item.name.startswith("."):
                return item

        # 4. Multi-directory structure (e.g. Blender app bundle)
        return target_dir

    return None


def resolve_worker_binary_base_dir(
    binary_name: str,
    os_folder: str,
) -> Path:
    """Return base local cache directory for a binary on worker: ./worker/binaries/<binary_name>/<os_name>/"""
    cache_dir = Path(WORKER_BINARY_DIRECTORY) / binary_name / os_folder
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def resolve_worker_binary_cache_path(
    os_folder: str,
    arch_folder: str,
    binary_name: str,
) -> Path:
    """Return expected local cache path for a binary on worker: ./worker/binaries/<binary_name>/<os_folder>/<binary_name>"""
    return resolve_worker_binary_base_dir(binary_name, os_folder) / binary_name
