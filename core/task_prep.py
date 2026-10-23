"""Master node video/data splitting, concat merging, and temporary file cleanup."""

from __future__ import annotations

import re
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from core.constants import (
    DEFAULT_INPUT_VIDEO,
    MASTER_INPUT_DIRECTORY,
    MASTER_OUTPUT_DIRECTORY,
    PART_FILENAME_PREFIX,
    PART_FILENAME_SUFFIX,
    PROGRESS_FINISHED,
    PROGRESS_MERGING_FILES,
    PROGRESS_SPLITTING_FILE,
    WORKER_INPUT_DIRECTORY,
    WORKER_OUTPUT_DIRECTORY,
)
from core.dashboard import printlog


def resolve_input_video(
    split_command: str = "",
    input_video: Optional[str] = None,
    parts_directory: str = MASTER_INPUT_DIRECTORY,
) -> str:
    """Resolve the actual input video path.

    If input_video exists on disk, returns it. Otherwise, attempts to infer it
    from split_command or by searching parts_directory.
    """
    if (
        input_video
        and input_video != DEFAULT_INPUT_VIDEO
        and Path(input_video).is_file()
    ):
        return input_video

    if Path(DEFAULT_INPUT_VIDEO).is_file():
        return DEFAULT_INPUT_VIDEO

    # Extract input file path from split_command (-i <path>)
    if split_command:
        match = re.search(r"-i\s+([^\s]+)", split_command)
        if match:
            candidate = match.group(1).replace("{input_directory}", parts_directory)
            if Path(candidate).is_file():
                return candidate

    parts_path = Path(parts_directory)
    if parts_path.is_dir():
        for candidate in sorted(parts_path.glob("input.*")):
            if candidate.is_file():
                return str(candidate)
        for candidate in sorted(parts_path.iterdir()):
            if (
                candidate.is_file()
                and not candidate.name.startswith("part")
                and candidate.name != "filelist.txt"
            ):
                return str(candidate)

    return input_video or DEFAULT_INPUT_VIDEO


def get_part_extension(
    split_command: str = "",
    parts_directory: str = MASTER_INPUT_DIRECTORY,
) -> str:
    """Infer part file extension (.webm, .mkv, .mp4) from split_command or existing part files."""
    if split_command:
        match = re.search(r"part(?:%d|\d+)?(\.[a-zA-Z0-9]+)", split_command)
        if match:
            return match.group(1)

    parts_path = Path(parts_directory)
    if parts_path.is_dir():
        for item in parts_path.iterdir():
            if item.is_file() and item.name.startswith("part"):
                return item.suffix

    return PART_FILENAME_SUFFIX


def part_filename_for_node(
    node_index: int,
    extension: Optional[str] = None,
    split_command: str = "",
    parts_directory: str = MASTER_INPUT_DIRECTORY,
) -> str:
    """Return the split output name for a node (part1.mkv, part2.webm, ...)."""
    if extension is None:
        extension = get_part_extension(split_command, parts_directory)
    return f"{PART_FILENAME_PREFIX}{node_index}{extension}"


def get_video_duration_seconds(input_video: str) -> float:
    """Read the input video duration in seconds using ffprobe."""
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            input_video,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"ffprobe failed for {input_video}: {result.stderr.strip()}"
        )
    return float(result.stdout.strip())


def build_split_command(
    split_command: str,
    active_node_count: int,
    input_video: Optional[str] = None,
    parts_directory: str = MASTER_INPUT_DIRECTORY,
) -> str:
    """Fill split_command placeholders so ffmpeg produces exactly one part per active node."""
    actual_input = resolve_input_video(split_command, input_video, parts_directory)
    if "{segment_times}" in split_command:
        duration_seconds = get_video_duration_seconds(actual_input)
        split_points = [
            duration_seconds * node_index / active_node_count
            for node_index in range(1, active_node_count)
        ]
        segment_times = ",".join(f"{split_point:.3f}" for split_point in split_points)
        resolved_command = split_command.replace("{segment_times}", segment_times)
    else:
        resolved_command = split_command

    resolved_command = resolved_command.replace("{input_directory}", parts_directory)
    return resolved_command


def run_split_command(
    split_command: str,
    active_node_count: int,
    input_video: Optional[str] = None,
    parts_directory: str = MASTER_INPUT_DIRECTORY,
) -> None:
    """On start, master runs split_command against input video, writing part files into parts_directory."""
    print(PROGRESS_SPLITTING_FILE)
    Path(parts_directory).mkdir(parents=True, exist_ok=True)
    actual_input = resolve_input_video(split_command, input_video, parts_directory)

    if active_node_count == 1:
        ext = get_part_extension(split_command, parts_directory)
        destination = Path(parts_directory) / part_filename_for_node(1, extension=ext)
        shutil.copyfile(actual_input, destination)
        return

    resolved_command = build_split_command(
        split_command, active_node_count, actual_input, parts_directory
    )
    command_parts = shlex.split(resolved_command)
    result = subprocess.run(
        command_parts,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"split_command failed with exit code {result.returncode}:\n{result.stderr.strip()}"
        )


def verify_part_files(
    active_node_count: int,
    parts_directory: str = MASTER_INPUT_DIRECTORY,
    split_command: str = "",
) -> list[str]:
    """Verify part1 through partN exist for each active node."""
    parts_path = Path(parts_directory)
    ext = get_part_extension(split_command, parts_directory)
    part_filenames = [
        part_filename_for_node(node_index, extension=ext)
        for node_index in range(1, active_node_count + 1)
    ]
    missing_parts = [
        filename
        for filename in part_filenames
        if not (parts_path / filename).is_file()
    ]
    if missing_parts:
        raise FileNotFoundError(
            "Split output missing required part files: " + ", ".join(missing_parts)
        )
    return [str(parts_path / filename) for filename in part_filenames]


def generate_filelist_text(
    active_node_count: int,
    output_directory: str = MASTER_OUTPUT_DIRECTORY,
    split_command: str = "",
) -> str:
    """Generate the contents of filelist.txt for ffmpeg concat demuxer using absolute paths."""
    dir_path = Path(output_directory).resolve()
    lines = [
        f"file '{dir_path / part_filename_for_node(index, split_command=split_command, parts_directory=output_directory)}'"
        for index in range(1, active_node_count + 1)
    ]
    return "\n".join(lines) + "\n"


def write_filelist_txt(
    active_node_count: int,
    output_directory: str = MASTER_OUTPUT_DIRECTORY,
    split_command: str = "",
) -> str:
    """Write filelist.txt into output_directory AND root working directory for universal compatibility."""
    dir_path = Path(output_directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    content = generate_filelist_text(
        active_node_count, output_directory, split_command=split_command
    )

    filelist_in_output = dir_path / "filelist.txt"
    filelist_in_output.write_text(content)

    filelist_in_root = Path("filelist.txt")
    filelist_in_root.write_text(content)

    return str(filelist_in_output)


def build_merge_command(
    merge_command_template: str,
    output_directory: str = MASTER_OUTPUT_DIRECTORY,
) -> str:
    """Substitute placeholders in merge_command template."""
    resolved = merge_command_template.replace("{output_directory}", output_directory)
    resolved = resolved.replace("{filelist}", f"{output_directory}/filelist.txt")
    return resolved


def resolve_output_file(
    merge_command_template: str,
    output_directory: str = MASTER_OUTPUT_DIRECTORY,
) -> Path:
    """Resolve the merged output file path by checking the merge command or output directory."""
    resolved_command = build_merge_command(merge_command_template, output_directory)
    out_dir_path = Path(output_directory).resolve()

    try:
        command_parts = shlex.split(resolved_command)
        for part in reversed(command_parts):
            try:
                p = Path(part).resolve()
                if (
                    out_dir_path in p.parents or p.parent == out_dir_path
                ) and p.name != "filelist.txt" and not p.name.startswith("part"):
                    if p.is_file():
                        return p
            except Exception:
                pass
    except Exception:
        pass

    out_dir = Path(output_directory)
    if out_dir.is_dir():
        for candidate in out_dir.glob("output.*"):
            if candidate.is_file():
                return candidate
        for candidate in out_dir.iterdir():
            if (
                candidate.is_file()
                and candidate.name != "filelist.txt"
                and not candidate.name.startswith("part")
            ):
                return candidate

    return Path(output_directory) / "output.mkv"


def run_merge_command(
    merge_command_template: str,
    active_node_count: int,
    output_directory: str = MASTER_OUTPUT_DIRECTORY,
    split_command: str = "",
) -> str:
    """Once all worker nodes finish, combine output parts into one final file."""
    print(PROGRESS_MERGING_FILES)
    write_filelist_txt(active_node_count, output_directory, split_command=split_command)
    resolved_command = build_merge_command(merge_command_template, output_directory)
    command_parts = shlex.split(resolved_command)

    result = subprocess.run(
        command_parts,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"merge_command failed with exit code {result.returncode}:\n{result.stderr.strip()}"
        )

    final_output_path = resolve_output_file(merge_command_template, output_directory)
    if not final_output_path.is_file():
        raise FileNotFoundError(
            f"Merged output file not found at expected location: {final_output_path}"
        )

    print(f"{PROGRESS_FINISHED} (master: {final_output_path.name})")
    return str(final_output_path)


def cleanup_master_temporary_files(
    parts_directory: str = MASTER_INPUT_DIRECTORY,
    output_directory: str = MASTER_OUTPUT_DIRECTORY,
    input_video: Optional[str] = None,
) -> None:
    """Clean up temporary split and intermediate part files on master node."""
    input_path = Path(input_video).resolve() if input_video else None

    # Clean temporary split part files in parts_directory
    parts_dir = Path(parts_directory)
    if parts_dir.is_dir():
        for file_path in parts_dir.iterdir():
            if file_path.is_file():
                try:
                    resolved = file_path.resolve()
                    if input_path and resolved == input_path:
                        continue
                    if file_path.name.startswith(
                        PART_FILENAME_PREFIX
                    ) or file_path.name.startswith("part"):
                        file_path.unlink()
                        printlog(f"Cleaned up temporary master part file: {file_path}")
                except Exception as exc:
                    printlog(f"Failed to delete {file_path}: {exc}")

    # Clean intermediate received part files in output_directory
    out_dir = Path(output_directory)
    if out_dir.is_dir():
        for file_path in out_dir.iterdir():
            if file_path.is_file():
                try:
                    if file_path.name.startswith(
                        PART_FILENAME_PREFIX
                    ) or file_path.name.startswith("part"):
                        file_path.unlink()
                        printlog(
                            f"Cleaned up temporary master output part file: {file_path}"
                        )
                except Exception as exc:
                    printlog(f"Failed to delete {file_path}: {exc}")

    # Clean temporary filelist.txt files if present
    for fl in (Path("filelist.txt"), Path(output_directory) / "filelist.txt"):
        if fl.is_file():
            try:
                fl.unlink()
            except Exception:
                pass


def cleanup_worker_temporary_files(
    download_directory: str = WORKER_INPUT_DIRECTORY,
    output_directory: str = WORKER_OUTPUT_DIRECTORY,
) -> None:
    """Clean up downloaded and processed part files on worker node."""
    for dir_path_str in (download_directory, output_directory):
        dir_path = Path(dir_path_str)
        if dir_path.is_dir():
            for file_path in dir_path.iterdir():
                if file_path.is_file():
                    try:
                        file_path.unlink()
                        printlog(f"Cleaned up temporary worker file: {file_path}")
                    except Exception as exc:
                        printlog(f"Failed to delete {file_path}: {exc}")
