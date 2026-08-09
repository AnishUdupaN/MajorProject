"""Run split_command on the master and verify one part file per active node."""

import shlex
import shutil
import subprocess
from pathlib import Path

from constants import (
    MASTER_INPUT_DIRECTORY,
    PART_FILENAME_PREFIX,
    PART_FILENAME_SUFFIX,
    PROGRESS_SPLITTING_FILE,
)

DEFAULT_INPUT_VIDEO = f"{MASTER_INPUT_DIRECTORY}/input.mkv"


def part_filename_for_node(node_index: int) -> str:
    """Return the split output name for a node (part1.mkv, part2.mkv, ...)."""
    return f"{PART_FILENAME_PREFIX}{node_index}{PART_FILENAME_SUFFIX}"


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
    input_video: str = DEFAULT_INPUT_VIDEO,
    parts_directory: str = MASTER_INPUT_DIRECTORY,
) -> str:
    """Fill split_command placeholders so ffmpeg produces exactly one part per active node.

    Uses ffmpeg's -segment_times (explicit split points) instead of
    -segment_time (a target interval). -segment_time only tells ffmpeg how
    often to *aim* to cut, and with -c copy it snaps to the nearest keyframe
    each time, so rounding/keyframe spacing can silently produce far more
    (or fewer) parts than active_node_count. -segment_times gives ffmpeg an
    explicit, fixed list of N-1 cut points, which always yields exactly N
    output files, regardless of keyframe layout.
    """
    duration_seconds = get_video_duration_seconds(input_video)
    split_points = [
        duration_seconds * node_index / active_node_count
        for node_index in range(1, active_node_count)
    ]
    segment_times = ",".join(f"{split_point:.3f}" for split_point in split_points)
    resolved_command = split_command.replace("{segment_times}", segment_times)
    resolved_command = resolved_command.replace("{input_directory}", parts_directory)
    return resolved_command


def run_split_command(
    split_command: str,
    active_node_count: int,
    input_video: str = DEFAULT_INPUT_VIDEO,
    parts_directory: str = MASTER_INPUT_DIRECTORY,
) -> None:
    """On start, master runs split_command from config against the input video,
    writing exactly active_node_count part files into parts_directory (master/input/)."""
    print(PROGRESS_SPLITTING_FILE)
    Path(parts_directory).mkdir(parents=True, exist_ok=True)

    if active_node_count == 1:
        # A single active node means "split into 1 part": just copy the
        # input video in place rather than invoking ffmpeg's segment muxer
        # with an empty -segment_times list (which ffmpeg would reject).
        destination = Path(parts_directory) / part_filename_for_node(1)
        shutil.copyfile(input_video, destination)
        return

    resolved_command = build_split_command(
        split_command, active_node_count, input_video, parts_directory
    )
    command_parts = shlex.split(resolved_command)
    result = subprocess.run(command_parts, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"split_command failed with exit code {result.returncode}: {resolved_command}"
        )


def verify_part_files(
    active_node_count: int, parts_directory: str = MASTER_INPUT_DIRECTORY
) -> list[str]:
    """Verify part1.mkv through partN.mkv exist for each active node."""
    parts_path = Path(parts_directory)
    part_filenames = [
        part_filename_for_node(node_index)
        for node_index in range(1, active_node_count + 1)
    ]
    missing_parts = [
        filename
        for filename in part_filenames
        if not (parts_path / filename).is_file()
    ]
    if missing_parts:
        raise FileNotFoundError(
            "Split output missing required part files: "
            + ", ".join(missing_parts)
        )
    return [str(parts_path / filename) for filename in part_filenames]
