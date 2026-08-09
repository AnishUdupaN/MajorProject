"""Master merge module: generate filelist.txt and run merge_command."""

import shlex
import subprocess
from pathlib import Path

from constants import (
    MASTER_OUTPUT_DIRECTORY,
    PART_FILENAME_PREFIX,
    PART_FILENAME_SUFFIX,
    PROGRESS_FINISHED,
    PROGRESS_MERGING_FILES,
)


def generate_filelist_text(
    active_node_count: int, output_directory: str = MASTER_OUTPUT_DIRECTORY
) -> str:
    """Generate the contents of filelist.txt for ffmpeg concat demuxer using absolute paths."""
    dir_path = Path(output_directory).resolve()
    lines = [
        f"file '{dir_path / f'{PART_FILENAME_PREFIX}{index}{PART_FILENAME_SUFFIX}'}'"
        for index in range(1, active_node_count + 1)
    ]
    return "\n".join(lines) + "\n"


def write_filelist_txt(
    active_node_count: int, output_directory: str = MASTER_OUTPUT_DIRECTORY
) -> str:
    """Write filelist.txt into output_directory AND root working directory for universal compatibility."""
    dir_path = Path(output_directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    content = generate_filelist_text(active_node_count, output_directory)

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



def run_merge_command(
    merge_command_template: str,
    active_node_count: int,
    output_directory: str = MASTER_OUTPUT_DIRECTORY,
) -> str:
    """Once all worker nodes finish, combine output parts into one final file."""
    print(PROGRESS_MERGING_FILES)
    write_filelist_txt(active_node_count, output_directory)
    resolved_command = build_merge_command(merge_command_template, output_directory)
    command_parts = shlex.split(resolved_command)

    result = subprocess.run(command_parts, check=False)

    if result.returncode != 0:
        raise RuntimeError(
            f"merge_command failed with exit code {result.returncode}: {resolved_command}"
        )

    final_output_path = Path(output_directory) / "output.mkv"
    if not final_output_path.is_file():
        raise FileNotFoundError(
            f"Merged output file not found at expected location: {final_output_path}"
        )

    print(f"{PROGRESS_FINISHED} (master: {final_output_path.name})")
    return str(final_output_path)
