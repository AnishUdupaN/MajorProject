"""Shared constants for the distributed video processing prototype."""

FIXED_PORT = 5000

CONFIG_KEY_SPLIT_COMMAND = "split_command"
CONFIG_KEY_EXECUTE_COMMAND = "execute_command"
CONFIG_KEY_MERGE_COMMAND = "merge_command"
CONFIG_KEY_MAX_NODES = "max_nodes"

CONFIG_KEYS = (
    CONFIG_KEY_SPLIT_COMMAND,
    CONFIG_KEY_EXECUTE_COMMAND,
    CONFIG_KEY_MERGE_COMMAND,
    CONFIG_KEY_MAX_NODES,
)

PROGRESS_SPLITTING_FILE = "splitting file"
PROGRESS_SENDING_FILE = "sending file"
PROGRESS_EXECUTING = "executing"
PROGRESS_RECEIVING_FILES = "receiving files"
PROGRESS_FINISHED = "finished"
PROGRESS_MERGING_FILES = "merging files"
