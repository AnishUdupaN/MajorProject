"""Control messages (ready, file_received) sent over the fixed TCP port."""

import json
import socket

from constants import (
    MESSAGE_TYPE_FAILED,
    MESSAGE_TYPE_FILE_RECEIVED,
    MESSAGE_TYPE_FINISHED,
    MESSAGE_TYPE_READY,
)


def send_json_message(connection: socket.socket, message: dict) -> None:
    """Send one JSON control message terminated by a newline."""
    payload = json.dumps(message) + "\n"
    connection.sendall(payload.encode("utf-8"))


def receive_json_message(connection: socket.socket) -> dict:
    """Receive one JSON control message from the connection."""
    buffer = ""
    while "\n" not in buffer:
        chunk = connection.recv(4096)
        if not chunk:
            raise ConnectionError("Control connection closed before message was received")
        buffer += chunk.decode("utf-8")
    line, _remainder = buffer.split("\n", 1)
    return json.loads(line)


def send_ready_message(
    connection: socket.socket, master_ip_address: str, file_transfer_port: int
) -> None:
    """Master sends a ready message with this node's file-transfer daemon port."""
    send_json_message(
        connection,
        {
            "type": MESSAGE_TYPE_READY,
            "master_ip_address": master_ip_address,
            "file_transfer_port": file_transfer_port,
        },
    )


def receive_ready_message(connection: socket.socket) -> dict:
    """Worker receives the ready message from the master."""
    message = receive_json_message(connection)
    if message.get("type") != MESSAGE_TYPE_READY:
        raise ValueError(f"Expected ready message, got: {message!r}")
    return message


def send_file_received_message(connection: socket.socket) -> None:
    """Worker sends file_received after fetching its allocated part file."""
    send_json_message(connection, {"type": MESSAGE_TYPE_FILE_RECEIVED})


def receive_file_received_message(connection: socket.socket) -> None:
    """Master waits for file_received after sending a node's part file."""
    message = receive_json_message(connection)
    if message.get("type") != MESSAGE_TYPE_FILE_RECEIVED:
        raise ValueError(f"Expected file_received message, got: {message!r}")


def send_finished_message(
    connection: socket.socket, execution_time_seconds: float, part_filename: str
) -> None:
    """Worker sends finished message to master on completing execution."""
    send_json_message(
        connection,
        {
            "type": MESSAGE_TYPE_FINISHED,
            "execution_time": execution_time_seconds,
            "part_filename": part_filename,
        },
    )


def receive_finished_message(connection: socket.socket) -> dict:
    """Master receives finished message from worker."""
    message = receive_json_message(connection)
    if message.get("type") != MESSAGE_TYPE_FINISHED:
        raise ValueError(f"Expected finished message, got: {message!r}")
    return message


def send_failed_message(connection: socket.socket, reason: str) -> None:
    """Worker sends failed message to master when task fails or is killed."""
    send_json_message(
        connection,
        {
            "type": MESSAGE_TYPE_FAILED,
            "reason": reason,
        },
    )


def receive_failed_message(connection: socket.socket) -> dict:
    """Master receives failed message from worker."""
    message = receive_json_message(connection)
    if message.get("type") != MESSAGE_TYPE_FAILED:
        raise ValueError(f"Expected failed message, got: {message!r}")
    return message
