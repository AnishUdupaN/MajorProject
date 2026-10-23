"""Low-level socket messaging, protocol framing, HMAC authentication, and PIN exchange."""

from __future__ import annotations

import json
import os
import random
import select
import socket

from core.constants import (
    MESSAGE_REQUIRED_KEYS,
    MESSAGE_TYPE_AUTH,
    MESSAGE_TYPE_BINARY_INFO,
    MESSAGE_TYPE_BINARY_INFO_REQUEST,
    MESSAGE_TYPE_BINARY_READY,
    MESSAGE_TYPE_FAILED,
    MESSAGE_TYPE_FILE_RECEIVED,
    MESSAGE_TYPE_FINISHED,
    MESSAGE_TYPE_PIN_ACCEPTED,
    MESSAGE_TYPE_PIN_CHALLENGE,
    MESSAGE_TYPE_PIN_REJECTED,
    MESSAGE_TYPE_PIN_RESPONSE,
    MESSAGE_TYPE_READY,
    MESSAGE_TYPE_SHUTDOWN,
    MESSAGE_TYPE_WORKER_TELEMETRY,
    SHARED_SECRET_ENV_VAR,
)


class MasterShutdownError(Exception):
    """Raised when master sends a shutdown message to worker nodes."""

    pass


# Socket receive buffers for newline-delimited JSON streams
_SOCKET_BUFFERS: dict[socket.socket, str] = {}


def validate_message(msg: dict) -> None:
    """Validate that a received message has the correct structure and required keys."""
    if not isinstance(msg, dict):
        raise ValueError(f"Expected dict message, got {type(msg).__name__}")
    msg_type = msg.get("type")
    if msg_type is None:
        raise ValueError("Message missing required 'type' field")
    required = MESSAGE_REQUIRED_KEYS.get(msg_type)
    if required is None:
        raise ValueError(f"Unknown message type: {msg_type!r}")
    missing = [key for key in required if key not in msg]
    if missing:
        raise ValueError(
            f"Message type '{msg_type}' missing required keys: {', '.join(missing)}"
        )


def get_shared_secret() -> str | None:
    """Read the optional shared secret from environment variable."""
    secret = os.environ.get(SHARED_SECRET_ENV_VAR, "").strip()
    return secret if secret else None


def send_auth_message(connection: socket.socket, secret: str) -> None:
    """Send an authentication message with the shared secret."""
    send_json_message(connection, {"type": MESSAGE_TYPE_AUTH, "secret": secret})


def verify_auth_message(connection: socket.socket, expected_secret: str) -> bool:
    """Receive and verify an authentication message. Returns True if valid."""
    try:
        msg = receive_json_message(connection)
        if msg.get("type") != MESSAGE_TYPE_AUTH:
            return False
        return msg.get("secret") == expected_secret
    except (ValueError, ConnectionError, json.JSONDecodeError):
        return False


def send_json_message(connection: socket.socket, message: dict) -> None:
    """Send one JSON control message terminated by a newline."""
    payload = json.dumps(message) + "\n"
    connection.sendall(payload.encode("utf-8"))


def receive_json_message(
    connection: socket.socket, timeout: float | None = None
) -> dict:
    """Receive one JSON control message from the connection, buffering extra lines."""
    buf = _SOCKET_BUFFERS.get(connection, "")
    while "\n" not in buf:
        if timeout is not None:
            r, _, _ = select.select([connection], [], [], timeout)
            if not r:
                raise TimeoutError("Timeout waiting for JSON message")
        chunk = connection.recv(4096)
        if not chunk:
            _SOCKET_BUFFERS.pop(connection, None)
            raise ConnectionError(
                "Control connection closed before message was received"
            )
        buf += chunk.decode("utf-8")

    line, remainder = buf.split("\n", 1)
    _SOCKET_BUFFERS[connection] = remainder
    message = json.loads(line)
    validate_message(message)
    return message


def has_buffered_message(connection: socket.socket) -> bool:
    """Return True if an unread newline-terminated message is buffered for this socket."""
    return "\n" in _SOCKET_BUFFERS.get(connection, "")


def send_shutdown_message(connection: socket.socket) -> None:
    """Master sends a shutdown message to worker nodes to signal clean termination."""
    send_json_message(connection, {"type": MESSAGE_TYPE_SHUTDOWN})


def send_ready_message(
    connection: socket.socket,
    master_ip_address: str,
    file_transfer_port: int,
    execute_command: str | None = None,
) -> None:
    """Master sends a ready message with this node's file-transfer daemon port."""
    msg = {
        "type": MESSAGE_TYPE_READY,
        "master_ip_address": master_ip_address,
        "file_transfer_port": file_transfer_port,
    }
    if execute_command:
        msg["execute_command"] = execute_command
    send_json_message(connection, msg)


def receive_ready_message(
    connection: socket.socket, timeout: float | None = None
) -> dict:
    """Worker receives the ready message from the master."""
    message = receive_json_message(connection, timeout=timeout)
    if message.get("type") == MESSAGE_TYPE_SHUTDOWN:
        raise MasterShutdownError("Master sent shutdown signal")
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


# ------ Binary handshake messaging functions ------


def send_binary_info_request(connection: socket.socket, binary_name: str) -> None:
    """Master asks a worker for its platform info and whether it has the binary."""
    send_json_message(
        connection,
        {
            "type": MESSAGE_TYPE_BINARY_INFO_REQUEST,
            "binary_name": binary_name,
        },
    )


def receive_binary_info_request(connection: socket.socket) -> dict:
    """Worker receives the master's binary_info_request message."""
    msg = receive_json_message(connection)
    if msg.get("type") == MESSAGE_TYPE_SHUTDOWN:
        raise MasterShutdownError("Master sent shutdown signal")
    if msg.get("type") != MESSAGE_TYPE_BINARY_INFO_REQUEST:
        raise ValueError(f"Expected binary_info_request message, got: {msg!r}")
    return msg


def send_binary_info(
    connection: socket.socket,
    os_name: str,
    arch: str,
    binary_name: str,
    has_binary: bool,
    md5: str | None,
) -> None:
    """Worker replies with its platform and binary status (md5 is None if absent)."""
    send_json_message(
        connection,
        {
            "type": MESSAGE_TYPE_BINARY_INFO,
            "os_name": os_name,
            "arch": arch,
            "binary_name": binary_name,
            "has_binary": has_binary,
            "md5": md5 or "",
        },
    )


def receive_binary_info(connection: socket.socket) -> dict:
    """Master receives the worker's binary_info message."""
    msg = receive_json_message(connection)
    if msg.get("type") != MESSAGE_TYPE_BINARY_INFO:
        raise ValueError(f"Expected binary_info message, got: {msg!r}")
    return msg


def send_binary_ready(connection: socket.socket) -> None:
    """Master tells the worker that binary setup is complete (present or just sent)."""
    send_json_message(connection, {"type": MESSAGE_TYPE_BINARY_READY})


def receive_binary_ready(connection: socket.socket) -> None:
    """Worker waits for the master's binary_ready signal before proceeding."""
    msg = receive_json_message(connection)
    if msg.get("type") == MESSAGE_TYPE_SHUTDOWN:
        raise MasterShutdownError("Master sent shutdown signal")
    if msg.get("type") != MESSAGE_TYPE_BINARY_READY:
        raise ValueError(f"Expected binary_ready message, got: {msg!r}")


# ------ Worker Telemetry ------


def send_worker_telemetry(connection: socket.socket, telemetry: dict) -> None:
    """Worker sends hardware benchmark & specs telemetry to master."""
    send_json_message(connection, {"type": MESSAGE_TYPE_WORKER_TELEMETRY, **telemetry})


def receive_worker_telemetry(connection: socket.socket) -> dict:
    """Master receives worker telemetry."""
    msg = receive_json_message(connection)
    validate_message(msg)
    if msg["type"] != MESSAGE_TYPE_WORKER_TELEMETRY:
        raise ValueError(f"Expected worker_telemetry, got {msg['type']}")
    return msg


# ------ PIN-Based Pairing ------


def generate_pin() -> str:
    """Generate a random 4-digit PIN excluding common sequences."""
    bad_pins = {"0000", "1111", "1234", "4321", "9876", "0123"}
    while True:
        pin = f"{random.randint(0, 9999):04d}"
        if pin not in bad_pins:
            return pin


def send_pin_challenge(connection: socket.socket, master_name: str) -> None:
    """Master sends pairing challenge to newly connected worker."""
    send_json_message(
        connection,
        {"type": MESSAGE_TYPE_PIN_CHALLENGE, "master_name": master_name},
    )


def receive_pin_challenge(connection: socket.socket) -> dict:
    """Worker receives pairing challenge from master."""
    msg = receive_json_message(connection)
    validate_message(msg)
    if msg["type"] != MESSAGE_TYPE_PIN_CHALLENGE:
        raise ValueError(f"Expected pin_challenge, got {msg['type']}")
    return msg


def send_pin_response(connection: socket.socket, pin: str) -> None:
    """Worker sends entered PIN response to master."""
    send_json_message(connection, {"type": MESSAGE_TYPE_PIN_RESPONSE, "pin": pin})


def receive_pin_response(connection: socket.socket) -> dict:
    """Master receives PIN response from worker."""
    msg = receive_json_message(connection)
    validate_message(msg)
    if msg["type"] != MESSAGE_TYPE_PIN_RESPONSE:
        raise ValueError(f"Expected pin_response, got {msg['type']}")
    return msg


def send_pin_accepted(connection: socket.socket) -> None:
    """Master notifies worker that PIN pairing succeeded."""
    send_json_message(connection, {"type": MESSAGE_TYPE_PIN_ACCEPTED})


def send_pin_rejected(
    connection: socket.socket, reason: str, attempts_left: int
) -> None:
    """Master notifies worker that PIN pairing failed."""
    send_json_message(
        connection,
        {
            "type": MESSAGE_TYPE_PIN_REJECTED,
            "reason": reason,
            "attempts_left": attempts_left,
        },
    )
