"""Master connection establishment, discovery fallback, and PIN/password pairing."""

from __future__ import annotations

import socket
import sys

from core.constants import (
    FIXED_PORT,
    MESSAGE_TYPE_PIN_ACCEPTED,
    MESSAGE_TYPE_PIN_REJECTED,
)
from core.discovery import discover_master_ip, discover_master_udp
from core.messaging import (
    receive_json_message,
    receive_pin_challenge,
    send_auth_message,
    send_pin_response,
)


def connect_to_master(
    master_ip_address: str, port: int = FIXED_PORT, bind_ip: str | None = None
) -> socket.socket:
    """Connect to the master node on the fixed control port."""
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if bind_ip:
        connection.bind((bind_ip, 0))
    connection.connect((master_ip_address, port))
    return connection


def discover_master_address(
    timeout_mdns: float = 10.0, timeout_udp: float = 30.0
) -> str:
    """Discover master node IP using mDNS first, falling back to UDP broadcast."""
    print("No master IP address specified. Auto-discovering Master...")
    try:
        master_ip = discover_master_ip(timeout_seconds=timeout_mdns)
        print(f"Found master via mDNS: {master_ip}")
        return master_ip
    except TimeoutError:
        print("mDNS timed out. Trying UDP broadcast (works on mobile hotspots)...")
        try:
            master_ip, _ = discover_master_udp(timeout_seconds=timeout_udp)
            print(f"Found master via UDP broadcast: {master_ip}")
            return master_ip
        except TimeoutError:
            print(
                "Error: Could not discover master on the network.",
                file=sys.stderr,
            )
            sys.exit(1)


def perform_handshake_authentication(
    connection: socket.socket,
    shared_secret: str | None = None,
    password: str | None = None,
    cached_pin: str | None = None,
) -> str | None:
    """Perform shared secret authentication or PIN-based pairing.

    Returns the cached PIN string if entered by the user, or None.
    """
    if shared_secret:
        send_auth_message(connection, shared_secret)
        return cached_pin

    challenge = receive_pin_challenge(connection)
    master_name = challenge.get("master_name", "unknown")

    if cached_pin:
        pin = cached_pin
    elif password:
        pin = password
        cached_pin = pin
    else:
        pin = input(
            f"Master '{master_name}' requests pairing. Enter Password/PIN shown on master's screen: "
        ).strip()
        cached_pin = pin

    send_pin_response(connection, pin.strip())

    resp = receive_json_message(connection)
    if resp.get("type") == MESSAGE_TYPE_PIN_REJECTED:
        reason = resp.get("reason", "Invalid PIN/Password")
        if password is not None:
            print(f"Wrong password: {reason}. Exiting.", file=sys.stderr)
            try:
                connection.close()
            except Exception:
                pass
            sys.exit(1)
        else:
            print(f"Pairing rejected: {reason}. Retrying...", file=sys.stderr)
            try:
                connection.close()
            except Exception:
                pass
            raise ConnectionError(f"Pairing rejected: {reason}")
    elif resp.get("type") != MESSAGE_TYPE_PIN_ACCEPTED:
        print(
            f"Unexpected response during PIN pairing: {resp}",
            file=sys.stderr,
        )
        sys.exit(1)

    return cached_pin
