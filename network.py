"""Network helpers for choosing the correct local IP on multi-homed hosts."""

import socket


def get_local_ip_for_peer(peer_ip_address: str) -> str:
    """Return the local IP address used to reach a peer on the LAN."""
    connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        connection.connect((peer_ip_address, 9))
        return connection.getsockname()[0]
    finally:
        connection.close()
