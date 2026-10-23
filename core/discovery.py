"""Network discovery helpers: mDNS auto-discovery and UDP broadcast beacons."""

from __future__ import annotations

import socket
import struct
import threading
import time

from core.constants import (
    DISCOVERY_BEACON_TYPE,
    MASTER_OFFER_TYPE,
    MDNS_ADDR,
    MDNS_KEYWORD_MASTER,
    MDNS_MASTER_FQDN,
    MDNS_PORT,
    MDNS_SERVICE_MASTER,
    UDP_DISCOVERY_PORT,
)


def get_local_ip() -> str:
    """Determine the local machine's primary outbound LAN IP."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"


def get_local_ip_for_peer(peer_ip_address: str) -> str:
    """Return the local IP address used to reach a peer on the LAN."""
    peer_ip = peer_ip_address.split(":")[0]
    connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        connection.connect((peer_ip, 9))
        return connection.getsockname()[0]
    finally:
        connection.close()


def encode_dns_label(name: str) -> bytes:
    """Encode a DNS name into label wire format (no compression)."""
    out = b""
    for part in name.rstrip(".").split("."):
        enc = part.encode("utf-8")
        out += bytes([len(enc)]) + enc
    out += b"\x00"
    return out


def build_a_record(fqdn: str, ipv4: str, ttl: int = 4500) -> bytes:
    """Return a DNS A-record answer RR in wire format."""
    name = encode_dns_label(fqdn)
    rdata = socket.inet_aton(ipv4)
    rr = struct.pack("!HHIH", 1, 0x8001, ttl, 4)  # type A, class IN+flush
    return name + rr + rdata


def build_ptr_record(service: str, instance: str, ttl: int = 4500) -> bytes:
    """Return a DNS PTR-record answer RR in wire format."""
    name = encode_dns_label(service)
    rdata = encode_dns_label(instance)
    rr = struct.pack("!HHIH", 12, 0x0001, ttl, len(rdata))  # type PTR
    return name + rr + rdata


def build_mdns_announcement(
    local_ip: str,
    fqdn: str = MDNS_MASTER_FQDN,
    service: str = MDNS_SERVICE_MASTER,
) -> bytes:
    """Assemble a full mDNS (DNS-SD) response packet."""
    a_rec = build_a_record(fqdn, local_ip)
    ptr_rec = build_ptr_record(service, f"{fqdn.split('.')[0]}.{service}")

    header = struct.pack(
        "!HHHHHH",
        0x0000,  # transaction id (always 0 for mDNS)
        0x8400,  # flags: response + authoritative
        0,  # questions
        2,  # answer RRs
        0,  # authority RRs
        0,  # additional RRs
    )
    return header + ptr_rec + a_rec


def parse_dns_name(data: bytes, offset: int) -> tuple[str, int]:
    """Parse a DNS wire-format name starting at offset."""
    labels = []
    visited = set()

    while True:
        if offset >= len(data):
            break
        length = data[offset]

        if length == 0:
            offset += 1
            break
        elif (length & 0xC0) == 0xC0:
            if offset + 1 >= len(data):
                break
            ptr = ((length & 0x3F) << 8) | data[offset + 1]
            offset += 2
            if ptr in visited:
                break
            visited.add(ptr)
            label, _ = parse_dns_name(data, ptr)
            labels.append(label)
            break
        else:
            offset += 1
            end = offset + length
            labels.append(data[offset:end].decode("utf-8", errors="replace"))
            offset = end

    return ".".join(labels), offset


def extract_a_record_ip(data: bytes, keyword: str = MDNS_KEYWORD_MASTER) -> str | None:
    """Walk through DNS answer RRs looking for an A record whose name matches keyword."""
    try:
        if len(data) < 12:
            return None
        if keyword.encode("utf-8") not in data:
            return None

        qdcount = struct.unpack_from("!H", data, 4)[0]
        ancount = struct.unpack_from("!H", data, 6)[0]

        offset = 12
        for _ in range(qdcount):
            _, offset = parse_dns_name(data, offset)
            offset += 4  # QTYPE + QCLASS

        for _ in range(ancount):
            name, offset = parse_dns_name(data, offset)
            if offset + 10 > len(data):
                break
            rtype, rclass, ttl, rdlen = struct.unpack_from("!HHIH", data, offset)
            offset += 10
            rdata = data[offset : offset + rdlen]
            offset += rdlen

            if rtype == 1 and rdlen == 4:  # A record
                ip = socket.inet_ntoa(rdata)
                if keyword.lower() in name.lower():
                    return ip
    except Exception:
        pass
    return None


def make_mdns_sender_socket() -> socket.socket:
    """Create a UDP socket for sending mDNS multicast announcements."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except AttributeError:
        pass
    try:
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 255)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
    except OSError:
        pass
    return sock


def make_mdns_listener_socket() -> socket.socket:
    """Create a UDP socket listening on the mDNS multicast group (224.0.0.251:5353)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except AttributeError:
        pass
    try:
        sock.bind(("", MDNS_PORT))
    except OSError:
        sock.bind(("0.0.0.0", MDNS_PORT))

    try:
        mreq = struct.pack("4sL", socket.inet_aton(MDNS_ADDR), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    except OSError:
        try:
            local_ip = get_local_ip()
            mreq = struct.pack(
                "4s4s", socket.inet_aton(MDNS_ADDR), socket.inet_aton(local_ip)
            )
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        except OSError:
            pass
    return sock


class MdnsAnnouncer:
    """Background thread announcing node presence over mDNS multicast."""

    def __init__(
        self,
        local_ip: str | None = None,
        fqdn: str = MDNS_MASTER_FQDN,
        service: str = MDNS_SERVICE_MASTER,
        interval_seconds: float = 2.0,
    ) -> None:
        self.local_ip = local_ip or get_local_ip()
        self.fqdn = fqdn
        self.service = service
        self.interval_seconds = interval_seconds
        self.running = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self.running = True
        self._thread = threading.Thread(target=self._announce_loop, daemon=True)
        self._thread.start()

    def _announce_loop(self) -> None:
        packet = build_mdns_announcement(self.local_ip, self.fqdn, self.service)
        try:
            with make_mdns_sender_socket() as sock:
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                except Exception:
                    pass
                while self.running:
                    try:
                        sock.sendto(packet, (MDNS_ADDR, MDNS_PORT))
                    except Exception:
                        pass
                    try:
                        sock.sendto(packet, ("255.255.255.255", MDNS_PORT))
                    except Exception:
                        pass
                    try:
                        sock.sendto(packet, ("127.0.0.1", MDNS_PORT))
                    except Exception:
                        pass
                    time.sleep(self.interval_seconds)
        except Exception:
            pass

    def stop(self) -> None:
        self.running = False


def discover_master_ip(timeout_seconds: float = 60.0) -> str:
    """Listen on mDNS multicast group for Master announcement; return Master IP or raise TimeoutError."""
    start_time = time.time()
    deadline = start_time + timeout_seconds

    with make_mdns_listener_socket() as sock:
        sock.settimeout(1.0)
        print(
            f"Searching for Master via mDNS on {MDNS_ADDR}:{MDNS_PORT} (timeout: {int(timeout_seconds)}s)..."
        )
        while time.time() < deadline:
            try:
                data, addr = sock.recvfrom(4096)
                master_ip = extract_a_record_ip(data, MDNS_KEYWORD_MASTER)
                if master_ip:
                    print(f"\n[mDNS DISCOVERY] Master found at {master_ip}!")
                    return master_ip
            except socket.timeout:
                continue
            except Exception:
                time.sleep(0.5)

    raise TimeoutError(
        f"No Master device found via mDNS within {int(timeout_seconds)} seconds!"
    )


def start_udp_discovery_listener(master_ip: str, control_port: int) -> threading.Thread:
    """Listen for UDP broadcast beacons and reply with master info."""

    def listener():
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except Exception:
                pass
        sock.bind(("", UDP_DISCOVERY_PORT))
        while True:
            try:
                data, addr = sock.recvfrom(1024)
                if data.decode("utf-8").strip() == DISCOVERY_BEACON_TYPE:
                    reply = f"{MASTER_OFFER_TYPE}:{master_ip}:{control_port}"
                    sock.sendto(reply.encode("utf-8"), addr)
            except Exception:
                pass

    t = threading.Thread(target=listener, daemon=True)
    t.start()
    return t


def discover_master_udp(timeout_seconds: float = 30.0) -> tuple[str, int]:
    """Broadcast UDP beacons and wait for master offer."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(2.0)

    end_time = time.time() + timeout_seconds
    while time.time() < end_time:
        try:
            sock.sendto(
                DISCOVERY_BEACON_TYPE.encode("utf-8"),
                ("255.255.255.255", UDP_DISCOVERY_PORT),
            )
            data, addr = sock.recvfrom(1024)
            msg = data.decode("utf-8").strip()
            if msg.startswith(MASTER_OFFER_TYPE):
                parts = msg.split(":")
                if len(parts) >= 3:
                    return parts[1], int(parts[2])
        except (socket.timeout, OSError):
            pass
        time.sleep(1.0)
    raise TimeoutError("UDP discovery timed out")
