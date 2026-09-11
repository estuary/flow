#!/usr/bin/env python3
"""Test-only stand-ins for the internet, run by egress-netns-test.sh.

Two things the probes need and the real internet will not reliably give:

- a name with a TTL below the clamp floor, so the floor is observable. Public
  names change their TTLs; this one does not.
- a server that holds an idle TCP connection open for longer than the clamp
  floor, so "a connection opened before expiry stays open" can be told apart
  from "the peer hung up". Public servers close idle connections in seconds.

Everything else is forwarded to the real upstream, so probes that want the
real DNS path (a public name, a name that answers with an RFC1918 address)
still get it. Runs in the host namespace; it is not part of the sandbox.
"""

import argparse
import socket
import struct
import threading


def endpoint(raw):
    address, port = raw.rsplit(":", 1)
    return address, int(port)


def serve_dns(listen, upstream, name, address, ttl):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(listen)
    wanted = name.lower().rstrip(".")

    while True:
        query, client = sock.recvfrom(4096)
        try:
            answer = answer_for(query, wanted, address, ttl)
            if answer is None:
                answer = forward(query, upstream)
        except (OSError, IndexError, struct.error):
            continue
        if answer is not None:
            sock.sendto(answer, client)


def answer_for(query, wanted, address, ttl):
    """One A record for the one name we are authoritative for, else None."""
    if len(query) < 12:
        return None
    cursor = 12
    labels = []
    while query[cursor] != 0:
        length = query[cursor]
        labels.append(query[cursor + 1:cursor + 1 + length].decode())
        cursor += 1 + length
    cursor += 1
    qtype, _qclass = struct.unpack("!HH", query[cursor:cursor + 4])
    cursor += 4

    if ".".join(labels).lower() != wanted or qtype != 1:
        return None

    header = struct.pack("!HHHHHH", struct.unpack("!H", query[:2])[0], 0x8180, 1, 1, 0, 0)
    record = struct.pack("!HHHIH", 0xC00C, 1, 1, ttl, 4) + socket.inet_aton(address)
    return header + query[12:cursor] + record


def forward(query, upstream):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(5)
    try:
        sock.sendto(query, upstream)
        return sock.recv(4096)
    except OSError:
        return None
    finally:
        sock.close()


def serve_tcp(listen):
    """Answers anything with a status line and never hangs up first."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(listen)
    sock.listen(16)

    while True:
        peer, _ = sock.accept()
        threading.Thread(target=hold, args=(peer,), daemon=True).start()


def hold(peer):
    try:
        while True:
            if not peer.recv(4096):
                return
            peer.sendall(b"HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n")
    except OSError:
        return
    finally:
        peer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dns-listen", required=True, type=endpoint)
    parser.add_argument("--upstream", required=True, type=endpoint)
    parser.add_argument("--name", required=True)
    parser.add_argument("--address", required=True)
    parser.add_argument("--ttl", type=int, default=5)
    parser.add_argument("--tcp-listen", required=True, type=endpoint)
    args = parser.parse_args()

    threading.Thread(target=serve_tcp, args=(args.tcp_listen,), daemon=True).start()
    serve_dns(args.dns_listen, args.upstream, args.name, args.address, args.ttl)


if __name__ == "__main__":
    main()
