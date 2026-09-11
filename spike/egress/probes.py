#!/usr/bin/env python3
"""Egress probes: one named set of expectations, one JSON line per probe.

Runs unchanged in a veth netns on the host (WP02) and inside the
derive-python guest (WP07), so: Python 3.12 stdlib only, no imports beyond it,
and every address, name and limit comes from argv.

Output is one line per probe:

    {"probe": str, "expect": str, "result": str, "pass": bool, "ms": int}

Exit 0 iff every probe that ran passed.

A dropped packet and a refused connection are different results, and the
difference is the whole point: `timeout` means the ruleset dropped it, while
`refused` means the SYN reached a host that answered RST. Nothing here treats
one as the other.

Two probes need root (raw sockets): `icmp-blocked` and `spoofed-source`. With
`--root-probes auto` they run only when euid is 0 and are otherwise left out
of the output entirely; with `only` they are the whole run, which is how WP07
repeats them under a second helper configuration without repeating the set.
"""

import argparse
import errno
import json
import os
import random
import socket
import struct
import sys
import time

# Long enough for a TCP handshake across the tap, short enough that a set of
# dropped destinations does not dominate the run. A `timeout` result is a
# lower bound on the real failure latency, never the kernel's own SYN budget.
DEFAULT_TIMEOUT = 3.0


def tcp_connect(address, port, timeout):
    """"connected" | "refused" | "timeout" | "unreachable" | "error:NAME"."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((address, port))
        return "connected"
    except socket.timeout:
        return "timeout"
    except OSError as error:
        return _socket_error(error)
    finally:
        sock.close()


def _socket_error(error):
    if error.errno in (errno.ECONNREFUSED,):
        return "refused"
    if error.errno in (errno.EHOSTUNREACH, errno.ENETUNREACH):
        return "unreachable"
    return "error:%s" % errno.errorcode.get(error.errno, error.errno)


def resolve(name):
    """"ok:A,B" | "error:NAME"."""
    try:
        answers = socket.getaddrinfo(name, None, family=socket.AF_INET,
                                     type=socket.SOCK_STREAM)
    except socket.gaierror as error:
        return "error:gai%d" % error.args[0]
    except OSError as error:
        return _socket_error(error)
    return "ok:" + ",".join(sorted({answer[4][0] for answer in answers}))


def dns_query(nameserver, name, timeout, qtype=1):
    """A raw A query, which is the only way to see the TTL the resolver wrote.

    Returns (status, [(address, ttl)], answer count), where the count covers
    every record type and the list only A records.
    """
    query_id = random.randrange(0, 65536)
    question = b"".join(
        bytes([len(label)]) + label.encode() for label in name.split(".")
    ) + b"\x00"
    query = struct.pack("!HHHHHH", query_id, 0x0100, 1, 0, 0, 0) + question
    query += struct.pack("!HH", qtype, 1)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        sock.sendto(query, (nameserver, 53))
        response, _ = sock.recvfrom(4096)
    except socket.timeout:
        return "timeout", [], 0
    except OSError as error:
        return _socket_error(error), [], 0
    finally:
        sock.close()

    answer_id, flags, questions, answers = struct.unpack("!HHHH", response[:8])
    if answer_id != query_id:
        return "error:id-mismatch", [], 0
    rcode = flags & 0xF
    if rcode != 0:
        return "rcode:%d" % rcode, [], 0

    cursor = 12
    for _ in range(questions):
        cursor = _skip_name(response, cursor) + 4

    records = []
    for _ in range(answers):
        cursor = _skip_name(response, cursor)
        rtype, _rclass, ttl, rdlength = struct.unpack("!HHIH", response[cursor:cursor + 10])
        cursor += 10
        if rtype == 1 and rdlength == 4:
            records.append((socket.inet_ntoa(response[cursor:cursor + 4]), ttl))
        cursor += rdlength
    return "ok", records, answers


def _skip_name(message, cursor):
    while True:
        length = message[cursor]
        if length & 0xC0 == 0xC0:
            return cursor + 2
        cursor += 1
        if length == 0:
            return cursor
        cursor += length


def icmp_echo(address, timeout):
    """"reply" | "timeout" | "error:NAME". Needs root: SOCK_RAW."""
    packet = struct.pack("!BBHHH", 8, 0, 0, os.getpid() & 0xFFFF, 1) + b"flow-sandbox"
    packet = packet[:2] + struct.pack("!H", _checksum(packet)) + packet[4:]

    sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)
    sock.settimeout(timeout)
    try:
        sock.sendto(packet, (address, 0))
        sock.recvfrom(1024)
        return "reply"
    except socket.timeout:
        return "timeout"
    except OSError as error:
        return _socket_error(error)
    finally:
        sock.close()


def spoofed_udp(source, address, port, payload=b"flow-sandbox-spoof"):
    """Sends one UDP datagram with a forged source. Needs root: IP_HDRINCL.

    The guest cannot see what happens next; the helper's anti-spoof counter is
    the observation. This probe only reports that the packet was handed to the
    kernel.
    """
    udp = struct.pack("!HHHH", 4321, port, 8 + len(payload), 0) + payload
    header = struct.pack(
        "!BBHHHBBH4s4s",
        0x45, 0, 20 + len(udp), 0, 0, 64, socket.IPPROTO_UDP, 0,
        socket.inet_aton(source), socket.inet_aton(address),
    )
    sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_RAW)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)
    try:
        sock.sendto(header + udp, (address, 0))
        return "sent"
    except OSError as error:
        return _socket_error(error)
    finally:
        sock.close()


def _checksum(data):
    if len(data) % 2:
        data += b"\x00"
    total = sum(struct.unpack("!%dH" % (len(data) // 2), data))
    total = (total & 0xFFFF) + (total >> 16)
    return ~total & 0xFFFF


def await_inbound(port, wait):
    """Binds a listener and reports whether anything connected within `wait`."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(wait)
    try:
        sock.bind(("0.0.0.0", port))
        sock.listen(1)
        peer, address = sock.accept()
        peer.close()
        return "connection-from:%s" % address[0]
    except socket.timeout:
        return "no-connection"
    except OSError as error:
        return _socket_error(error)
    finally:
        sock.close()


# Each expectation is a predicate over the result string. `blocked` is
# deliberately strict: `refused` would mean the packet was forwarded.
EXPECTATIONS = {
    "connect-ok": lambda result: result == "connected",
    "blocked": lambda result: result == "timeout",
    "reachable": lambda result: result in ("connected", "refused"),
    "dns-ok": lambda result: result.startswith("ok:"),
    "dns-error": lambda result: result.startswith(("error:", "rcode:")),
    "no-ipv6": lambda result: result != "connected",
    "no-inbound": lambda result: result == "no-connection",
    "sent": lambda result: result == "sent",
    "in-range": lambda result: result.startswith("ttl:"),
    "aaaa-empty": lambda result: result == "empty",
    "rate-limited": lambda result: result.startswith("limited:"),
}


class Run:
    def __init__(self):
        self.failures = 0

    def probe(self, name, expect, call):
        started = time.monotonic()
        result = call()
        elapsed = int((time.monotonic() - started) * 1000)
        passed = EXPECTATIONS[expect](result)
        self.failures += 0 if passed else 1
        print(json.dumps({
            "probe": name, "expect": expect, "result": result,
            "pass": passed, "ms": elapsed,
        }), flush=True)
        return result


def require(args, *names):
    missing = [name for name in names if getattr(args, name) in (None, "")]
    if missing:
        sys.exit("probes.py: --set %s requires %s" % (
            args.set, ", ".join("--" + name.replace("_", "-") for name in missing)))


def set_public(run, args):
    require(args, "helper_ip", "uplink_ip", "gateway", "public_name", "rfc1918_name",
            "short_ttl_name", "nginx_ip", "metadata_ip", "ipv6_ip", "spoof_source")

    if args.root_probes == "only":
        return set_public_root(run, args, args.nginx_ip)

    resolved = run.probe("dns-public", "dns-ok", lambda: resolve(args.public_name))
    address = resolved.split(":", 1)[1].split(",")[0] if resolved.startswith("ok:") else None

    if address:
        run.probe("connect-resolved-443", "connect-ok",
                  lambda: tcp_connect(address, 443, args.timeout))
        run.probe("connect-resolved-25", "blocked",
                  lambda: tcp_connect(address, 25, args.timeout))
    run.probe("connect-unresolved-ip", "blocked",
              lambda: tcp_connect(args.nginx_ip, args.nginx_port, args.timeout))
    run.probe("dns-rfc1918", "dns-error", lambda: resolve(args.rfc1918_name))
    run.probe("connect-metadata", "blocked",
              lambda: tcp_connect(args.metadata_ip, 80, args.timeout))
    run.probe("connect-helper-tap", "blocked",
              lambda: tcp_connect(args.helper_ip, 443, args.timeout))
    run.probe("connect-helper-uplink", "blocked",
              lambda: tcp_connect(args.uplink_ip, 443, args.timeout))
    run.probe("dns-helper-uplink", "blocked",
              lambda: dns_query(args.uplink_ip, args.public_name, args.timeout)[0])
    run.probe("dns-aaaa-empty", "aaaa-empty", lambda: aaaa_empty(args))
    run.probe("connect-gateway", "blocked",
              lambda: tcp_connect(args.gateway, args.gateway_port, args.timeout))
    run.probe("connect-ipv6", "no-ipv6", lambda: connect_ipv6(args.ipv6_ip, args.timeout))
    run.probe("inbound-listener", "no-inbound",
              lambda: await_inbound(args.inbound_port, args.inbound_wait))

    if args.root_probes != "skip" and os.geteuid() == 0:
        set_public_root(run, args, address or args.nginx_ip)

    run.probe("dns-ttl-clamped", "in-range",
              lambda: ttl_in_range(args, args.short_ttl_name))
    if args.slow:
        ttl_expiry(run, args)


def set_public_root(run, args, address):
    """The two that need raw sockets, and so guest root."""
    run.probe("icmp-blocked", "blocked", lambda: icmp_echo(address, args.timeout))
    run.probe("spoofed-source", "sent",
              lambda: spoofed_udp(args.spoof_source, args.nginx_ip, 9))


def aaaa_empty(args):
    """AAAA answers NOERROR with nothing in it: no IPv6 crosses the tap, and a
    resolvable AAAA would only cost the guest a connect attempt to reach that
    conclusion slowly."""
    status, _, answers = dns_query(args.helper_ip, args.public_name,
                                   args.timeout, qtype=28)
    if status != "ok":
        return status
    return "empty" if answers == 0 else "records:%d" % answers


def connect_ipv6(address, timeout):
    try:
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    except OSError as error:
        return _socket_error(error)
    sock.settimeout(timeout)
    try:
        sock.connect((address, 443))
        return "connected"
    except socket.timeout:
        return "timeout"
    except OSError as error:
        return _socket_error(error)
    finally:
        sock.close()


def ttl_in_range(args, name):
    """The TTL the guest sees is the timeout the set element got."""
    status, records, _ = dns_query(args.helper_ip, name, args.timeout)
    if status != "ok" or not records:
        return status if status != "ok" else "no-records"
    ttl = records[0][1]
    if not args.ttl_floor <= ttl <= args.ttl_cap:
        return "out-of-range:%d" % ttl
    return "ttl:%d" % ttl


def ttl_expiry(run, args):
    """The set element expires; a connection opened before it does not."""
    status, records, _ = dns_query(args.helper_ip, args.short_ttl_name, args.timeout)
    if status != "ok" or not records:
        run.probe("ttl-resolve", "dns-ok", lambda: status)
        return
    address = records[0][0]

    held = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    held.settimeout(args.timeout)
    run.probe("ttl-connect-before", "connect-ok",
              lambda: hold_open(held, address, args.ttl_port))

    time.sleep(args.ttl_floor + 5)
    run.probe("ttl-connect-after-expiry", "blocked",
              lambda: tcp_connect(address, args.ttl_port, args.timeout))
    run.probe("ttl-held-connection-survives", "connect-ok",
              lambda: http_head(held, args.short_ttl_name))
    held.close()
    run.probe("ttl-reresolve-connect", "connect-ok",
              lambda: reresolve_connect(args))


def hold_open(sock, address, port):
    try:
        sock.connect((address, port))
        return "connected"
    except socket.timeout:
        return "timeout"
    except OSError as error:
        return _socket_error(error)


def http_head(sock, name):
    """An established connection keeps passing traffic after its element goes.

    A response is the proof: with a drop rule there is no reset to observe, so
    silence and success have to be told apart by the bytes coming back.
    """
    try:
        sock.sendall(b"HEAD / HTTP/1.0\r\nHost: %s\r\n\r\n" % name.encode())
        return "connected" if sock.recv(64) else "timeout"
    except socket.timeout:
        return "timeout"
    except OSError as error:
        return _socket_error(error)


def reresolve_connect(args):
    status, records, _ = dns_query(args.helper_ip, args.short_ttl_name, args.timeout)
    if status != "ok" or not records:
        return status if status != "ok" else "no-records"
    return tcp_connect(records[0][0], args.ttl_port, args.timeout)


def set_none(run, args):
    require(args, "public_name", "nginx_ip", "metadata_ip")

    run.probe("dns-any", "dns-error", lambda: resolve(args.public_name))
    run.probe("connect-raw-ip", "blocked",
              lambda: tcp_connect(args.nginx_ip, args.nginx_port, args.timeout))
    run.probe("connect-metadata", "blocked",
              lambda: tcp_connect(args.metadata_ip, 80, args.timeout))


def set_declared(run, args):
    require(args, "nginx_ip", "undeclared_ip")

    run.probe("connect-declared-port", "connect-ok",
              lambda: tcp_connect(args.nginx_ip, args.nginx_port, args.timeout))
    run.probe("connect-declared-other-port", "blocked",
              lambda: tcp_connect(args.nginx_ip, args.nginx_other_port, args.timeout))
    run.probe("connect-undeclared-ip", "blocked",
              lambda: tcp_connect(args.undeclared_ip, args.nginx_port, args.timeout))


def set_ratelimit(run, args):
    require(args, "fanout_ips")
    destinations = args.fanout_ips.split(",")

    # Sequential, because which destinations get the set's slots is decided by
    # arrival order. The first `--dest-limit` are expected through; every one
    # after that finds the set full and falls through to the chain's policy.
    accepted = []
    for index, address in enumerate(destinations):
        expect = "reachable" if index < args.dest_limit else "blocked"
        result = run.probe("fanout-%02d" % (index + 1), expect,
                           lambda address=address: tcp_connect(address, args.fanout_port,
                                                               args.timeout))
        if result in ("connected", "refused"):
            accepted.append(address)

    if not accepted:
        return
    run.probe("rate-limit", "rate-limited", lambda: rate_limit(args, accepted[0]))

    if args.slow:
        # The `limit rate` bucket refills and the `dests` elements time out on
        # the same one-minute scale; one wait covers both.
        time.sleep(65)
        run.probe("rate-recovers", "reachable",
                  lambda: tcp_connect(accepted[0], args.fanout_port, args.timeout))
        run.probe("fanout-recovers", "reachable",
                  lambda: tcp_connect(destinations[-1], args.fanout_port, args.timeout))


def rate_limit(args, address):
    """What the limit bounds is the rate, so the rate is what is measured.

    Counting failures is the wrong instrument: nft drops the SYN, and TCP
    retransmits it a second later into a bucket that has refilled by then, so
    at 60/minute the guest sees slow connections rather than errors. The claim
    that holds either way is that connections do not get through faster than
    the configured rate, burst included.
    """
    reached = 0
    blocked = 0
    started = time.monotonic()
    for _ in range(args.rate_attempts):
        if tcp_connect(address, args.fanout_port, args.timeout) in ("connected", "refused"):
            reached += 1
        else:
            blocked += 1
    per_minute = reached * 60.0 / max(time.monotonic() - started, 0.001)

    if per_minute > args.rate_limit + 10:
        return "over:%d-reached-%d-blocked-%d-per-minute" % (reached, blocked, per_minute)
    return "limited:%d-reached-%d-blocked-%d-per-minute" % (reached, blocked, per_minute)


SETS = {
    "public": set_public,
    "none": set_none,
    "declared": set_declared,
    "ratelimit": set_ratelimit,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--set", required=True, choices=sorted(SETS))
    parser.add_argument("--helper-ip", help="the helper's tap-side address")
    parser.add_argument("--uplink-ip", help="the helper's uplink address")
    parser.add_argument("--gateway", help="the bridge gateway, i.e. the host")
    parser.add_argument("--gateway-port", type=int, default=9000,
                        help="a port the reactor listens on there")
    parser.add_argument("--public-name")
    parser.add_argument("--rfc1918-name", help="a name that answers with an RFC1918 address")
    parser.add_argument("--short-ttl-name", help="a name whose TTL is below the floor")
    parser.add_argument("--nginx-ip")
    parser.add_argument("--nginx-port", type=int, default=443)
    parser.add_argument("--nginx-other-port", type=int, default=80)
    parser.add_argument("--undeclared-ip", help="an address the declared CIDRs do not cover")
    parser.add_argument("--metadata-ip", help="the cloud metadata server")
    parser.add_argument("--ipv6-ip")
    parser.add_argument("--spoof-source", help="a source address the guest does not own")
    parser.add_argument("--fanout-ips", help="comma-separated, in the order to try them")
    parser.add_argument("--fanout-port", type=int, default=443)
    parser.add_argument("--dest-limit", type=int, default=5)
    parser.add_argument("--rate-limit", type=int, default=60)
    parser.add_argument("--rate-attempts", type=int, default=80)
    parser.add_argument("--ttl-port", type=int, default=80,
                        help="a port the short-TTL name's host answers on")
    parser.add_argument("--ttl-floor", type=int, default=90)
    parser.add_argument("--ttl-cap", type=int, default=3600)
    parser.add_argument("--inbound-port", type=int, default=34567)
    parser.add_argument("--inbound-wait", type=float, default=2.0)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--root-probes", choices=("auto", "only", "skip"), default="auto")
    parser.add_argument("--slow", action="store_true",
                        help="also run the probes that wait out a TTL or a rate window")
    args = parser.parse_args()

    run = Run()
    SETS[args.set](run, args)
    return 1 if run.failures else 0


if __name__ == "__main__":
    sys.exit(main())
