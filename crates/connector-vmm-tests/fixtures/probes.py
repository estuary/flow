"""Guest-side probes for the connector VMM's KVM suite.

Python stdlib only, because it runs as the test guest image's own interpreter.

`serve` listens on AF_VSOCK port 49092, which the VMM maps to the host's
`/sock/init.sock`, writes the readiness marker (a line holding one space) once
it is listening, and then answers one request per line from the one host
connection it accepts:

    request   {"probe": "unlisted-refused", "op": "query", ...arguments}
    response  {"probe": "unlisted-refused", "result": ..., "ms": 12}

The host decides when each probe runs, so it can read the kernel's state
between two probes rather than racing a script's sleeps. Every response is
also written to stderr, where it survives in the suite's diagnostics even if
the connection does not.

`once REQUEST` answers a single request on stderr, for the probes that need
guest root and so run under `--as-root-exec` before the workload drops it.

A dropped packet and a refused connection are different results: `timeout`
means nothing answered, `refused` means a host answered with a reset.
"""

import errno
import json
import os
import random
import socket
import struct
import sys
import time

VSOCK_PORT = 49092
VMADDR_CID_HOST = 2
TSI_PROXY_CREATE = 1024

# virtio device ids, from the virtio specification.
VIRTIO_NAMES = {1: "net", 2: "blk", 3: "console", 4: "rng", 5: "balloon",
                0x13: "vsock", 0x1A: "fs"}

held = {}
listeners = {}
kept = []


def oserror(error):
    if error.errno in (errno.ECONNREFUSED,):
        return "refused"
    if error.errno in (errno.EHOSTUNREACH, errno.ENETUNREACH):
        return "unreachable"
    return "error:%s" % errno.errorcode.get(error.errno, error.errno)


def connect(family, kind, address, timeout):
    sock = socket.socket(family, kind)
    sock.settimeout(timeout)
    try:
        sock.connect(address)
        return sock, "connected"
    except socket.timeout:
        sock.close()
        return None, "timeout"
    except OSError as error:
        sock.close()
        return None, oserror(error)


def op_tcp(r):
    sock, result = connect(socket.AF_INET, socket.SOCK_STREAM, (r["addr"], r["port"]), r["timeout"])
    if sock:
        sock.close()
    return result


def op_hold(r):
    sock, result = connect(socket.AF_INET, socket.SOCK_STREAM, (r["addr"], r["port"]), r["timeout"])
    if sock:
        held[r["id"]] = sock
    return result


def op_exchange(r):
    """Traffic on a held connection. A reply is the proof it still passes:
    a dropped packet produces no reset to observe, only silence."""
    sock = held[r["id"]]
    sock.settimeout(r["timeout"])
    try:
        sock.sendall(b"ping\n")
        reply = sock.recv(16)
        return reply.decode().strip() if reply else "closed"
    except socket.timeout:
        return "timeout"
    except OSError as error:
        return oserror(error)


def op_release(r):
    held.pop(r["id"]).close()
    return "closed"


def op_resolve(r):
    try:
        answers = socket.getaddrinfo(r["name"], None, family=socket.AF_INET,
                                     type=socket.SOCK_STREAM)
    except socket.gaierror as error:
        return "error:gai%d" % error.args[0]
    return "ok:" + ",".join(sorted({answer[4][0] for answer in answers}))


def op_query(r):
    """A raw query, which is the only way to see the TTL the resolver wrote."""
    query_id = random.randrange(0, 65536)
    question = b"".join(bytes([len(label)]) + label.encode()
                        for label in r["name"].split(".")) + b"\x00"
    message = struct.pack("!HHHHHH", query_id, 0x0100, 1, 0, 0, 0) + question
    message += struct.pack("!HH", r.get("qtype", 1), 1)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(r["timeout"])
    try:
        sock.sendto(message, (r["nameserver"], 53))
        response, _ = sock.recvfrom(4096)
    except socket.timeout:
        return {"status": "timeout"}
    except OSError as error:
        return {"status": oserror(error)}
    finally:
        sock.close()

    answer_id, flags, questions, answers = struct.unpack("!HHHH", response[:8])
    if answer_id != query_id:
        return {"status": "error:id-mismatch"}
    if flags & 0xF:
        return {"status": "rcode:%d" % (flags & 0xF)}

    cursor = 12
    for _ in range(questions):
        cursor = skip_name(response, cursor) + 4
    records = []
    for _ in range(answers):
        cursor = skip_name(response, cursor)
        rtype, _class, ttl, length = struct.unpack("!HHIH", response[cursor:cursor + 10])
        cursor += 10
        if rtype == 1 and length == 4:
            records.append(["A", socket.inet_ntoa(response[cursor:cursor + 4]), ttl])
        elif rtype == 5:
            records.append(["CNAME", read_name(response, cursor), ttl])
        cursor += length
    return {"status": "ok", "records": records}


def skip_name(message, cursor):
    while True:
        length = message[cursor]
        if length & 0xC0 == 0xC0:
            return cursor + 2
        cursor += 1
        if length == 0:
            return cursor
        cursor += length


def read_name(message, cursor):
    labels = []
    while True:
        length = message[cursor]
        if length & 0xC0 == 0xC0:
            cursor = ((length & 0x3F) << 8) | message[cursor + 1]
            continue
        if length == 0:
            return ".".join(labels)
        labels.append(message[cursor + 1:cursor + 1 + length].decode())
        cursor += 1 + length


def op_ipv6(r):
    try:
        sock, result = connect(socket.AF_INET6, socket.SOCK_STREAM, (r["addr"], r["port"]), r["timeout"])
    except OSError as error:
        return "socket-" + oserror(error)
    if sock:
        sock.close()
    return result


def op_icmp(r):
    """An echo request to a resolved, authorized address. Needs guest root."""
    address = socket.gethostbyname(r["name"])
    packet = struct.pack("!BBHHH", 8, 0, 0, os.getpid() & 0xFFFF, 1) + b"connector-vmm-kvm"
    packet = packet[:2] + struct.pack("!H", checksum(packet)) + packet[4:]
    sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)
    sock.settimeout(r["timeout"])
    try:
        sock.sendto(packet, (address, 0))
        sock.recvfrom(1024)
        return "reply"
    except socket.timeout:
        return "timeout"
    except OSError as error:
        return oserror(error)
    finally:
        sock.close()


def checksum(data):
    if len(data) % 2:
        data += b"\x00"
    total = sum(struct.unpack("!%dH" % (len(data) // 2), data))
    total = (total & 0xFFFF) + (total >> 16)
    return ~total & 0xFFFF


def op_listen(r):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("0.0.0.0", r["port"]))
    sock.listen(1)
    listeners[r["port"]] = sock
    return "listening"


def op_accepted(r):
    sock = listeners.pop(r["port"])
    sock.settimeout(r["wait"])
    try:
        peer, address = sock.accept()
        peer.close()
        return "connection-from:%s" % address[0]
    except socket.timeout:
        return "no-connection"
    finally:
        sock.close()


def op_vsock(r):
    """A blocking AF_VSOCK connect has no default timeout, and an unmapped
    port draws no response at all, so the probe imposes its own."""
    sock, result = connect(socket.AF_VSOCK, socket.SOCK_STREAM, (r["cid"], r["port"]), r["timeout"])
    if sock:
        sock.close()
    return result


def op_tsi(r):
    """libkrun's TSI proxy-create request: peer port u32, family u16
    (AF_INET) and type u16 (SOCK_STREAM), little endian, to control port 1024."""
    try:
        sock = socket.socket(socket.AF_VSOCK, socket.SOCK_DGRAM, 0)
    except OSError as error:
        return "socket-" + oserror(error)
    try:
        sock.sendto(struct.pack("<IHH", 12345, 2, 1), (VMADDR_CID_HOST, TSI_PROXY_CREATE))
        return "sent"
    except OSError as error:
        return oserror(error)
    finally:
        sock.close()


def op_devices(r):
    counts = {}
    base = "/sys/bus/virtio/devices"
    for name in sorted(os.listdir(base)):
        with open(os.path.join(base, name, "device")) as f:
            ident = int(f.read().strip(), 16)
        key = VIRTIO_NAMES.get(ident, "id-0x%x" % ident)
        counts[key] = counts.get(key, 0) + 1
    return " ".join("%s:%d" % item for item in sorted(counts.items()))


def op_same(r):
    """Path handling only: the guest's own VFS resolves `..` and never sends
    it to the host, so this says nothing about a compromised guest kernel."""
    try:
        a, b = os.stat(r["a"]), os.stat(r["b"])
    except OSError as error:
        return oserror(error)
    return "same-file" if (a.st_dev, a.st_ino) == (b.st_dev, b.st_ino) else "different-file"


def op_fill(r):
    """Write zeros until `mib` or an error, then sync, so the host sees what
    the guest wrote rather than what its page cache still holds. With `keep`,
    the file stays open for the life of the probe server."""
    chunk = b"\x00" * (1 << 20)
    written = 0
    stop = "done"
    fd = os.open(r["path"], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        for _ in range(r["mib"]):
            written += os.write(fd, chunk)
    except OSError as error:
        stop = oserror(error)
    os.sync()
    if r.get("keep"):
        kept.append(fd)
    else:
        os.close(fd)
    return {"written": written, "stop": stop}


def op_write(r):
    try:
        with open(r["path"], "w") as f:
            f.write(r["data"])
        return "ok"
    except OSError as error:
        return oserror(error)


def op_read(r):
    try:
        with open(r["path"]) as f:
            return {"content": f.read()}
    except OSError as error:
        return oserror(error)


def op_list(r):
    try:
        return sorted(os.listdir(r["path"]))
    except OSError as error:
        return oserror(error)


def op_exec(r):
    try:
        pid = os.fork()
    except OSError as error:
        return oserror(error)
    if pid == 0:
        try:
            devnull = os.open(os.devnull, os.O_WRONLY)
            os.dup2(devnull, 1)
            os.dup2(devnull, 2)
            os.execv(r["argv"][0], r["argv"])
        except OSError as error:
            os._exit(126 if error.errno in (errno.EACCES, errno.EPERM) else 127)
    _, status = os.waitpid(pid, 0)
    return "exit:%d" % os.waitstatus_to_exitcode(status)


def op_env(r):
    return dict(os.environ)


def op_mounts(r):
    with open("/proc/mounts") as f:
        return f.read().splitlines()


def op_await(r):
    """Poll a file until it holds `content`: the connector's view of a file the
    host replaced."""
    started = time.monotonic()
    while time.monotonic() - started < r["timeout"]:
        try:
            with open(r["path"]) as f:
                if f.read() == r["content"]:
                    return {"seen": True}
        except OSError:
            pass
        time.sleep(0.05)
    return {"seen": False}


OPS = {name[3:]: fn for name, fn in globals().items() if name.startswith("op_")}


def answer(request):
    started = time.monotonic()
    result = OPS[request["op"]](request)
    response = {"probe": request["probe"], "result": result,
                "ms": int((time.monotonic() - started) * 1000)}
    print(json.dumps(response), file=sys.stderr, flush=True)
    return response


def serve():
    listener = socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM)
    listener.bind((socket.VMADDR_CID_ANY, VSOCK_PORT))
    listener.listen(1)
    # The readiness marker, which a workload replacing connector-init owns.
    sys.stderr.write(" \n")
    sys.stderr.flush()

    conn, _ = listener.accept()
    reader = conn.makefile("rb")
    for line in reader:
        conn.sendall(json.dumps(answer(json.loads(line))).encode() + b"\n")


def main():
    if sys.argv[1] == "serve":
        serve()
    elif sys.argv[1] == "once":
        answer(json.loads(sys.argv[2]))
    else:
        sys.exit("probes.py: expected `serve` or `once REQUEST`")


if __name__ == "__main__":
    main()
