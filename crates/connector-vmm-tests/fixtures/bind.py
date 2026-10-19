"""Bind one socket in this network namespace and hand it to the suite.

Run as root inside another network namespace (`ip netns exec` or `nsenter
-n`). A socket stays in the namespace it was created in, so the unprivileged
test that receives the descriptor over PATH serves from there.

    bind.py PATH tcp|udp ADDRESS PORT
"""

import socket
import sys

path, kind, address, port = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM if kind == "tcp" else socket.SOCK_DGRAM)
sock.bind((address, port))
if kind == "tcp":
    sock.listen(64)

channel = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
channel.connect(path)
socket.send_fds(channel, [b"fd"], [sock.fileno()])
