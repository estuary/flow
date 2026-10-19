"""Observe the guest's pre-init checkpoint and create HELD.

Usage: gate.py HELD CHECKPOINT. Only the exact checkpoint line is withheld;
all other stderr, including a final partial line, passes unchanged.
"""

import sys

held, checkpoint = sys.argv[1], sys.argv[2].encode() + b"\n"
source, sink = sys.stdin.buffer, sys.stdout.buffer
for line in source:
    if line == checkpoint:
        open(held, "w").close()
    else:
        sink.write(line)
        sink.flush()
