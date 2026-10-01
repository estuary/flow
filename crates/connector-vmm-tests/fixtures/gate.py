"""Withhold connector-init's readiness byte and mark when it arrives."""

import sys

held = sys.argv[1]
source, sink = sys.stdin.buffer, sys.stdout.buffer
line_start, withheld = True, False

while byte := source.read(1):
    if byte == b" " and line_start and not withheld:
        withheld = True
        open(held, "w").close()
        continue
    sink.write(byte)
    sink.flush()
    line_start = byte == b"\n"
