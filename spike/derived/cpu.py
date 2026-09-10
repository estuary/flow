#!/usr/bin/env python
"""A CPU control for experiment 5: the same work with no IO at all.

`import pandas` being slower in the guest is only a virtiofs result if the
guest's CPU is not slower too. This runs an allocation-light, IO-free loop so
the guest-vs-container ratio here can be subtracted from the import ratio.
Python 3.14 stdlib only, so it runs in both places unchanged.
"""

import argparse
import json
import time

parser = argparse.ArgumentParser()
parser.add_argument("--label", required=True)
parser.add_argument("--iterations", type=int, default=5_000_000)
args = parser.parse_args()

start = time.perf_counter()
total = 0
for i in range(args.iterations):
    total += i * i
cpu_ms = (time.perf_counter() - start) * 1000.0

print(json.dumps({"label": args.label, "cpu_ms": round(cpu_ms, 3), "checksum": total}),
      flush=True)
