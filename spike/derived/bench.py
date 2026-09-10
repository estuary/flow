#!/usr/bin/env python
"""Experiment 5's measurement: what a cold `import pandas` costs.

Runs unchanged as the guest workload (`--exec /usr/local/bin/python
/venv/spike/bench.py`) and as the baseline under a plain `podman run`, so the
two numbers differ only in how site-packages reached the interpreter. Python
3.14 stdlib only; the venv under test is put on sys.path rather than being the
running interpreter, so every cell uses the image's `/usr/local/bin/python`
and the transport is the only variable.

One JSON line on stdout. The guest's stdout carries the kernel console too
(CONTRACTS "Helper CLI"), so the caller picks the line out by its leading `{`.
"""

import argparse
import glob
import json
import subprocess
import sys
import time


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--venv", required=True, help="venv root holding pandas")
    parser.add_argument("--label", required=True, help="cell name, echoed back")
    return parser.parse_args()


def site_packages(venv: str) -> str:
    found = glob.glob(f"{venv}/lib/python*/site-packages")
    if len(found) != 1:
        raise SystemExit(f"bench.py: {venv} holds {len(found)} site-packages: {found}")
    return found[0]


def main() -> None:
    args = parse_args()
    sys.path.insert(0, site_packages(args.venv))

    # Timed before `import pandas`, so pandas' own reads cannot warm it. It is
    # still a warm-cache number: this interpreter already read the binary,
    # libpython and the startup stdlib to get here. Cold interpreter cost lives
    # in boot time (experiment 2), not here.
    start = time.perf_counter()
    child = subprocess.run([sys.executable, "-c", "pass"])
    pass_ms = (time.perf_counter() - start) * 1000.0
    if child.returncode != 0:
        raise SystemExit(f"bench.py: python -c pass exited {child.returncode}")

    start = time.perf_counter()
    import pandas
    import_ms = (time.perf_counter() - start) * 1000.0

    print(
        json.dumps(
            {
                "label": args.label,
                "import_pandas_ms": round(import_ms, 3),
                "python_c_pass_ms": round(pass_ms, 3),
                "pandas_version": pandas.__version__,
                "pandas_file": pandas.__file__,
            }
        ),
        flush=True,
    )


main()
