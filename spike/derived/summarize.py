#!/usr/bin/env python3
"""Summarize either of experiment 5's CSVs into the table the report carries.

Column-driven rather than told which file it got: the matrix CSV groups by
`cell` and carries a wall time, the diagnostic CSV groups by `stage` and
carries a CPU time, and only the matrix has the gate's two rows.

Percentiles are linearly interpolated between order statistics (the method
`statistics.quantiles(..., method="inclusive")` uses). At n=10 a nearest-rank
p95 would just be the maximum, which says more about one boot than about the
cell.
"""

import collections
import csv
import statistics
import sys

MEASURES = [
    ("import_pandas_ms", "import"),
    ("python_c_pass_ms", "pass"),
    ("cpu_ms", "cpu"),
    ("helper_wall_ms", "wall"),
]


def percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = q * (len(ordered) - 1)
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def collect(rows: list[dict], key: str, column: str) -> dict[str, list[float]]:
    """Values of `column` per group, skipping groups where it is never set."""
    out = collections.OrderedDict()
    for row in rows:
        raw = row.get(column, "")
        if raw != "":
            out.setdefault(row[key], []).append(float(raw))
    return out


def main() -> None:
    with open(sys.argv[1], newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise SystemExit("summarize.py: no runs in the CSV")

    key = "cell" if "cell" in rows[0] else "stage"
    present = [(c, name) for c, name in MEASURES if any(r.get(c) for r in rows)]
    groups = collections.OrderedDict((r[key], None) for r in rows)
    values = {c: collect(rows, key, c) for c, _ in present}

    header = f"{key:<18}"
    for _, name in present:
        header += f" {name + ' median':>14} {name + ' p95':>12}"
    print(header + f" {'n':>3}")

    for group in groups:
        line = f"{group:<18}"
        count = 0
        for column, _ in present:
            series = values[column].get(group)
            if not series:
                line += f" {'-':>14} {'-':>12}"
                continue
            count = max(count, len(series))
            line += f" {statistics.median(series):14.1f} {percentile(series, 0.95):12.1f}"
        print(line + f" {count:3d}")

    # PLAN experiment 5's gate, against whichever transport the file measured:
    # `primary` is the venv as an image layer on virtiofs (WP08), `blk-ext4`
    # the per-tag block image that replaced it (WP08b).
    imports = values.get("import_pandas_ms", {})
    if "baseline" in imports:
        baseline = statistics.median(imports["baseline"])
        for cell in ("primary", "blk-ext4"):
            if cell not in imports:
                continue
            median = statistics.median(imports[cell])
            ratio = median / baseline
            print()
            print(f"gate: {cell} {median:.1f} ms / baseline {baseline:.1f} ms = "
                  f"{ratio:.2f}x -> {'PASS' if ratio <= 2.0 else 'FAIL'} (limit 2.00x)")


main()
