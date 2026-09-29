#!/usr/bin/env python3
"""Example scripted run: cap one host's reactor mid-run, then release it.

Usage (from an experiment directory, outside of any repository):
  example_cap_cpu.py topology.yaml [--node HOST/reactor] [--profile NAME] [--data-root DIR] [--duration 3m]

At +1m, `--node` (default: the reactor of the topology's first host) is capped
to half a core (cpu.max "50000 100000"); at +2m the cap is removed; at +3m the
run stops. Its effect appears in the report's Phases (split at the two script
events) as throttling of the node and a drop of committed throughput, and, with
more than one shard, a rise of source skew.

It's a template: copy it into an experiment and change the schedule, the
condition, or the action.
"""
import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import labrun  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument("topology")
parser.add_argument("--node", default=None)
parser.add_argument("--profile", default=None)
parser.add_argument("--data-root", default=None)
parser.add_argument("--duration", default="3m", help="run duration (the schedule assumes at least 2m)")
args = parser.parse_args()

with labrun.start(
    args.topology,
    label="cap-cpu",
    duration=args.duration,
    profile=args.profile,
    data_root=args.data_root,
) as run:
    node = args.node or f"{sorted(run.manifest['hosts'])[0]}/reactor"
    run.sleep_until(60)
    run.set_cgroup(node, "cpu.max", "50000 100000")
    run.sleep_until(120)
    run.set_cgroup(node, "cpu.max", "max 100000")

print(f"run directory: {run.run_dir}", file=sys.stderr)
report = Path(__file__).resolve().parent / "report.py"
subprocess.run([sys.executable, str(report), str(run.run_dir), "--skip", "20s"], check=False)
sys.exit(run.process.returncode)
