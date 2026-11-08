"""Helpers for scripted runtime-lab runs (Python stdlib only).

A scripted run is an ordinary Python program: it starts a controller, waits
for the run's manifest, and then acts on the run directly — writing cgroup
files, signalling processes — on whatever schedule or condition it likes.
The controller has no control API; the manifest is the whole contract. See
WORKFLOW.md, and `example_cap_cpu.py` for a worked example.

    import labrun
    with labrun.start("topology.yaml", duration="4m", label="cap") as run:
        run.sleep_until(60)
        run.set_cgroup("h2/reactor", "cpu.max", "50000 100000")
        ...
    # Leaving the block waits for the controller to exit.

Every action a script takes through this module is appended to the run's
`events.ndjson` (with `"source": "script"`), so reports align measures
against it.
"""
import datetime
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

def default_bin():
    """The `runtime-lab` executable: $RUNTIME_LAB_BIN, else the release build of
    $CARGO_TARGET_DIR (set under `mise`), else `runtime-lab` on PATH."""
    if os.environ.get("RUNTIME_LAB_BIN"):
        return os.environ["RUNTIME_LAB_BIN"]
    if os.environ.get("CARGO_TARGET_DIR"):
        return str(Path(os.environ["CARGO_TARGET_DIR"]) / "release" / "runtime-lab")
    found = shutil.which("runtime-lab")
    if found:
        return found
    raise RuntimeError("can't find runtime-lab: set RUNTIME_LAB_BIN to its path (see WORKFLOW.md, Setup)")


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Run:
    def __init__(self, process, run_dir):
        self.process = process
        self.run_dir = Path(run_dir)
        self.manifest = None
        self.started = time.monotonic()

    # -- Lifecycle ----------------------------------------------------------

    def wait_ready(self, timeout=300):
        """Block until the controller writes the run manifest (every host is
        serving), returning it. Fails if the controller exits first."""
        path = self.run_dir / "manifest.json"
        deadline = time.monotonic() + timeout
        while not path.exists():
            if self.process.poll() is not None:
                raise RuntimeError(f"controller exited ({self.process.returncode}) before the run was ready")
            if time.monotonic() > deadline:
                raise TimeoutError(f"no {path} within {timeout}s")
            time.sleep(0.2)
        self.manifest = json.loads(path.read_text())
        self.started = time.monotonic()
        return self.manifest

    def elapsed(self):
        """Seconds since the run became ready."""
        return time.monotonic() - self.started

    def sleep_until(self, seconds):
        """Sleep until `seconds` after the run became ready. Fails if the
        controller exits meanwhile (a run which failed stops the script)."""
        while self.elapsed() < seconds:
            if self.process.poll() is not None:
                raise RuntimeError(f"controller exited ({self.process.returncode}) at +{self.elapsed():.0f}s")
            time.sleep(min(0.2, max(0.0, seconds - self.elapsed())))

    def stop(self):
        """Request a clean stop (as SIGINT would)."""
        self.event("scriptStop")
        self.process.send_signal(signal.SIGINT)

    def wait(self):
        """Wait for the controller to exit, returning its exit code."""
        return self.process.wait()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None and self.process.poll() is None:
            self.stop()
        self.wait()
        return False

    # -- Addressing -----------------------------------------------------------

    def cgroup(self, node):
        """Path of a cgroup node: "h1", "h1/reactor", "h1/sidecar", "h1/connectors",
        or the run's "broker" or "etcd"."""
        if node in ("broker", "etcd"):
            return Path(self.manifest[node]["cgroup"])
        host, _, role = node.partition("/")
        h = self.manifest["hosts"][host]
        if not role:
            return Path(h["cgroup"])
        if role == "connectors":
            return Path(h["connectorsCgroup"])
        return Path(self._process(node)["cgroup"])

    def pid(self, process):
        """PID of "h1/reactor", "h1/sidecar", "broker", or "etcd"."""
        return self._process(process)["pid"]

    def _process(self, process):
        if process in ("broker", "etcd"):
            return self.manifest[process]
        host, _, role = process.partition("/")
        found = self.manifest["hosts"][host].get(role)
        if found is None:
            # A host on which no shard is placed runs no reactor.
            raise KeyError(f"run has no process {process}")
        return found

    def shards(self, task):
        """Shards of `task` from the manifest: label, host, id, range, socket."""
        return self.manifest["tasks"][task]["shards"]

    # -- Actions --------------------------------------------------------------

    def set_cgroup(self, node, file, value):
        """Write a cgroup interface file of `node`, e.g. ("h2/reactor", "cpu.max", "50000 100000")."""
        path = self.cgroup(node) / file
        path.write_text(value)
        self.event("scriptSetCgroup", node=node, file=file, value=value)

    def signal(self, process, sig=signal.SIGKILL):
        """Signal a host process, e.g. ("h1/sidecar", SIGKILL) to inject a fault."""
        os.kill(self.pid(process), sig)
        self.event("scriptSignal", process=process, signal=signal.Signals(sig).name)

    def event(self, name, **fields):
        """Append a script event to the run's event log."""
        line = {"ts": now(), "event": name, "source": "script", **fields}
        with open(self.run_dir / "events.ndjson", "a") as f:
            f.write(json.dumps(line) + "\n")
        print(f"[{self.elapsed():7.1f}s] {name} {fields}", file=sys.stderr)


def start(topology, run_id=None, label=None, bin=None, **flags):
    """Start a controller of `topology`, returning its Run once it's ready.

    `flags` become controller flags: duration="5m" => --duration=5m,
    data_root=... => --data-root=..., profile=... => --profile=...
    """
    topology = Path(topology).resolve()
    if run_id is None:
        run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
        if label:
            run_id = f"{run_id}-{label}"
    args = [bin or default_bin(), "controller", str(topology), f"--run-id={run_id}"]
    for key, value in flags.items():
        if value is not None:
            args.append(f"--{key.replace('_', '-')}={value}")

    process = subprocess.Popen(args)
    run = Run(process, topology.parent / "runs" / run_id)
    run.wait_ready()
    return run
