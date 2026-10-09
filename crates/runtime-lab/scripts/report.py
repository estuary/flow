#!/usr/bin/env python3
"""Summarize a runtime-lab run directory into its headline measures.

Usage: report.py <run-dir> [--skip DURATION] [--until DURATION] [--json]

The window of the report opens `--skip` (default 30s) after every task's first
session opened, and closes at the run's stop (or its last sample), or `--until`
after the window opens. Terms are defined in crates/runtime-lab/GLOSSARY.md.

Measures:
  1. Committed throughput: source documents and bytes per second, counted at
     the close of their transaction, once it's known to have committed (from
     the Leader's stats documents). A capture's source documents are those
     its connector captured, and its `out` those published after combining.
  2. Transaction cadence: transaction durations and commits per minute.
  3. Source skew and source lag, by cohort, in source-producer wall-clock time.
  4. Shuffle pressure: log disk backlog and stalled reads, by shard.
  5. Append backpressure: bytes appended per second to each journal, the
     share of them which the broker's flow control delayed, and each
     collection's partitions (splits) over the window.
  6. Resource use of each cgroup node: CPU, CPU throttling, memory, IO, pressure.
Plus per-binding left / right / out, each binding's committed source clock
and bytes behind at the first and last commit of the run (where did it start?),
CPU time by thread, phases between script events (interventions), and a
timeline of committed throughput, CPU, throttling, and skew in `--bucket`
intervals. Offsets are from the run's start, as in the event log.

This is a baseline. Copy it into an experiment and change it freely.
"""
import argparse
import collections
import datetime
import json
import re
import statistics
import sys
from pathlib import Path


def parse_ts(value):
    """Parse an RFC 3339 timestamp (of any fractional precision) to epoch seconds."""
    m = re.match(r"^(.*?T\d\d:\d\d:\d\d)(\.\d+)?(Z|[+-]\d\d:\d\d)$", value)
    base, frac, tz = m.groups()
    tz = "+00:00" if tz == "Z" else tz
    dt = datetime.datetime.fromisoformat(base + tz)
    return dt.timestamp() + (float(frac) if frac else 0.0)


def parse_duration(value):
    m = re.match(r"^(\d+(?:\.\d+)?)(ms|s|m|h)$", value)
    if not m:
        raise argparse.ArgumentTypeError(f"invalid duration {value!r} (e.g. 30s, 5m)")
    n, unit = float(m.group(1)), m.group(2)
    return n * {"ms": 0.001, "s": 1, "m": 60, "h": 3600}[unit]


def read_ndjson(path):
    path = Path(path)
    if not path.exists():
        return []
    out = []
    for line in path.open():
        line = line.strip()
        if line:
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass  # A torn final line of a live run.
    return out


SERIES = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?$')
LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def split_series(series):
    m = SERIES.match(series)
    return m.group(1), dict(LABEL.findall(m.group(2) or ""))


def task_of_shard_id(shard_id):
    """`{type}/{task name...}/{generation}/{key}-{rclock}` => task name."""
    return "/".join(shard_id.split("/")[1:-2])


def pct(values, q):
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, int(round(q * (len(values) - 1))))]


def summary(values):
    if not values:
        return None
    return {
        "mean": statistics.fmean(values),
        "p50": pct(values, 0.5),
        "p90": pct(values, 0.9),
        "max": max(values),
    }


def rate(points, t0, t1):
    """Per-second rate of a counter's [(t, v)] points over [t0, t1]."""
    inside = [(t, v) for t, v in points if t0 <= t <= t1]
    if len(inside) < 2 or inside[-1][0] == inside[0][0]:
        return None
    (ta, va), (tb, vb) = inside[0], inside[-1]
    return (vb - va) / (tb - ta)


def read_stats(run):
    """Stats documents of a run, tailed from its ops stats journals. Those
    also hold ACK documents, which have no `shard`. A run predating the lab's
    broker kept them per host."""
    files = [run / "stats.ndjson"] + sorted(run.glob("hosts/*/stats.ndjson"))
    return [d for f in files for d in read_ndjson(f) if "shard" in d]


def window_of(events, samples, skip, until):
    opened = collections.defaultdict(list)
    stop = None
    for e in events:
        if e["event"] == "sessionOpened":
            opened[e["task"]].append(parse_ts(e["ts"]))
        elif e["event"] in ("stopping", "runFailed") and stop is None:
            stop = parse_ts(e["ts"])
    first_samples = [parse_ts(s["ts"]) for s in samples[:1]]
    last_samples = [parse_ts(s["ts"]) for s in samples[-1:]]

    t_open = max(min(ts) for ts in opened.values()) if opened else (first_samples or [0])[0]
    t0 = t_open + skip
    t1 = min(x for x in [stop, (last_samples or [None])[0]] if x is not None) if (stop or last_samples) else t0
    if until is not None:
        t1 = min(t1, t0 + until)
    return t0, t1


def binding_sides(doc):
    """A stats document's left / right / out, by binding (or transform). A
    capture's right is captured from its connector, and its out published."""
    if "capture" in doc:
        return {
            binding: {k: s.get(k, {}) for k in ("right", "out")}
            for binding, s in doc["capture"].items()
        }
    if "materialize" in doc:
        return {
            binding: {k: s.get(k, {}) for k in ("left", "right", "out")}
            for binding, s in doc["materialize"].items()
        }
    if "derive" in doc:
        derive = doc["derive"]
        parts = {
            name: {"right": s.get("input", {})}
            for name, s in derive.get("transforms", {}).items()
        }
        if derive.get("out"):
            parts.setdefault("(derived)", {})["out"] = derive["out"]
        return parts
    return {}


def source_progress(doc):
    """(binding, lastSourcePublishedAt, bytesBehind) of a stats document."""
    if "materialize" in doc:
        items = doc["materialize"].items()
    elif "derive" in doc:
        items = doc["derive"].get("transforms", {}).items()
    else:
        items = []
    for binding, s in items:
        if s.get("lastSourcePublishedAt"):
            yield binding, s["lastSourcePublishedAt"], int(s.get("bytesBehind", 0))


def close_of(doc):
    """A stats document is stamped at its transaction's open: its close is the
    end of reading. Its commit follows the close by the commit's duration, which
    stats don't record."""
    return parse_ts(doc["ts"]) + doc.get("openSecondsTotal", 0.0)


def split_in_flight(stats, events):
    """-> (committed stats, {task: in-flight stats document}).

    A Leader writes a transaction's stats document before committing it, and
    the lab tails it at once (production's readers read it only once
    acknowledged). A Leader commits a transaction before it closes the next,
    so only a task's last document may not have committed. It has if a
    `sessionStopped` follows its close: a session stops only after committing."""
    by_task = collections.defaultdict(list)
    for doc in stats:
        by_task[doc["shard"]["name"]].append(doc)
    stopped = collections.defaultdict(list)
    for e in events:
        if e["event"] == "sessionStopped":
            stopped[e["task"]].append(parse_ts(e["ts"]))

    committed, in_flight = [], {}
    for task, docs in by_task.items():
        docs.sort(key=close_of)
        last = docs[-1]
        if not any(t >= close_of(last) for t in stopped[task]):
            in_flight[task] = docs.pop()
        committed.extend(docs)
    return committed, in_flight


def committed_throughput(stats, t0, t1):
    """Stats documents are per-transaction. A committed transaction is counted
    in the window when it closed within it."""
    tasks = {}
    for doc in stats:
        name = doc["shard"]["name"]
        duration = doc.get("openSecondsTotal", 0.0)
        close = close_of(doc)

        task = tasks.setdefault(name, {
            "kind": doc["shard"]["kind"], "txns": 0, "emptyTxns": 0, "durations": [], "commits": [],
            "readDocs": 0, "readBytes": 0, "bindings": {}, "progress": {},
        })
        # Where each binding's committed reads began and ended, over the whole
        # run: the evidence of where a task started (fresh, snapshot, resumed).
        for binding, clock, behind in source_progress(doc):
            p = task["progress"].setdefault(binding, {"first": None, "last": None})
            point = {"sourceClock": clock, "bytesBehind": behind, "closedAt": close}
            p["first"] = p["first"] or point
            p["last"] = point

        if not (t0 <= close <= t1):
            continue
        parts = binding_sides(doc)
        # A transaction which read nothing (as many are, as a fresh session
        # starts) says nothing of cadence under load: it's counted apart.
        if not any(side.get("right", {}).get("docsTotal") for side in parts.values()):
            task["emptyTxns"] += doc.get("txnCount", 1)
            continue
        task["txns"] += doc.get("txnCount", 1)
        task["durations"].append(duration)
        task["commits"].append(close)

        for binding, sides in parts.items():
            acc = task["bindings"].setdefault(binding, {})
            for side, db in sides.items():
                a = acc.setdefault(side, {"docs": 0, "bytes": 0})
                a["docs"] += int(db.get("docsTotal", 0))
                a["bytes"] += int(db.get("bytesTotal", 0))
            if binding != "(derived)":
                task["readDocs"] += int(sides.get("right", {}).get("docsTotal", 0))
                task["readBytes"] += int(sides.get("right", {}).get("bytesTotal", 0))

    span = max(t1 - t0, 1e-9)
    out = {}
    for name, task in tasks.items():
        out[name] = {
            "kind": task["kind"],
            "committedDocsPerSec": task["readDocs"] / span,
            "committedBytesPerSec": task["readBytes"] / span,
            "transactions": task["txns"],
            "emptyTransactions": task["emptyTxns"],
            "sourceProgress": task["progress"],
            "commitsPerMinute": task["txns"] / span * 60,
            "txnSeconds": summary(task["durations"]),
            "bindings": {
                b: {side: {"docs": v["docs"], "bytes": v["bytes"],
                           "docsPerSec": v["docs"] / span, "bytesPerSec": v["bytes"] / span}
                    for side, v in sides.items()}
                for b, sides in task["bindings"].items()
            },
        }
    return out


def metric_series(metrics):
    """-> {(process, name, frozenset(labels)): [(t, v)]}"""
    out = collections.defaultdict(list)
    for s in metrics:
        t = parse_ts(s["ts"])
        for series, v in s["metrics"].items():
            name, labels = split_series(series)
            out[(s["process"], name, frozenset(labels.items()))].append((t, v))
    return out


def source_clocks(series):
    """-> {"{task} cohort={cohort}": {t: {shard: last-read source clock}}}.

    A Slice reports a clock for each cohort, and clocks of distinct cohorts
    aren't comparable (a read delay holds its cohort back, by design), so
    shards are compared only within a cohort. Only a shard whose Slice reads
    journals reports a clock, so a task with fewer source journals than shards
    has fewer reporting shards than shards."""
    # service-kit prunes a metric not updated for 10 minutes, so during a long
    # stall these series end early (their absence is itself a signal).
    out = collections.defaultdict(lambda: collections.defaultdict(dict))
    for (_proc, name, labels), points in series.items():
        if name != "shuffle_slice_last_source_published_at_time_seconds":
            continue
        labels = dict(labels)
        key = f"{task_of_shard_id(labels['shard_id'])} cohort={labels.get('cohort', '')}"
        for t, v in points:
            if v > 0:
                out[key][round(t, 1)][labels["shard_id"]] = v
    return out


def source_skew(clocks, t0, t1):
    """By task and cohort: skew is the max - min over shards of each shard's
    last-read source clock; lag is sample time - the minimum clock."""
    out = {}
    for key, samples in clocks.items():
        skews, lags, n_shards = [], [], 0
        for t, shards in samples.items():
            if not t0 <= t <= t1:
                continue
            n_shards = max(n_shards, len(shards))
            values = list(shards.values())
            skews.append(max(values) - min(values))
            lags.append(t - min(values))
        if not n_shards:
            continue
        out[key] = {
            "shards": n_shards,
            "skewSeconds": summary(skews) if n_shards > 1 else None,
            "lagSeconds": summary(lags),
        }
    return out


def shuffle_pressure(series, t0, t1):
    out = collections.defaultdict(dict)
    for (_proc, name, labels), points in series.items():
        labels = dict(labels)
        if "shard_id" not in labels:
            continue
        shard = labels["shard_id"]
        inside = [v for t, v in points if t0 <= t <= t1]
        if name == "shuffle_log_disk_backlog_bytes":
            out[shard]["diskBacklogBytes"] = summary(inside)
        elif name == "shuffle_slice_stalled_reads":
            out[shard]["stalledReadsMax"] = max(inside) if inside else None
        elif name == "shuffle_slice_bytes_read_bytes_total":
            out[shard]["sliceReadBytesPerSec"] = rate(points, t0, t1)
        elif name == "shuffle_log_bytes_appended_bytes_total":
            out[shard]["logAppendedBytesPerSec"] = rate(points, t0, t1)
    return dict(out)


APPEND_SERIES = re.compile(r"^gazette_append(_delayed)?(?:_bytes)?(?:_total)?$")


def append_backpressure(series, journal_samples, t0, t1):
    """By journal: bytes appended per second, and the share of them which the
    broker's flow control delayed, summed over every appending process. By
    collection: its partitions over the window, which grow as journals split."""
    journals = collections.defaultdict(lambda: {"appendedBytesPerSec": 0.0, "delayedBytesPerSec": 0.0})
    for (_proc, name, labels), points in series.items():
        m = APPEND_SERIES.match(name)
        labels = dict(labels)
        if not m or "journal" not in labels:
            continue
        key = "delayedBytesPerSec" if m.group(1) else "appendedBytesPerSec"
        journals[labels["journal"]][key] += rate(points, t0, t1) or 0.0
    for j in journals.values():
        j["delayedShare"] = (j["delayedBytesPerSec"] / j["appendedBytesPerSec"]
                             if j["appendedBytesPerSec"] else None)

    partitions = {}
    for sample in journal_samples:
        if not t0 <= parse_ts(sample["ts"]) <= t1:
            continue
        counts = collections.Counter(j["collection"] for j in sample["journals"])
        for collection, n in counts.items():
            p = partitions.setdefault(collection, {"first": n, "last": n, "max": n})
            p["last"], p["max"] = n, max(p["max"], n)
    return {"journals": dict(journals), "partitions": partitions}


def resources(cgroups, t0, t1):
    nodes = collections.defaultdict(list)
    for s in cgroups:
        t = parse_ts(s["ts"])
        if t0 <= t <= t1:
            nodes[s["node"]].append((t, s["sample"]))

    out = {}
    for node, samples in nodes.items():
        if len(samples) < 2:
            continue
        (ta, a), (tb, b) = samples[0], samples[-1]
        dt = tb - ta
        cpu_a, cpu_b = a.get("cpu", {}), b.get("cpu", {})
        d = lambda k: cpu_b.get(k, 0) - cpu_a.get(k, 0)
        periods = d("nr_periods")

        io = {}
        for dev, stats in b.get("io", {}).items():
            prev = a.get("io", {}).get(dev, {})
            io[dev] = {
                "readBytesPerSec": (stats.get("rbytes", 0) - prev.get("rbytes", 0)) / dt,
                "writeBytesPerSec": (stats.get("wbytes", 0) - prev.get("wbytes", 0)) / dt,
            }

        def pressure(key):
            pa, pb = a.get(key, {}).get("some", {}), b.get(key, {}).get("some", {})
            # `total` is cumulative microseconds stalled: as a fraction of time.
            return (pb.get("total", 0) - pa.get("total", 0)) / 1e6 / dt if pb else None

        out[node] = {
            "cpuCores": d("usage_usec") / 1e6 / dt,
            "throttledPeriodsFraction": d("nr_throttled") / periods if periods else None,
            "throttledSecondsPerSec": d("throttled_usec") / 1e6 / dt,
            "cpuPressureSome": pressure("cpuPressure"),
            "ioPressureSome": pressure("ioPressure"),
            "memoryPressureSome": pressure("memoryPressure"),
            "memoryBytes": summary([s.get("memory.current", 0) for _, s in samples]),
            "io": {dev: v for dev, v in io.items() if v["readBytesPerSec"] or v["writeBytesPerSec"]},
        }
    return out


def threads(thread_samples, t0, t1):
    by_proc = collections.defaultdict(list)
    for s in thread_samples:
        t = parse_ts(s["ts"])
        if t0 <= t <= t1:
            by_proc[s["process"]].append((t, s))
    out = {}
    for proc, samples in by_proc.items():
        if len(samples) < 2:
            continue
        (ta, a), (tb, b) = samples[0], samples[-1]
        tck = b.get("clkTck", 100)
        out[proc] = {
            name: (sum(ticks) - sum(a["threads"].get(name, [0, 0]))) / tck / (tb - ta)
            for name, ticks in b["threads"].items()
        }
    return out


def timeline(stats, cgroups, clocks, t0, t1, bucket, t_start):
    """Per-bucket committed throughput by task, CPU and throttling by cgroup
    node, and source skew by task and cohort, to see an intervention take
    effect."""
    edges = []
    t = t0
    while t < t1:
        edges.append((t, min(t + bucket, t1)))
        t += bucket

    commits = collections.defaultdict(list)  # task -> [(close_ts, bytes, max source clock)]
    for doc in stats:
        n = sum(int(side.get("right", {}).get("bytesTotal", 0)) for side in binding_sides(doc).values())
        sources = [parse_ts(c) for _, c, _ in source_progress(doc)]
        commits[doc["shard"]["name"]].append((close_of(doc), n, max(sources) if sources else None))

    nodes = collections.defaultdict(list)
    for s in cgroups:
        nodes[s["node"]].append((parse_ts(s["ts"]), s["sample"].get("cpu", {})))

    rows = []
    for a, b in edges:
        row = {"from": a - t_start, "to": b - t_start, "committedBytesPerSec": {}, "transactions": {},
               "committedSourceClock": {}, "cpuCores": {}, "throttledSecondsPerSec": {}, "skewMaxSeconds": {}}
        for task, points in commits.items():
            inside = [(c, n, k) for c, n, k in points if a <= c < b]
            row["committedBytesPerSec"][task] = sum(n for _, n, _ in inside) / (b - a)
            row["transactions"][task] = sum(1 for _, n, _ in inside if n)
            # The committed source clock as of the bucket's end: a frozen clock
            # while behind is a stall, whatever the bytes say.
            committed = [k for c, _, k in points if c < b and k is not None]
            row["committedSourceClock"][task] = max(committed) if committed else None
        for node, points in nodes.items():
            inside = [(t, cpu) for t, cpu in points if a <= t <= b]
            if len(inside) >= 2 and inside[-1][0] > inside[0][0]:
                (ta, ca), (tb, cb) = inside[0], inside[-1]
                row["cpuCores"][node] = (cb.get("usage_usec", 0) - ca.get("usage_usec", 0)) / 1e6 / (tb - ta)
                row["throttledSecondsPerSec"][node] = (cb.get("throttled_usec", 0) - ca.get("throttled_usec", 0)) / 1e6 / (tb - ta)
        for key, samples in clocks.items():
            skews = [max(v.values()) - min(v.values()) for t, v in samples.items() if a <= t <= b and len(v) > 1]
            if skews:
                row["skewMaxSeconds"][key] = max(skews)
        rows.append(row)
    return rows


def phases(events, stats, cgroups, t0, t1):
    """Split the window at each script event (an intervention), and measure
    committed throughput and each node's CPU and throttling within each phase.
    Committed throughput counts transactions which commit within a phase, so a
    phase should span several transactions to be meaningful."""
    labels = sorted((e for e in events if e.get("source") == "script" and t0 < parse_ts(e["ts"]) < t1),
                    key=lambda e: parse_ts(e["ts"]))
    if not labels:
        return []
    edges = [t0] + [parse_ts(e["ts"]) for e in labels] + [t1]
    out = []
    for i, (a, b) in enumerate(zip(edges, edges[1:])):
        tp = committed_throughput(stats, a, b)
        res = resources(cgroups, a, b)
        out.append({
            "from": a, "to": b,
            "after": None if i == 0 else {k: v for k, v in labels[i - 1].items() if k not in ("ts", "source")},
            "committedBytesPerSec": {t: v["committedBytesPerSec"] for t, v in tp.items()},
            "transactions": {t: v["transactions"] for t, v in tp.items()},
            "cpuCores": {n: r["cpuCores"] for n, r in res.items()},
            "throttledSecondsPerSec": {n: r["throttledSecondsPerSec"] for n, r in res.items()},
        })
    return out


def starts(run):
    """Each task's start (`fresh`, `snapshot:<name>`, `resumed:<run-id>`), and
    for a resumed task, the last commit of the run it resumed: a resumed run
    continues from there, which its first commit should show."""
    try:
        manifest = json.loads((run / "manifest.json").read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    out = {}
    prior = Path(manifest["resumed"]) if manifest.get("resumed") else None
    prior_stats = []
    if prior:
        prior_stats, _ = split_in_flight(read_stats(prior), read_ndjson(prior / "events.ndjson"))
        prior_stats.sort(key=close_of)
    for task, t in manifest.get("tasks", {}).items():
        start = t.get("start", "")
        out[task] = {"start": start}
        if start.startswith("resumed:") and prior:
            last = {}
            for doc in (d for d in prior_stats if d["shard"]["name"] == task):
                for binding, clock, behind in source_progress(doc):
                    last[binding] = {"sourceClock": clock, "bytesBehind": behind}
            out[task]["priorLast"] = last
    return out


def clock_label(ts):
    if ts is None:
        return "-"
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%m-%dT%H:%M")


def session_counts(events):
    """Per task: sessions started, and sessions which stopped themselves
    (`requested: false`: a shard-initiated stop, such as shedding pinned
    shuffle segments, whose cause is in the host logs)."""
    out = collections.defaultdict(lambda: {"sessions": 0, "selfStopped": 0})
    for e in events:
        if e["event"] == "sessionStarted":
            out[e["task"]]["sessions"] += 1
        elif e["event"] == "sessionStopped" and not e.get("requested"):
            out[e["task"]]["selfStopped"] += 1
    return dict(out)


def human_bytes(n):
    if n is None:
        return "-"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(n) < 1000:
            return f"{n:.1f}{unit}"
        n /= 1000
    return f"{n:.1f}PB"


def fmt(v, spec=".2f"):
    return "-" if v is None else format(v, spec)


def render(report):
    lines = []
    w = report["window"]
    lines.append(f"# Run {report['runId']}  ({report['outcome']})")
    if report.get("error"):
        lines.append(f"error: {report['error']}")
    lines.append(f"window: {w['seconds']:.0f}s, from +{w['fromRunStart']:.0f}s of the run")
    lines.append("")
    lines.append("## Events")
    for e in report["events"]:
        extra = {k: v for k, v in e.items() if k not in ("ts", "event", "at")}
        lines.append(f"  +{e['at']:8.1f}s  {e['event']:<16} {json.dumps(extra) if extra else ''}")
    lines.append("")

    lines.append("## Committed throughput and transaction cadence")
    for task, t in report["throughput"].items():
        txn = t["txnSeconds"] or {}
        lines.append(
            f"  {task} ({t['kind']}): {t['committedDocsPerSec']:.0f} docs/s, "
            f"{human_bytes(t['committedBytesPerSec'])}/s; "
            f"{t['transactions']} non-empty txns ({t['commitsPerMinute']:.1f}/min), "
            f"txn seconds p50 {fmt(txn.get('p50'))} p90 {fmt(txn.get('p90'))} max {fmt(txn.get('max'))}"
        )
        if t["emptyTransactions"]:
            lines.append(f"    (+ {t['emptyTransactions']} empty transactions, which read nothing, excluded above)")
        in_flight = report["inFlight"].get(task)
        if in_flight:
            lines.append(
                f"    (+ 1 transaction closed at +{in_flight['closedAt']:.0f}s which isn't known to have committed "
                f"({human_bytes(in_flight['readBytes'])} read), excluded above and below)")
        for binding, sides in t["bindings"].items():
            parts = ", ".join(
                f"{side} {v['docsPerSec']:.0f} docs/s {human_bytes(v['bytesPerSec'])}/s"
                for side, v in sides.items()
            )
            lines.append(f"    {binding}: {parts}")
        start = report["starts"].get(task, {})
        sessions = report["sessions"].get(task, {})
        if start or sessions:
            lines.append(
                f"    start: {start.get('start', '?')}; sessions: {sessions.get('sessions', 0)}, "
                f"of which {sessions.get('selfStopped', 0)} stopped themselves (causes: host logs, 'stopping session')"
            )
        for binding, p in t["sourceProgress"].items():
            first, last = p["first"], p["last"]
            lines.append(
                f"    {binding}: source clock at first commit {first['sourceClock'][:19]}, at last {last['sourceClock'][:19]}; "
                f"bytes behind {human_bytes(first['bytesBehind'])} -> {human_bytes(last['bytesBehind'])} (whole run)"
            )
            prior = start.get("priorLast", {}).get(binding)
            if prior:
                lines.append(
                    f"      resumed run's last commit: source clock {prior['sourceClock'][:19]}, "
                    f"bytes behind {human_bytes(prior['bytesBehind'])}"
                )
    lines.append("")

    if report["phases"]:
        lines.append("## Phases (split at script events)")
        for p in report["phases"]:
            after = f"after {json.dumps(p['after'])}" if p["after"] else "start"
            lines.append(f"  +{p['from']:.0f}-{p['to']:.0f}s  {after}")
            for task, v in sorted(p["committedBytesPerSec"].items()):
                lines.append(f"    {task}: committed {human_bytes(v)}/s in {p['transactions'][task]} txns")
            busy = {n: c for n, c in p["cpuCores"].items() if n != "controller"}
            lines.append("    " + ", ".join(
                f"{n} {c:.2f} cores" + (f" (throttled {p['throttledSecondsPerSec'][n]:.2f} s/s)" if p["throttledSecondsPerSec"].get(n) else "")
                for n, c in sorted(busy.items())))
        lines.append("")

    lines.append("## Source skew and lag (seconds of source clock)")
    for key, s in report["skew"].items():
        skew, lag = s["skewSeconds"] or {}, s["lagSeconds"] or {}
        lines.append(
            f"  {key} ({s['shards']} shards reading): skew mean {fmt(skew.get('mean'), '.1f')} "
            f"p90 {fmt(skew.get('p90'), '.1f')} max {fmt(skew.get('max'), '.1f')}; "
            f"lag mean {fmt(lag.get('mean'), '.0f')} max {fmt(lag.get('max'), '.0f')}"
        )
    lines.append("")

    lines.append("## Shuffle pressure")
    for shard, s in sorted(report["shuffle"].items()):
        backlog = s.get("diskBacklogBytes") or {}
        lines.append(
            f"  {shard}: read {human_bytes(s.get('sliceReadBytesPerSec'))}/s, "
            f"appended {human_bytes(s.get('logAppendedBytesPerSec'))}/s, "
            f"backlog mean {human_bytes(backlog.get('mean'))} max {human_bytes(backlog.get('max'))}, "
            f"stalled reads max {fmt(s.get('stalledReadsMax'), '.0f')}"
        )
    lines.append("")

    lines.append("## Append backpressure")
    appends = report["appends"]
    for journal, j in sorted(appends["journals"].items()):
        lines.append(
            f"  {journal}: appended {human_bytes(j['appendedBytesPerSec'])}/s, "
            f"delayed {human_bytes(j['delayedBytesPerSec'])}/s ({fmt(j['delayedShare'], '.0%')})"
        )
    for collection, p in sorted(appends["partitions"].items()):
        lines.append(f"  {collection}: partitions {p['first']} -> {p['last']} (max {p['max']})")
    lines.append("")

    lines.append("## Resources by cgroup node")
    for node, r in sorted(report["resources"].items()):
        mem = r["memoryBytes"] or {}
        io = "; ".join(
            f"{dev} r {human_bytes(v['readBytesPerSec'])}/s w {human_bytes(v['writeBytesPerSec'])}/s"
            for dev, v in r["io"].items()
        )
        lines.append(
            f"  {node:<16} cpu {r['cpuCores']:.2f} cores, throttled in {fmt(r['throttledPeriodsFraction'], '.0%')} of enforced periods "
            f"({r['throttledSecondsPerSec']:.2f} s/s), pressure cpu {fmt(r['cpuPressureSome'], '.0%')} "
            f"io {fmt(r['ioPressureSome'], '.0%')} mem {fmt(r['memoryPressureSome'], '.0%')}, "
            f"memory max {human_bytes(mem.get('max'))}" + (f", io {io}" if io else "")
        )
    lines.append("")

    lines.append(f"## Timeline ({report['bucketSeconds']:.0f}s buckets, offsets from the run's start)")
    lines.append("  committed throughput is lumpy in buckets narrower than transactions: widen --bucket, or see Phases")
    rows = report["timeline"]
    if rows:
        tasks = sorted({t for r in rows for t in r["committedBytesPerSec"]})
        # Hosts, plus any node which was throttled at all.
        nodes = sorted({n for r in rows for n in r["cpuCores"] if "/" not in n and n != "controller"}
                       | {n for r in rows for n, v in r["throttledSecondsPerSec"].items() if v > 0})
        skews = sorted({k for r in rows for k in r["skewMaxSeconds"]})
        for i, task in enumerate(tasks):
            lines.append(f"  t{i} = {task}")
        for i, key in enumerate(skews):
            lines.append(f"  s{i} = {key}")
        lines.append("  MB/s: committed source bytes; txns: non-empty commits; clock: committed source clock at the bucket's end")
        lines.append("  skew: max source skew across shards, seconds")
        lines.append("  cpu: cores used; thr: throttled seconds per second")
        header = "  " + "window".ljust(13) + "".join(
            f"{'MB/s t'+str(i):>10}{'txns':>5}{'clock t'+str(i):>13}" for i in range(len(tasks)))
        header += "".join(f"{n+' cpu':>18}{'thr':>6}" for n in nodes)
        header += "".join(f"{'skew s'+str(i):>9}" for i in range(len(skews)))
        lines.append(header)
        for r in rows:
            line = "  " + f"+{r['from']:.0f}-{r['to']:.0f}s".ljust(13)
            line += "".join(
                f"{r['committedBytesPerSec'].get(t, 0)/1e6:>10.1f}{r['transactions'].get(t, 0):>5}"
                f"{clock_label(r['committedSourceClock'].get(t)):>13}" for t in tasks)
            line += "".join(f"{fmt(r['cpuCores'].get(n)):>18}{fmt(r['throttledSecondsPerSec'].get(n)):>6}" for n in nodes)
            line += "".join(f"{fmt(r['skewMaxSeconds'].get(k), '.0f'):>9}" for k in skews)
            lines.append(line)
    lines.append("")

    lines.append("## CPU by thread (cores)")
    for proc, by_name in sorted(report["threads"].items()):
        busy = {n: c for n, c in by_name.items() if c >= 0.005}
        parts = ", ".join(f"{n} {c:.2f}" for n, c in sorted(busy.items(), key=lambda kv: -kv[1]))
        lines.append(f"  {proc}: {parts or 'idle'}")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--skip", type=parse_duration, default=30.0)
    parser.add_argument("--until", type=parse_duration, default=None)
    parser.add_argument("--bucket", type=parse_duration, default=30.0, help="timeline bucket width")
    parser.add_argument("--json", action="store_true", help="emit the report as JSON")
    args = parser.parse_args()

    run = args.run_dir
    events = read_ndjson(run / "events.ndjson")
    cgroups = read_ndjson(run / "samples" / "cgroups.ndjson")
    metrics = read_ndjson(run / "samples" / "metrics.ndjson")
    thread_samples = read_ndjson(run / "samples" / "threads.ndjson")
    journal_samples = read_ndjson(run / "samples" / "journals.ndjson")
    stats, in_flight = split_in_flight(read_stats(run), events)

    if not events:
        sys.exit(f"{run} has no events.ndjson: is it a run directory?")
    t_start = parse_ts(events[0]["ts"])
    t0, t1 = window_of(events, cgroups, args.skip, args.until)

    outcome, error = "running", None
    for e in events:
        if e["event"] == "runStopped":
            outcome = "stopped"
        elif e["event"] == "runFailed":
            outcome, error = "failed", e.get("error")

    series = metric_series(metrics)
    clocks = source_clocks(series)
    report = {
        "runId": run.name,
        "outcome": outcome,
        "error": error,
        "window": {"start": t0, "end": t1, "seconds": t1 - t0, "fromRunStart": t0 - t_start},
        "events": [dict(e, at=parse_ts(e["ts"]) - t_start) for e in events],
        "throughput": committed_throughput(stats, t0, t1),
        "inFlight": {
            task: {
                "closedAt": close_of(doc) - t_start,
                "readBytes": sum(int(sides.get("right", {}).get("bytesTotal", 0))
                                 for binding, sides in binding_sides(doc).items() if binding != "(derived)"),
            }
            for task, doc in in_flight.items()
        },
        "starts": starts(run),
        "sessions": session_counts(events),
        "skew": source_skew(clocks, t0, t1),
        "shuffle": shuffle_pressure(series, t0, t1),
        "appends": append_backpressure(series, journal_samples, t0, t1),
        "resources": resources(cgroups, t0, t1),
        "threads": threads(thread_samples, t0, t1),
        "bucketSeconds": args.bucket,
        "timeline": timeline(stats, cgroups, clocks, t0, t1, args.bucket, t_start),
        "phases": [dict(p, **{"from": p["from"] - t_start, "to": p["to"] - t_start})
                   for p in phases(events, stats, cgroups, t0, t1)],
    }
    if args.json:
        json.dump(report, sys.stdout, indent=2)
        print()
    else:
        print(render(report))


if __name__ == "__main__":
    main()
