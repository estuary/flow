# Experiment 13: helper crash and cleanup

**PASS on cleanup, FAIL on one half of the panic gate.** A `SIGKILL` to the shim
mid-transaction is clean in every way the design needs: the runtime sees the
socket close within half a second and fails the task, the container and its netns and
tap are gone, the scratch space comes back, and the runtime removes the
per-connector directory it made. A guest kernel panic is prompt too - the helper
is gone five seconds after the workload starts, with no hang and no reboot
loop - but **it exits 0**. PLAN's gate asks for non-zero. Nothing leaks either
way; what fails is the ability to tell a panicked guest from a clean one by exit
code.

Measured at commit `0c68f442cf1` (this package's parent; the runs precede its
commit), `spike/tasks/exp13-crash.sh`. Connector
`ghcr.io/estuary/materialize-sqlite@sha256:0f04fd0a0cb7f0563839acbe56ff546f05e4236f7e8b692f384e0420eebcae85`
(it moved from `7c89b59b...` during this session: `flowctl preview` pulls the
image). Panic guest
`ghcr.io/estuary/derive-python@sha256:c26548740a9e967274f6d7b9c79bed73630bf61bd187c3afca7564577ef364c7`.
Raw data: `data/exp13-runtime.log`, `data/exp13-panic-console.log`.

## SIGKILL mid-transaction

`flowctl preview` driving `materialize-sqlite` through the fake reactor with the
spike switch on, fed from a **FIFO fixture** so the connector is genuinely busy:
the preview streams a named pipe as one unbounded session, so transactions keep
arriving for as long as the writer keeps writing. A regular file is read eagerly
and its eight documents would be committed before the helper finished booting.
The helper was killed after **46 committed transactions**, with `tap0` up.

| observation | result |
|---|---|
| `fs_` helpers running at the kill | exactly one |
| transactions after the kill | 0 or 1, whatever was already in flight |
| the runtime's account | `Materialize error (expected connector response) from connector` / `h2 protocol error: error reading a body from connection` |
| `flowctl` exit | 1, 0.3-0.4 s after the kill |
| helper container afterwards | none |
| named netns | 1 -> 2 -> 1 |
| `df` on the reactor filesystem | back below the level it held with the helper alive |
| `<id>/` under the reactor directory | removed by the runtime |

The full runtime log, with a marker at the instant of the kill, is
`data/exp13-runtime.log`.

Two things about this pass are worth keeping, because both cost a run to find:

- **One preview starts the connector twice** - the shard validates, then opens -
  and the first helper is gone within a couple of seconds (WP06 recorded this).
  Picking "the first `fs_` container" while both exist kills the wrong one: the
  runtime carries on committing, the preview never ends, and the teardown checks
  pass afterwards on a container that died of natural causes. The pass now waits
  until exactly one `fs_` container is up *and* a transaction has committed.
- **podman tears down after its client has exited.** Taking the "afterwards"
  measurements when `flowctl` returns reports netns and disk that have not been
  reclaimed yet. The pass polls until the containers are gone and the netns
  count is back, and calls the settle time out (0.0 s when the kill is clean, 9 s
  when the run ends through EOF instead).

The tap needs no cleanup code and gets none: it lives in the container's own
network namespace and goes with it. The scratch disk needs none either - it is
an `O_TMPFILE`, and the space returns when the last fd closes. Experiment 11
measures that return at 3995 MiB; this pass only shows that a `SIGKILL` is not
different.

## Guest kernel panic

The guest has no sysrq, so the panic is armed with a sysctl and provoked by
allocating past the guest's RAM:

```
--run-as-root --exec /bin/sh -c
    'echo 1 > /proc/sys/vm/panic_on_oom;
     /usr/local/bin/python -c "a=[]; [a.append(bytearray(64<<20)) for _ in range(1000)]"'
```

with `--memory-mib 1024` and 512 MiB of cgroup overhead, so the guest runs out
before the container does. The guest's last words, from `data/exp13-panic-console.log`:

```
[    3.861356] Kernel panic - not syncing: Out of memory: system-wide panic_on_oom is enabled
[    3.862439] Kernel Offset: disabled
[INFO  krun_vmm] Vmm is stopping.
```

| half of the gate | result |
|---|---|
| the kernel panics | yes, at 3.86 s of guest uptime |
| the helper exits promptly rather than hanging | yes, 5.0 s after the workload started |
| the helper exits non-zero | **no: 0** |

### Why it is 0, and why that is libkrun's answer and not a bug in the shim

The shim never regains control: `krun_start_enter` does not return, and libkrun
`_exit`s the process itself. Which value it picks is decided in
`Vmm::process` (`src/vmm/src/lib.rs:405-430`): the VMM prefers the exit code
that libkrun's init stored through the `0x7602` ioctl, and falls back to the
vcpu's, which defaults to `FC_EXIT_CODE_OK` when the exit came from the i8042
controller - a reset. A guest that panics and reboots never reaches init's
report, so the fallback is what runs, and the fallback is 0.

The console shows no reboot banner and the VMM stops at the panic, so `panic=-1`
does not produce a loop here; it produces a reset that libkrun reads as a clean
shutdown.

### What that costs, and what it does not

It does not cost containment, and it does not cost detection. Experiment 13's
first half is the evidence: the runtime learns that its connector is gone from
the **socket**, under half a second after the process dies, and fails the task with a message
naming the connector. That path does not consult the exit code at all.

What it costs is diagnosis. A helper that exits 0 after a guest kernel panic is
indistinguishable, from the reactor's records alone, from a connector that
finished cleanly - and the kernel's own explanation went to the helper's stdout,
which is interleaved with the workload's and is not structured. An operator
looking at "task stopped, connector exited 0" has no way to reach "the guest ran
out of memory". Two things would change that, neither of them this spike's to
decide:

- **The shim could watch its own guest.** It has the console; a line matching
  `Kernel panic` seen before `krun_start_enter` returns is enough to exit with a
  chosen code, or to emit a structured log line first. That is shim work, not
  libkrun work.
- **The runtime could treat a zero exit with no graceful shutdown as a failure**
  regardless of code. It already fails the task on the socket close; the exit
  code only decorates the message.

Recorded as an open question. PLAN's gate is written as "the helper exits
non-zero promptly rather than hanging", and the accurate result is: prompt, not
non-zero.

## A smaller observation

Under `--debug`, flow-init's two timing lines arrive on the helper's stderr with
their prefixes missing - bare `203970` where the undebugged run gives
`flow-init: timing stage=start boot_us=203970`. The guest's stderr and the teed
kernel console share one host descriptor by libkrun's construction, and under
`--debug` they are interleaved at sub-line granularity. Nothing in the spike
parses those lines, and the undebugged path is unaffected, but anything that
scrapes guest stderr should not be run with `--debug`.
