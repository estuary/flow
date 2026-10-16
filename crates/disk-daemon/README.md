# disk-daemon

`disk-daemon` serves mounted filesystems whose contents commit atomically with an
external transaction. It lets Flow connectors keep databases, indexes, and other
working state in ordinary files and recover them alongside their checkpoints.

Each disk is a disposable sparse image exposed through Linux `ublk` and mounted
as ext4. Block mutations go to a Gazette journal; the client coordinates their
commit with its own durable state. Recording below the filesystem keeps the
journal independent of the application's storage engine.

The crate provides `flow-disk-daemon` and its Rust client, independently of Flow's
runtime and connector protocols. Flow integration still needs to place mounts in
connector sandboxes, persist acknowledgements with checkpoints, manage journal
and fragment lifetimes, and define shard splitting.

## Entry points and ownership

An **attachment** owns one disk for the life of a bidirectional gRPC stream. It
starts as a standby and becomes the writer on promotion. The client controls it over
a Unix socket; the workload uses the returned mount through ordinary file I/O.

```text
transaction client ── Attach RPC ──► attachment task
       │                                  │
       │ passes mount to workload         │ opens, prepares, tears down
       ▼                                  ▼
    workload ──► ext4 ──► ublk ──► owner thread + io_uring
                                      │              │
                                 image I/O      recorded mutations
                                      │              │
                                      ▼              ▼
                               sparse image     journal writer ──► Gazette
```

Paths below are relative to `src/`.

| Path | Start here for |
| --- | --- |
| `client.rs` | `Client`, `Standby`, `Disk`, and `Error`; the caller's transaction and recovery contract. |
| `lib.rs` | Protocol exports, block size, disk content type, and attachment status codes. |
| `main.rs` | Process setup: crypto provider, tracing, and the runtime. |
| `daemon.rs` | Flags, configuration, the `Disk` gRPC service and its socket, shared broker credentials, and process drain. |
| `attachment.rs` | `Attachment`: request ordering, promotion, and error codes. `MountedDisk`: bootstrap commit, prepare ordering, and teardown of mount, device, and writer. |
| `device/mod.rs` | `Device` lifecycle, admission at the prepare boundary, and its `Recorded` stream. |
| `device/owner.rs`, `device/queue.rs` | Owner thread, ring, tag ownership, and character-device data copies. |
| `device/target.rs` | The ublk target: request handling, mutation ordering and cuts, horizon opening, copies, and discharge, and image I/O, testable without a real device. |
| `device/recording.rs`, `device/wake.rs` | Bounded recording and wakeups. |
| `ublk/` | Kernel control commands and ABI bindings generated from `ublk_cmd.h`. |
| `image.rs`, `chunk.rs` | Sparse image allocation and the durable write/punch codec. |
| `horizon.rs` | Recovery-horizon bitmap and compaction policy. |
| `filesystem.rs` | ext4 format, mount, ownership, and `syncfs`. |
| `journal/lifecycle.rs` | Opening, standby playback, fencing, and promotion. |
| `journal/mod.rs` | Append state shared by the lifecycle and writer, head confirmation, and parsing and setting the recovery-floor label. |
| `journal/replay.rs`, `journal/sequencer.rs`, `journal/uncommitted.rs` | Replay I/O, pure sequencing decisions, and buffered uncommitted deltas. |
| `journal/writer.rs`, `journal/ledger.rs` | The writer, which appends mutations during a drain, and the pure delta ledger. |

The wire contract and durable record schema are in
[`disk.proto`](../../go/protocols/disk/disk.proto). Public modules are `client`
and `daemon`; the implementation modules are private.

## Commit and recovery boundaries

A **delta** contains block changes since the previous commit, including any
compaction copies. `Prepare` confirms its data in Gazette and returns an opaque,
unappended acknowledgement. The client stores those bytes atomically with its
own state, then sends `Acknowledge`. After failure, promotion repairs any saved
acknowledgement that has not already committed. The client must hand back its
saved acknowledgement on every recovery: a replay which applies nothing under one
is how promotion tells lost committed state from a fresh disk. See `Disk::prepare` for the
application flush and write-pause requirements.

Only one prepared delta may await acknowledgement. The writer stops consuming
mutations until the acknowledgement is confirmed; device writes eventually park
behind the bounded recording channel. `journal/ledger.rs` explains the Gazette
sequencing rule requiring this barrier. `attachment.rs` defines the exact cut order
and commits format and mount writes before returning `Promoted`.

Replay applies only acknowledged deltas, holding an uncommitted delta in memory
and spilling beyond 1 MiB. The same replay serves immediate opens and hot
standbys; promotion fences before finishing replay. The client chooses which
attachment to promote. Concurrent promotions may both succeed, with the later fence
winning. See `journal/lifecycle.rs` and `journal/replay.rs` for the ownership
and recovery invariants.

Recovery preserves committed filesystem contents. Mounting and ext4 journal
recovery may change bookkeeping, so a recovered block image need not be
byte-for-byte identical. The caller must keep device size stable across attachments;
it is not stored in the journal.

A **recovery horizon** becomes a **recovery floor** once committed changes and
copies cover every allocated block at or after its opening offset. Compaction
trades write amplification for less replay, with no copying while the disk is
idle. The recovery range is a policy target, not a hard bound on transaction size
or process memory. The device opens a horizon at a delta's first mutation and
reports its discharge at a cut; the writer records the opening offset and stores
the floor. See `horizon.rs`, `device/target.rs`, and the writer ledger.

## Journal and host responsibilities

The caller creates the journal and owns its specification, including replication,
fragment stores, and compression. The daemon checks none of it, and updates only
the recovery-floor label. Spec convergence should preserve that label, which bounds
replay and pruning; without it both start from zero. The spec must set no fragment
retention: Gazette deletes fragments by age, which cannot see the floor.

An external pruner may delete fragments wholly below the floor, leaving an age
margin for standbys to finish backfill. A gap after replay begins fails the
attachment; a replacement starts from the current floor. The daemon deletes no
fragments. A floor label which is not an offset below the journal head fails the
attachment: replay from it would read nothing and format a fresh disk. A wrong
label below the head, such as one copied from another journal, goes undetected and
may fail replay or skip committed history. An operator may correct a known-wrong
label only if no fragments below it were deleted.

The host needs Linux `ublk` support, e2fsprogs with `assume_storage_prezeroed`, and
an image filesystem supporting `O_TMPFILE` and hole punching. Required privileges
are `CAP_SYS_ADMIN` for devices and mounts, `CAP_SYS_RESOURCE` for owner-thread I/O
flusher status, and `CAP_CHOWN` for recovered mount ownership. The daemon also needs
access to `/dev/ublk-control` and its `/dev/ublkc*` nodes.

Mounts belong to the Unix-socket peer UID/GID, so clients need no privilege. Socket
access authorizes access to all disk journals in the data plane. The socket is
mode `0666`; restrict traversal of its parent directory. The daemon shares a
refreshing, self-signed broker token scoped to the disk content type. Clients
supply neither broker endpoints nor credentials; key rotation requires a restart.

`SIGTERM` and `SIGINT` stop new attachments and drain existing ones. Teardown
abandons appends while continuing to drain mutations, unmounts, then stops and
deletes the device. This order is required because unmount writes and device stop
waits for parked requests. See `attachment.rs` and `device/mod.rs`. The daemon
waits for every teardown, so the supervisor's stop timeout bounds a drain.

Deployment owns capacity and placement: the daemon reserves no space and limits
no disk count. Mounts must remain visible to the process passing them into a
sandbox, which must apply `nodev,nosuid,noexec` when re-exporting them. The daemon's
help describes its configuration.

## Examples and tests

`examples/` covers commit and reopen, multi-disk transactions, and hot standby
promotion. `examples/demo-services.sh` starts their local services; each example
documents its invocation.

`tests/` exercises the shipped binary through the client, ordinary filesystem I/O,
and an independent Gazette client. `tests/support/` provides the data-plane
`Fixture`, privileged `Daemon` child, raw `Attachment` stream, and expected file
`Tree`. Cases compare recovered files with the workload's tree rather than a second
implementation of replay.

Inline unit and property tests cover internal modules without devices or
privilege. Broker tests use `src/test_support/`; both suites use the generic journal
helpers in `e2e-support`.

```console
mise run build:gazette
mise exec -- cargo nextest run -p disk-daemon
```

The integration suite runs the daemon through `sudo -n`, leaving Cargo
unprivileged. Run through nextest: its test group serializes binaries sharing the
host's `ublk` control device. The host prerequisites above still apply.
