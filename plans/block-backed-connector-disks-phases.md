# Disk Daemon — Implementation Phases

Companion to [block-backed-connector-disks.md](block-backed-connector-disks.md)
(the design). Scope here is **the disk daemon only**: the general-purpose,
Gazette-backed block-device service. The end state is a stand-alone
`flow-disk-daemon` binary — gRPC session API over a Unix socket, complete
durability semantics (fence, deltas, recovery, horizons), and a test suite
that exercises all of it without any Flow runtime involvement. Runtime
integration (provisioning shards, `Persist`/`AI:` coordination, container
mounts) is deferred and sketched at the end.

Each phase lands as its own reviewed unit: it compiles warning-free
(`RUSTFLAGS` makes warnings errors), passes its tests, and leaves the tree
releasable.

## How the daemon maps onto this repo

Grounding facts that shaped the phasing:

- **The daemon is Gazette-generic.** Its protocol uses opaque
  acknowledgements and generic journal metadata; Flow-specific coordination
  (`AI:` entries, connector timing) is the runtime's job, later. The one
  label it maintains is named by configuration, not by convention
  (decision 7). Its message types are packaged in the shared `proto-flow` /
  `proto-grpc` crates like every other protocol in this repo (decision 2) —
  packaging, not a semantic coupling.
- `crates/gazette` already supports everything the journal writer needs:
  streaming retried appends; `check_registers` / `union_registers` /
  `subtract_registers` on `AppendRequest`
  (`crates/proto-gazette/src/protocol.rs:459`) for the author fence;
  confirmed write head via `AppendResponse.commit.end`; `apply` for the
  floor label. Register caveats are documented in the proto: a zero-byte
  append is a pure register read, an empty register set matches any selector,
  and a failed RPC may still have updated registers.
- `gazette::uuid` (`crates/proto-gazette/src/uuid.rs`) provides `Producer` /
  `Clock` / `Flags` and a pure `sequence()` transition function implementing
  Gazette sequencing semantics (dedup, rollback, release-on-ACK) — the
  replay reader builds on it. Grouping records into transactions is
  per-application and ours to write.
- Gazette fixed framing is magic `[0x66, 0x33, 0x93, 0x36]` + u32-LE length
  (`message/fixed_framing.go` in the gazette Go module, including
  desync-scan recovery). No Rust implementation exists. It is Gazette's wire
  format rather than the daemon's, so Phase 3 adds one to `crates/gazette`
  beside the framings already there.
- **Gap:** the Rust read path does not implement SNAPPY decompression
  (`crates/gazette/src/journal/read/mod.rs:274`), but the design specifies
  SNAPPY for disk journals. Fixed in Phase 5.
- Test infra: `crates/e2e-support` spawns real etcd + gazette brokers over
  Unix sockets in a tempdir (prereq: `mise run build:gazette` →
  `$GOBIN/gazette`). Privileged tests have no precedent in the repo; the
  pattern we add (decision 4) keeps them inside the default nextest run by
  pushing privileged work into `sudo`-spawned child processes.
- This dev machine can run everything: kernel 6.8 with `ublk_drv` loaded and
  `/dev/ublk-control` present, e2fsprogs 1.47.0 (minimum for
  `assume_storage_prezeroed`), ext4 with hole punching, passwordless sudo.
- Server shape precedents: gRPC-over-UDS serving via `UnixListener` +
  `serve_with_incoming_shutdown` (`crates/runtime-next/src/task_service.rs:82`);
  a long-running Rust binary with `service-kit` admin/metrics surface
  (`crates/runtime-sidecar/src/main.rs`). Session-fed credential replacement
  maps onto `tokens::manual()`.

## Decisions taken (veto in review)

1. **Crate `crates/disk-daemon`**, lib + `[[bin]] name = "flow-disk-daemon"`
   (binary-name prefix per `flow-connector-init` convention). The lib
   re-exports the protocol types and exposes a session client so a future
   runtime phase — or any other system — can consume it.
2. **The proto follows the standard pipeline.** `go/protocols/disk/disk.proto`
   beside the other protocol packages; messages generated into
   `crates/proto-flow` as `proto_flow::disk` (target added to
   `proto_build::Boilerplate::resolve_flow_targets`, package added to the
   pbjson build list); tonic stubs generated into `crates/proto-grpc` behind
   a `disk_client` / `disk_server` feature pair. `mise run build:rust-protobufs`
   already covers proto-flow and proto-grpc, so it needs no changes.

   **No Go bindings.** This proto is absent from
   `mise/tasks/build/go-protobufs`, unlike every other protocol here. Nothing
   in Go imports it, and nothing is planned to: every integration point in the
   deferred section is Rust. Of the protos that task does generate, `flow` is
   imported by 72 Go files, `runtime` and `ops` by 28 each, and the rest by a
   handful — each earned its bindings by being used.

   Leaving it out buys proto3 `optional`, which `protoc-gen-gogo` refuses and
   which a configuration message needs: it is the only way to tell an unset
   field from a deliberate zero. `JournalConfig` uses it for the two fields
   where Gazette reads zero as a choice. Adding Go bindings later means
   dropping `optional` or replacing gogo, which is archived upstream and a
   repo-wide problem in its own right.

   This packages the protocol as Flow's, which the daemon is not: a client
   speaking to it must depend on `proto-flow` and so compile Flow's whole
   catalog model. Accepted, because stand-alone deployment is aspirational and
   the fix is one mechanical edge — move the generated module to a
   `crates/proto-disk` and repoint the re-export, with no change to the wire
   format or the daemon. Two things travel with that change if it ever
   happens: rename the proto package from the very generic `disk` to something
   like `estuary.disk` (it sets the gRPC method path, so it is free now and
   breaking after first deployment, and a dotted package nests awkwardly
   inside proto-flow), and settle licensing, since BSL governs whether anyone
   outside Estuary may deploy the binary at all.
3. **One proto package `disk`** holds both the session gRPC (`Open/Opened`,
   `Publish/Published`, `Commit/Committed`, `Broker`) and the journal
   record messages (`DiskRecord`, `Chunk`). Its only Gazette import is
   `CompressionCodec`; `gogoproto.casttype` covers the rest, giving Go the
   `protocol.Journal`, `protocol.Endpoint`, and `protocol.FragmentStore`
   types.
4. **Privileged tests run in the normal suite.** No special profile or
   manual task: privileged work happens in child processes spawned with
   `sudo -n`. Two binaries serve as that child, and both persist. The
   `flow-disk-daemon` binary is the child from Phase 4 on, so end-to-end tests
   exercise the daemon exactly as it ships. A scenario helper is the child for
   tests of the library beneath it, because privilege needs a process boundary
   and some things the session protocol does not reach: a queue depth shallow
   enough to force backpressure, and the image digests and extent lists which
   show a replay matching in holes as well as in bytes. `cargo nextest run -p
   disk-daemon` therefore covers everything on any machine with passwordless
   sudo and `ublk_drv` — dev boxes and CI runners alike (CI adds a
   `modprobe ublk_drv` step). Cargo itself never runs as root, so the target
   directory stays user-owned. Missing prerequisites fail fast with an
   actionable message rather than silently skipping, and a nextest serial
   test-group serializes contention on `/dev/ublk-control`.
5. **Tunables are configuration, not constants.** As a general-purpose tool
   the daemon exposes its knobs; Estuary's IaC will pin them in production.
   Two kinds with different rules:
   - *Per-disk and durable* — device size and block size are required `Open`
     parameters with no configured default, because a flag change must not
     reinterpret disks that already exist. Block size shapes the chunk
     encoding and bitmaps, so it is fixed once a disk first publishes and
     every later `Open` must present the same value (the design's
     10 GiB / 4 KiB are the caller's values, not the daemon's). `u32` block
     indexing caps device size at 2³² × block size. Neither value is recorded
     durably, so stability is an unenforced client contract; see the
     configuration section.
   - *Policy, safe to change between daemon restarts* — compaction (`k`,
     `r`, horizon minimum) are daemon flags with the design's values as
     defaults. Replay derives all state from
     journal contents, so retuning policy never invalidates an existing disk.
     Per-`Open` policy overrides can be added later without format impact.
     There are no resource ceilings; see the configuration section.

   The full split, including which fallbacks must be explicit rather than
   silent, is in [Configuration surface](#configuration-surface).
6. **Credentials are session-supplied or absent; the daemon mints none.** A
   token reaches the daemon only through `Broker`, in `Open` or as a later
   refresh request. If none is supplied it connects anonymously, which is
   correct against brokers running without authorization and wrong in a way
   that fails immediately otherwise. There is no configured signing key and no
   token minting, so the privileged daemon holds no key material and has one
   fewer code path.

   Session-supplied tokens are the primary path for reasons beyond custody:
   broker-enforced per-session scope turns a session-mixup bug in a
   many-tenant daemon into an authorization error rather than a cross-tenant
   journal write, and the runtime already holds correctly-scoped,
   auto-refreshing tokens for exactly this journal. In Flow a signing key
   would not even suffice — tokens come from the control plane's authorize
   flow, which the daemon would otherwise have to learn.

   The endpoint travels with the token, in the same `Broker`, because that is
   how a journal client extracts the pair. It has no config fallback: a
   credential refresh carries the endpoint anyway, so a daemon-wide default
   would be dead in every deployment which authenticates, while still letting
   a client which omitted an endpoint reach the host's cluster instead of
   failing. A session which names no endpoint is a terminal error.
7. **The daemon owns the recovery floor label, end to end.** It derives the
   floor, so it also persists it: on horizon completion it advances a label
   on its own journal's spec, and at `Open` it reads that label back as its
   replay seek hint.

   The label *name* is a **required** daemon flag with **no default** — a
   general-purpose daemon should carry no system's label vocabulary, and
   `estuary.dev/truncated-at` is Flow's, so each deployment states its own.
   There is no opting out: without a floor every recovery replays from the
   earliest fragment and the required journal range grows without bound, which
   is not a mode the daemon supports. A daemon which cannot write the label
   fails that write loudly instead.

   The label *value* encoding is fixed, not configurable: 16-character
   lowercase hex of the u64 message clock, matching
   `labels::truncated_at_value`. Fixed-width hex orders lexicographically as
   it does numerically, and matching the existing encoding means tooling
   which reads that label name generically cannot misparse a disk journal.

   Consequence: the floor leaves the protocol entirely. `Open` needs no hint
   because the daemon reads what it writes, and no reply reports a floor
   because no client consumes one — compaction progress belongs on the
   daemon's metrics surface. The daemon
   must implement the advance-only CAS write that Flow's existing
   `truncated-at` writer does (`list_watch` for `mod_revision`, `apply` with
   `expect_mod_revision`, treat `EtcdTransactionFailed` as a lost race and
   retry on the next watch snapshot), and it must never write a value ahead
   of the derived floor.

   The write must **mutate the listed spec** — change the one label and apply
   that value back — never assemble a fresh spec from `JournalConfig`. The
   live spec carries fields the daemon does not model, notably `suspend`,
   which Gazette sets when it idles a journal and which
   `crates/activate/src/lib.rs:570` is careful to pass through. Rebuilding
   would silently clobber them. When runtime integration lands, the runtime must
   *not* extend its own label-apply path to disk journals: two writers to one
   spec field is exactly what this decision avoids.
8. **The daemon creates the journal, lazily, from typed inputs.** Decision 7
   already made it a spec writer, so creation is the simpler case of something
   it does anyway: an insert-only `apply` (`expect_mod_revision: 0`,
   tolerating `EtcdTransactionFailed`) rather than a read-modify-write CAS.
   The client keeps spec convergence, truncation, and deletion.

   Creation is lazy because format and mount output is **retained rather than
   appended as it arrives**. A disk which is never written carries no
   information — it is reproducible by formatting again — so the trigger is
   the first device mutation *after* mount, and a disk no connector writes
   creates no journal. The retained set is bounded and known before any
   connector runs: prezeroed format leaves the inode tables and internal
   journal as holes, so it is superblock, group descriptors, bitmaps, root
   dir, and `lost+found` — single-digit MB. Nothing else is ever buffered,
   which is what rules out the alternatives: a client-triggered creation
   cannot know when the disk was touched, and waiting to be told would mean
   holding an unbounded delta until the next `Publish`.

   The journal name and its creation inputs travel together in a typed
   `JournalConfig` rather than a `JournalSpec`, so the daemon builds the spec
   and owns the fields a disk's recoverability depends on — its read-write
   flags, and validation that it can decompress the chosen codec, since it
   replays this journal to recover the disk. Exposed fields are those which
   must be correct at creation or vary by tenant: fragment stores,
   replication, labels, fragment length, flush interval, refresh interval, max
   append rate, and compression codec — the last using Gazette's own
   `CompressionCodec` enum so the daemon supports whatever Gazette does, with
   nothing to drift. Changing the codec later is safe: each fragment records
   the codec it was written with and readers honor it.

   Two fields of `assemble::recovery_log_template` are deliberately *not*
   exposed. **Retention** is out because Gazette deletes fragments by age,
   which cannot see the recovery floor, so a retention shorter than a
   horizon's completion time would silently delete records a live disk still
   needs. **`path_postfix_template`** is out for the same reason one step
   removed: date-prefixed fragment paths are what a bucket lifecycle rule keys
   on, so exposing it puts age-based expiry back within reach. Recovery logs
   leave it empty and disk journals have no external reader to partition for.

   Every field is required, and the daemon holds no default for any of them.
   A journal's spec is created once and never converged, so a value the daemon
   invented would be one the disk is stuck with for its life, and changing the
   daemon later would not reach it. Requiring the value puts it with the party
   which will live with it.
9. **A disk is served by one thread of its own, holding its own `io_uring`.**
   This replaces the pooled owner threads Phase 2 built, and lands as its own
   change once Phase 4 is reviewed.

   The thread is the kernel's requirement, not a preference. `ublk` binds a
   device's queue to the thread which issues its first `FETCH_REQ` and rejects
   every later command for that queue from any other thread with `EINVAL`. A
   tokio task migrates between workers, so it cannot serve a queue: this was
   tried, and the trace is unambiguous — the first request is served, and the
   `COMMIT_AND_FETCH_REQ` which follows it from another worker returns `-22`,
   after which nothing re-arms and the device wedges. Kernel 6.8 has no flag
   which relaxes this, and a later per-io daemon flag would not help: it moves
   the binding from queue to tag, because the kernel delivers a request by
   completing that tag's fetch and routes that completion to the task which
   submitted it. Affinity is inherent, not a granularity choice. `libublk`
   keeps its ring in a thread-local for exactly this reason.

   What was worth taking from the task idea is the shape, not the mechanism:
   one owner per disk rather than a pool. That deletes the round-robin which
   assigned disks to owners, the map of disks each owner held, the device id
   packed into every `user_data`, and the `--owners` flag — whose right value
   depends on a host's CPU count, which is the kind of knob a fleet of mixed
   machines should not carry. `wake.rs` stays, because a thread blocked in
   `submit_and_wait` still needs an out-of-band poke, and so does
   `capture.rs`'s pair of waits.

   The cost is a thread and a ring per disk. A thread parked in
   `io_uring_enter` is a `task_struct`, a 16 KiB kernel stack, and a user
   stack this crate caps at 256 KiB of address space; a 128-entry ring is
   about 12 KiB. So a hundred disks cost under four megabytes, and five
   hundred under twenty. Idle owners are not on any runqueue, so they cost the
   scheduler nothing until their device has work.

   Two limits do scale with disks and are worth naming. The first is systemd's
   `TasksMax`, whose default is derived from the host's `kernel.pid_max` and so
   differs between machines; the unit file should set it rather than inherit
   it. The second is the kernel's own `io-wq` workers, which serve operations
   `io_uring` cannot complete inline — punches reach them in ordinary
   operation. Their defaults come from the CPU count and `RLIMIT_NPROC`, and a
   pool per ring would mean a pool per disk, so every disk's ring attaches to
   one anchor ring with `IORING_SETUP_ATTACH_WQ` and that pool's size is
   registered explicitly.

   One property improves over the pool: each disk gets its own share of CPU
   time, so a disk running a large `encode_write` scan cannot delay another
   disk's completions. That was the jitter Phase 7 was going to measure, and
   this arrangement removes the question.

## Configuration surface

Two relationships between daemon configuration and a session's `Open`:

**Session-only, required.** The journal name, `recovered_acks`, `device_size`,
and `block_size`. The last two are durable per-disk facts and deliberately
have no configured default: a flag change would otherwise reinterpret every
existing disk on the host at once — silently for a grown device size, and
catastrophically for a changed block size, which misplaces every replayed
chunk. Requiring them puts the value with the party that has durable state to
derive it from.

Neither value is recorded durably, so their stability is a client contract the
daemon does not enforce. A mismatched block size surfaces as an image that will
not mount, rather than as a clear error. That is accepted: carrying the value in
each acknowledgement record would make recovery diagnose it, but it is not worth
a durable-format change for a contract a client derives from its own state.

**Session-only, and every field of `JournalConfig`.** The journal name, its
fragment stores, replication, fragment length, refresh interval, and
compression codec are each rejected when unset, because Gazette rejects them
too and failing at `Open` names the field.

Two are `optional` on the wire, because Gazette reads zero as a choice for
them: `max_append_rate` zero is no ceiling, and `flush_interval_seconds` zero
closes a fragment on size alone. Absence is what the daemon rejects, so both
effects stay reachable but only by asking for them. This is what decision 2
gives up Go bindings to buy.

`Broker.endpoint` is session-only and required, like the rest. No flag backs
it: it travels with the credential it pairs with, so a default would be
unreachable wherever brokers authenticate (decision 6).

**Session-only, optional.** `Broker.credential`, whose absence means an
anonymous broker client. There is no configured credential and no signing key:
the daemon mints no tokens and holds no key material (decision 6).

**No resource ceilings.** There is deliberately no maximum device size,
maximum disk count, or local-capacity policy. Capacity is an operational
concern answered by scaling the host, not by daemon logic, and the one hard
ceiling that does exist — `ublks_max` — is already enforced by the kernel;
duplicating it in config would only create two limits to keep in sync. The
daemon reports rather than enforces: see the `ublks_max` handling in Phase 2
and the capacity metrics in Phase 7.

**Config-only, host facts.** Socket path, image directory, mount directory,
admin port, log format. The admin surface is a port rather than an address
because `service-kit` states loopback-only as a rule and it carries no
authentication, so a flag should not be able to express the mistake. A host
with several NVMe drives stripes them beneath the one image directory rather
than naming each one, so no client ever learns host topology. Nothing here
sizes a thread pool: decision 9 gives each disk an owner of its own, so the
count follows the disks rather than the host's CPUs.

**Config-only, policy.** Compaction `k`, `r`, and horizon minimum; the floor
label name, which is required.

**Not configurable, though they could have been.** Queue depth, the `mkfs`
and mount timeouts, the SIGTERM drain timeout, and the capture channel's
capacity are constants. The test a knob has to
pass is what an operator would *observe* that makes them reach for it, and
these have no answer: they are hang guards and sizing choices with no tuning
signal. The drain timeout in particular must sit under systemd's
`TimeoutStopSec` to fire at all, and nothing could validate that pairing. They
can become flags the day a signal exists.

**Fixed in code, deliberately not configurable.** Mount options and journal
`O_RDWR` flags (correctness), the floor label's value encoding, the frame and
chunk format, retention and `path_postfix_template` (hazards), and the
filesystem type — the last behind Phase 4's seam, and if ever exposed it
belongs in `Open` as a durable per-disk fact like `block_size`.

### Validation this implies

`device_size` must be non-zero and a multiple of `block_size`, and its block
count must fit `u32`; `block_size` must be non-zero and a power of two the
device and `mkfs` both accept. Both are checked at `Open`, before any device
exists.

### Open questions

- **Does `ublks_max` bind privileged devices?** Its description reads "max
  number of *unprivileged* ublk devices allowed to add (default: 64)", and the
  daemon creates privileged devices under `CAP_SYS_ADMIN`. If the limit counts
  only unprivileged devices it never binds us; if it counts all of them, a
  default host caps at 64 concurrent disks. Settled empirically by Phase 2's
  first `ADD_DEV`, and it decides whether host images need a
  `/etc/modprobe.d/` entry.
- **Per-disk compaction policy** is an attractive future option: `r` × live
  allocated size self-scales, but the horizon minimum means something quite
  different on a 10 GiB disk than on a 1 TiB one. Config for now; a per-`Open`
  override is purely additive when measurement justifies it.

## Departures from the design doc

Where this plan and the design disagree, this plan is current. The design
should be amended to match before the feature ships.

| Design says | Plan does | Why |
| --- | --- | --- |
| Format and mount output "accrues during the format", i.e. appends as it arrives | Held until the first mutation after mount, then appended ahead of it | Appending during `mkfs` creates a journal for every disk at `Open`, so nothing downstream can make creation lazy. A never-written disk carries no information — it is reproducible by formatting again. |
| `Open` carries a `JournalSpec`, which the daemon applies if absent | `Open` carries typed `JournalConfig` inputs | Lets the daemon own what recoverability depends on (read-write flags, a codec it can decode), and keeps the proto free of Gazette message imports. |
| The runtime writes `estuary.dev/truncated-at` | The daemon writes it, under a configured label name (decision 7) | The daemon derives the floor, so round-tripping it through the client added a second party to one spec field for no gain. |
| `Opened` returns the derived floor; `Committed` returns a floor clock | Neither reports a floor | With the daemon persisting and re-reading the label, no client consumes the value; progress belongs on the metrics surface. |
| Retention is unspecified for the disk journal | Never set: kept indefinitely, truncated from the floor | Age-based fragment deletion cannot see the recovery floor, so a retention shorter than a horizon's completion time would delete records a live disk still needs. |
| "No udev rule or device helper is required" | True only when the daemon runs as root | `CAP_SYS_ADMIN` does not bypass file permissions, so a non-root daemon cannot open the root-owned `0600` `/dev/ublk-control` or the `/dev/ublkc*` nodes it must `pread`/`pwrite`. That posture needs a udev rule granting ownership. |

## Testing strategy

Three tiers, established once and reused by every phase:

| Tier | Needs | Runner |
| --- | --- | --- |
| Unit / property / snapshot | nothing | `cargo nextest run -p disk-daemon` |
| Broker-backed | etcd + `gazette` on `$PATH`/`$GOBIN` | same — `e2e-support::DataPlane` spawns them per test binary |
| Privileged e2e | `ublk_drv`, passwordless `sudo`, broker | same — privileged pieces run as `sudo -n` children (decision 4), serial test group |

Snapshot conventions follow the repo: `insta` with explicit names,
determinism by fixing inputs (`uuid::Producer::from_bytes`,
`uuid::Clock::UNIX_EPOCH`) rather than redaction. Property tests use
`quickcheck` (already a workspace dev-dependency).

The privileged tier is introduced in Phase 2 and grows with each later phase;
it is the tier that ultimately proves the durability guarantee (kill →
recover → identical filesystem).

## Phases

Linear order; each phase depends on the previous.

### Phase 1 — Crate scaffold, protocol, and durable-format core

The durable contract, reviewable before any I/O machinery exists.

**Build:**
- Crate scaffold (workspace member via the `crates/*` glob), README,
  workspace-inherited package metadata.
- Codegen registration per decision 2: the new target in
  `crates/proto-build::resolve_flow_targets`, `pub mod disk` + pbjson package
  in `crates/proto-flow`, the `disk_client`/`disk_server` feature pair in
  `crates/proto-grpc`, the Go `PROTO_FILES` entry — generated code committed
  via `mise run build:rust-protobufs` / `build:go-protobufs`.
- `go/protocols/disk/disk.proto`: `Disk` service (one bidirectional `Session` stream);
  `Request`/`Response` envelopes for `Open/Publish/Commit/Broker` and
  `Opened/Published/Committed`. `Open` carries a `JournalConfig` (the journal
  name plus typed creation inputs — no `JournalSpec`, decision 8), device
  size, block size, a `Broker` (endpoint and credential, shared with the
  refresh request rather than duplicated), and recovered
  acknowledgements (no floor hint — decision 7); `Opened` returns the mount path; `Published`
  returns the serialized-but-unappended acknowledgement (absent on the
  unchanged fast path); `Committed` is empty. Record messages per the design:
  `DiskRecord { uuid, chunks, opens_horizon, installs_epoch }`,
  `Chunk { block: u32, data bytes | punch: u32 }`.
- Chunk codec as pure functions, parameterized by block size: a write
  becomes a trimmed data chunk
  covering `max(1, ceil(len/block_size))` blocks plus empty-`data` chunks for
  residual all-zero blocks (empty `data` = one allocated zero block); discard
  and write-zeroes become punches; replay application writes data, explicitly
  zero-fills from `len(data)` to the end of the covered range, and
  hole-punches punch chunks — applied against a plain `std::fs::File`.
- Block bitmaps (allocated / horizon): plain word arrays — set/clear/test,
  population count, forward-cursor scan, snapshot-copy.

**Tests:** codec edge cases
(zero-tail trim boundaries, empty-data blocks, punch alignment) locked with
insta snapshots of the wire bytes; quickcheck property — a random sequence of
writes/discards/write-zeroes, encoded to chunks and replayed onto a fresh
sparse file, reproduces the directly-mutated file's content *and* its
allocation set (via the bitmap, and `SEEK_HOLE` spot checks).

**Review focus:** the wire format and session API — the hard-to-change
surface everything else builds on.

### Phase 2 — Sparse image and ublk device server

The privileged, novel-systems-code phase — early because it carries the most
risk.

**Build:**
- Image: `O_TMPFILE` + `ftruncate` sparse file in a configured directory;
  block read/write/punch; allocated-bitmap accounting (writes set bits,
  aligned discards clear them). An `ENOSPC` from an image write completes that
  device request with an I/O error rather than failing the session or the
  daemon: ext4's default `errors=remount-ro` then contains it to the one disk,
  the connector sees `EROFS`, and the shard restarts. There is no capacity
  policy beyond this — see the configuration section.
- Bitmaps are the disk's fixed memory cost, `device_size / block_size / 8`
  bytes each: 320 KiB per bitmap for 10 GiB at 4 KiB, 32 MiB for 1 TiB.
  Allocate the horizon bitmap lazily, since it only exists while a horizon is
  active, which halves the steady-state cost.
- ublk control plane over io_uring on `/dev/ublk-control`: ioctl-encoded
  `UBLK_U_CMD_*` only (`ADD_DEV` with kernel-chosen device number, params —
  single queue, the disk's block size as the logical block size, no volatile
  write cache, discard + write-zeroes support — `START_DEV`, `STOP_DEV`,
  `DEL_DEV`).
  `UBLK_F_USER_COPY`: request data moves by `pread`/`pwrite` against
  `/dev/ublkcN`.
- Devices are **privileged** — created without `UBLK_F_UNPRIVILEGED_DEV`.
  Unprivileged devices would buy nothing: ext4 requires a block device and
  lacks `FS_USERNS_MOUNT`, so mounting it needs real `CAP_SYS_ADMIN` whatever
  the device's flavor, and unprivileged devices are the population `ublks_max`
  limits.

  The code must not assume it is root, because production may run it as a
  dedicated UID with ambient `CAP_SYS_ADMIN`: no chown, and no writes outside
  its own directories. Note that posture needs ownership of
  `/dev/ublk-control` and `/dev/ublkc*` granted by a udev rule, since
  `CAP_SYS_ADMIN` does not bypass file permissions (`CAP_DAC_OVERRIDE` does,
  and the design withholds it) — so the design's "No udev rule or device
  helper is required" holds only when the daemon runs as root, as the tests
  do.
- Report the kernel's device ceiling rather than adding one: read
  `/sys/module/ublk_drv/parameters/ublks_max` at startup, log it beside the
  live device count, and turn an `ADD_DEV` refusal at the limit into an error
  naming the parameter and that root can raise it live
  (`echo N > …/ublks_max`) — otherwise it surfaces as an opaque `EBUSY`
  mid-session. Establish here whether the limit counts privileged devices at
  all: its description says *unprivileged*, the kernel default is 64, and this
  dev host has been raised to 1024 by `/etc/modprobe.d/ublk.conf`.
- Owner model: pooled owner threads, each disk owned by exactly one owner
  (decision 9 replaces the pool with one owner thread and one ring per disk);
  the owner reaps its ring and never blocks (all image I/O submitted to the
  ring); a per-disk in-flight range set serializes overlapping mutations
  (expected empty in practice — the guard that makes journal order equal
  application order).
- Mutation-capture seam: every accepted write/discard/write-zeroes hands its
  chunk(s) to an async sink over a bounded channel *before* the image write
  is issued; a full channel parks the request (the design's backpressure).
  Phase 3 plugs the journal in; Phase 2 tests use a collecting sink.
- Promote `io-uring` to a workspace dependency. Raw syscalls follow repo
  style (`libc` behind `cfg(target_os = "linux")`, per
  `crates/shuffle/src/log/writer/sealed.rs`).
- Privileged test harness (decision 4): the `sudo -n` child-process pattern,
  the serial test group, and the `modprobe ublk_drv` prerequisite wired into
  CI's standard test flow.

**Tests (privileged):** device create/serve/teardown with no leaked device
nodes or mounts; `mkfs.ext4` + mount + file I/O + fsync + unmount over the
served device; discards observed as punches and cleared allocated bits; the
captured chunk stream, replayed by Phase 1 rules onto a second image,
mount-compares equal (recursive content diff); backpressure test with a
stalled sink; overlap-guard unit tests (unprivileged) over synthetic request
sequences.

**Review focus:** unsafe/io_uring correctness, teardown paths, and the
admission-order invariant (chunk queued before image write issued).

### Phase 3 — Journal writer: fencing, append pipeline, acknowledgements

Unprivileged; testable against a real broker via `e2e-support`.

**Build:**
- Gazette fixed protobuf framing, in `crates/gazette` beside `read/lines.rs`
  and `read/json_lines.rs` which already frame the JSON-lines content type: a
  magic word, a u32-LE length, and the payload. `encode`/`decode` generic over
  `prost::Message`, with the desync scan-forward of `fixed_framing.go` — off a
  frame boundary a reader scans to the next magic word and reports the skipped
  span against its journal offset, while a well-framed payload that will not
  decode is an error, since a reader cannot skip a record it cannot interpret.
  Tests cover round-trip, a concatenated stream, desync with and without a
  following magic word, and a truncated tail. This phase is where the `gazette`
  dependency arrives and where the first real writer exercises it.
- Journal creation (decision 8): build a `JournalSpec` from `JournalConfig`
  merged over daemon configuration — daemon-owned flags, no retention,
  terminal error if no store resolves or if the codec is one the daemon's
  reader cannot decompress (which excludes SNAPPY until Phase 5 lands the
  decoder) — and `apply` it insert-only, tolerating `EtcdTransactionFailed`.
  Driven by the first append, not by `Open`.
- Fencing per the design: probe (zero-byte append → current `author`
  register `R` and confirmed head `H`); claim by appending `Fence(E)`
  — an `OUTSIDE_TXN` record with a distinct producer carrying
  `installs_epoch` — with `check_registers(R)`, `union {author: E}`,
  `subtract {author: R}`; ambiguous appends retried idempotently by checking
  for `E`, never with a new epoch; `check_registers(E)` on every later
  append; register mismatch is terminal. Claim timing is deferred (record
  `R` at open, claim on first publication) unless committed state exists.
- Append pipeline: bounded channel fed by owners; record-size and
  append-batch bounds; per-delta confirmation tracking; session producer
  stamping `CONTINUE_TXN` records; transient broker errors retried
  internally, terminal errors fail the session.
- Acknowledgement construction: assert no mutation in flight and every data
  append confirmed; build the `ACK_TXN` record, return its exact bytes
  without appending; append only on `Commit`, then await the barrier.
  Enforce the one-uncommitted-ack session invariant.
- `Broker` handling (decision 6): session-fed bearer with
  `tokens::manual()`-style replacement, or anonymous when absent — endpoint and
  credential swapped together, as `Client::new_with_tokens` extracts them from
  one token. Plus `Router::sweep` housekeeping.

**Tests (broker-backed):** first-use claim and fence record shape
(decoded via the new framing); contention — a second writer fences the
first, whose next append fails terminally; ambiguous-append repair
(idempotent re-check finds `E`); deferred-claim behavior (no journal
mutation when nothing publishes); a large synthetic delta carries one
record per mutation covering exactly its blocks; commit → read back → sequenced decode yields exactly
the delta's chunks. Bounds/assertion logic also unit-tested without a
broker.

**Review focus:** the fence state machine and the "acknowledgement returned
but not appended" split.

### Phase 4 — Daemon binary and session service: fresh-disk lifecycle

First full vertical slice (privileged + broker). This is where the
stand-alone binary appears.

**Build:**
- `flow-disk-daemon` binary: clap args (Unix-socket path, image directory,
  mount directory, admin port, log format, owner-pool size, and the
  decision-7 floor-label name), tonic server on the socket
  (`task_service.rs` listener pattern), `service-kit` registry + admin/
  metrics surface (`runtime-sidecar/src/main.rs` shape), structured tracing.
- Session = stream lifecycle: `Open` (fresh path, no committed state) →
  create image → create device → format through the served device → mount at
  a daemon-owned path → `Opened` with the mount path. Epoch `E` chosen and
  `R` read without claiming.
- Format and mount behind a **filesystem strategy seam** — a pair of
  routines (build the `mkfs` invocation; supply mount options for a given
  type) rather than `mkfs.ext4` inlined into the session path. The daemon
  parses no filesystem structures, and nothing about the filesystem appears
  in the journal, replay rules, or bitmaps, so the durable format is already
  type-agnostic; the type touches only these two places plus the assumption
  that frees produce discards.

  ext4 is the only implementation, and no knob is exposed: adding one means
  committing every filesystem to the full crash matrix. It is the right
  default because `assume_storage_prezeroed=1` (e2fsprogs 1.47+) lets `mkfs`
  skip zeroing unused inode tables and the internal journal — those ranges
  stay holes, never enter the allocated bitmap, and never trigger later
  background initialization — which is what keeps the critical-path first
  delta small. Metadata-only journaling keeps rewrite volume (and therefore
  journal volume) down, and it has the deepest hardening history, which
  matters because the host kernel parses metadata a connector influences.

  Fresh disks: `-b <block size>`, zero reserved blocks,
  `assume_storage_prezeroed=1`, `nodiscard`. Mount, fresh and recovered alike:
  `noatime,nodev,nosuid,noexec,discard`.
- Format and mount writes are captured as chunks but **held**, not appended,
  until the first mutation after mount — the lazy-creation trigger of decision
  8. They are then appended ahead of that mutation, so the first delta carries
  all allocated filesystem metadata. Recovered disks hold nothing: their
  journal exists, so mount writes append as they arrive.

  XFS is the alternative to measure if a reason appears — dynamic inode
  allocation and v5 metadata CRCs against a log that `mkfs` does initialize,
  and speculative preallocation that could inflate the allocated set and so
  the horizon copy budget. Btrfs (copy-on-write amplification) and F2FS
  (garbage collection rewriting live data on its own schedule) are poor fits,
  because every filesystem write becomes durable journal volume. Dropping the
  journal (ext2) would cut that volume but requires `fsck` on recovery, which
  repairs *a* consistent filesystem rather than reproducing contents — it
  forfeits the guarantee. Recovery need not record the type durably: `mount`
  auto-detects via libblkid, and the seam then supplies that type's options.
- `Publish`: daemon-issued `syncfs`; close admission (the cut — per-disk, not
  a barrier across an owner's other disks); wait for in-flight mutations and
  append confirmations; construct the ack; resume admission but hold
  mutations until the ack commits. Unchanged fast path returns no ack.
- `Commit`: append the exact ack bytes, await the barrier, reply `Committed`.
- Protocol violations (second `Publish` with one outstanding, early or
  mismatched `Commit`) and device errors are terminal: stream closes, and
  stream close in any order unmounts, stops/deletes the device, and drops
  the image.

**Tests (privileged + broker):** full loop — open, write files through the
mount, publish, commit, decode the journal and replay into an image whose
mount content-compares equal; several sequential transactions; the
no-change fast path appends nothing; a session whose disk is formatted,
mounted, and never written creates no journal at all, while one write to the
mount creates it and publishes the retained format output with that write
(decision 8); a `JournalConfig` resolving to no store is a terminal error; a
session against an unauthenticated broker, naming an endpoint but no
credential, works end to end as an anonymous client; violation matrix returns
terminal errors and tears down cleanly; abrupt client disconnect mid-I/O
tears down without leaks (asserted via `/sys/block` and `/proc/mounts`);
daemon process kill leaves no mounts after restart of the test.

**Review focus:** the cut ordering (`syncfs` → admission close → confirm →
ack) and session/stream teardown.

### Phase 5 — Recovery: replay, acknowledgement repair, committed-state startup

**Build:**
- SNAPPY read support in `crates/gazette` (frame-format decoder via the
  `snap` crate), closing the codec gap for disk journals (and recovery logs
  generally).
- Floor label read (decision 7): at `Open`, read the configured label from
  the journal's spec and resolve its clock to a seek offset. An absent label
  or absent name means "start at the first available fragment".
- Replay reader over a fixed range: after fencing and repair, obtain
  broker-confirmed head `H` — which is what makes the read fresh, since a
  broker serving that append holds an index covering every fragment below it,
  and refuses with `IndexHasGreaterOffset` otherwise; seek to the
  label-derived offset `O`
  (seek position only — never record filtering); one forward pass applying
  `gazette::uuid::sequence` per producer; apply acknowledged deltas in
  physical journal order; validate fence records; a new session producer
  abandons an older producer's pending delta (later ack of an abandoned
  delta is an ordering error); tolerate the range beginning mid-delta;
  explicit zero fills per the codec rules; rebuild the allocated bitmap.
  `opens_horizon` records are rejected in this phase (the writer cannot yet
  produce them).
- **Nothing is buffered to hold an uncommitted delta.** The reader applies
  every delta as it reads it, recording the offset at which each delta's
  first record appeared. The append barrier means an unacknowledged delta is
  always the last one, so if the pass reaches `H` with the final delta
  unacknowledged, the image is discarded and `[O, S)` is replayed into a
  fresh one, where `S` is that delta's first record. Everything below `S` is
  acknowledged by construction.

  This costs nothing when a session shut down cleanly, which is the ordinary
  case, and one extra read of the recovery range when it did not. It is
  possible because the image is disposable, which is the premise the whole
  design rests on, and it removes a spill file, its memory threshold, its
  disk budget, the `fsync` and validation a spill surviving a crash would
  need, and the shutdown deadlock that a spill budget's backpressure invites.
  `crates/shuffle` reaches the same conclusion from the other direction: its
  log records positions and re-reads its source rather than copying bytes
  aside.
- `Open` with committed state (journal has an acknowledged delta, or `Open`
  carries recovered acks): claim the fence *first*, append every recovered
  acknowledgement exactly and await its barrier (the crash-window repair),
  then replay `[O, H)` into a fresh image, create the device, mount (mount
  writes append as ordinary mutations belonging to the next delta), and
  return the mount path. A derived floor ahead of the label advances it
  (decision 7).
- Startup with neither committed state nor recovered acks ignores orphan
  records and only reads `R` (fresh path).
- **Snapshot the image instead of retaining its format output.** Phase 4 holds
  the `mkfs` and `mount` mutations in memory from the mount until the first
  client write, which for an idle disk is the whole session. Measured, that is
  99 KiB on a 128 MiB device and 4.1 MiB on a 10 GiB one — the largest thing a
  disk holds, next to about 12 KiB of ring and 25 KiB of thread stack.

  Replace it: retain nothing, and when the first mutation arrives ask the owner
  to snapshot its image — walk the allocated bitmap, read those blocks, emit
  them as chunks — and append that ahead of the delta. The snapshot is also
  smaller than what is retained today, because a block written twice while
  formatting collapses to one chunk.

  The apparent race is benign, which is why this waits for a phase that can
  test it. A mutation is captured before it is applied, so the owner may have
  already applied the first one when the snapshot is taken. Replay starts from
  an empty image, and every mutation captured since the mount is appended after
  the snapshot, so one already reflected in it is simply applied again: a chunk
  write sets the same bytes, and a punch clears the same blocks. Order among
  mutations is preserved, so a write later undone by a punch still ends undone,
  and no intermediate state is ever observed because a delta becomes durable
  only when its acknowledgement commits.

  It lands here rather than in Phase 4 because the encoder and its test belong
  beside the replay reader above, which reconstructs an image from chunks by
  the same rules.

**Tests (privileged + broker), the crash matrix:** commit → drop stream →
reopen → file contents identical (repeated across several transactions);
ack-lost-after-commit-decision — simulate the caller crash between
`Published` and `Commit`, reopen passing the ack as recovered: repair
appends it and recovery includes the delta; unacknowledged delta without a
recovered ack is discarded; orphaned failed-first-use journal yields a fresh
disk; a stale or absent floor label only increases replay work; replay
determinism — two recoveries of the same journal produce
content-identical filesystems; a snapshot-then-replay reproduces the filesystem a retained-then-replay does, including its allocation, and a disk which is opened and left idle holds no format output; ext4 journal replay on a mid-flush cut
(kill during heavy writeback, recover, mount succeeds, fsck clean).

**Review focus:** replay rules — they *are* the durability guarantee — and
the repair path.

### Phase 6 — Horizons: bounded recovery

**Build:**
- Writer side: horizon-open decision at the delta's *first* record (journal
  range from current floor > `r` × live allocated size, 1 GiB minimum);
  allocated→horizon bitmap snapshot taken before that record's chunks apply,
  mirroring replay; `opens_horizon` flag on the first record; `k`-proportional
  copy budget spent interleaved with ordinary traffic (candidates = horizon
  bits − blocks already published this delta − blocks with a mutation in
  flight; an arriving mutation supersedes an in-progress copy); bits cleared
  only when a delta commits; forward cursor so each horizon is one pass.
- Floor: a committed delta that empties the bitmap completes the horizon;
  the daemon derives the same floor during replay (snapshot at
  `opens_horizon`, clear at-or-after, floor on empty; later horizons replace
  earlier snapshots).
- Floor label write (decision 7): on completion, advance the configured label
  to the opening record's clock with an advance-only CAS against the journal
  spec (`list_watch` for `mod_revision`, `apply` with `expect_mod_revision`,
  lost race retries on the next watch snapshot). The write is off the commit
  path and best-effort — a failed write only costs replay work later — but it
  must never write a value ahead of the derived floor.
- Replace Phase 5's `opens_horizon` rejection with full reconstruction, so
  a replacement session resumes an open horizon rather than restarting it.
- `r = 2`, `k = 0.5`, 1 GiB minimum as the shipped defaults of the
  decision-5 configuration; tests exercise tiny values through the same
  knobs rather than test-only backdoors.

**Tests (privileged + broker, tiny thresholds):** sustained rewrite traffic
opens and completes horizons, and recovery succeeds reading only from the
labelled floor (earlier journal content deleted from the fragment store to
prove independence); write-amplification stays ≤ `1 + k` per delta
(excluding framing) and the retained range stays ~bounded by `(r + …)` ×
allocated size over a long run; an idle disk pauses its open horizon (no
appends while unchanged); kill mid-horizon → replacement session resumes and
completes it; a connector-style full rewrite completes a horizon with near
zero extra copying; floor monotonicity across sessions; the floor label is
advanced on completion, never moves backward, and a session whose label write
lost a CAS race retries and converges.

**Review focus:** the horizon invariant end to end — open decision, budget
accounting, replay reconstruction, floor arithmetic.

### Phase 7 — Stand-alone hardening: CLI, observability, fault and soak testing

Finishes the daemon as a usable, operable stand-alone service.

**Build:**
- A `client` dev subcommand on the binary (or a small companion bin) that
  drives a session interactively: open a disk against a broker (creating the
  spec from flags, authenticating per decision 6), print the mount path, and
  publish/commit on stdin/signal. This is the manual-testing and demo surface for the daemon
  as a general-purpose utility, and doubles as the smoke tool on real
  hardware.
- Observability: per-disk metrics (delta bytes, appended vs. confirmed,
  horizon progress, floor offset/clock, admission stalls, queue depths) via
  the `service-kit` surface, plus host-level capacity: allocated bytes summed
  across disks, free space on the image filesystem, and live device count
  against `ublks_max` — the daemon knows its true footprint exactly from the
  allocated bitmaps, which `st_blocks` cannot give under delayed allocation; session-scoped tracing spans; startup
  environment validation with actionable errors (kernel/`ublk_drv`,
  `/dev/ublk-control` access, hole-punch support probe of the image
  directory, `mkfs.ext4` version, and the decision-7 requirement that a floor
  label be named or explicitly disabled).
- Hardening: graceful drain on SIGTERM (sessions end, devices torn down);
  bounded per-session memory;
  defensive limits on concurrent sessions/disks per owner; consistent
  terminal-error taxonomy over the session stream.
- Docs: crate README (architecture roadmap per repo policy), a short
  operator doc (privileges required, flags, metrics), and a design-doc
  addendum recording anything that shifted during implementation.

  The operator doc must state that **the socket's directory is the access
  control**. A client connecting to a Unix socket needs write permission on
  it, and the daemon's clients are not the privileged user it runs as, so the
  socket is world-writable and its directory decides who may reach it. A
  session costs local resources, so a world-traversable directory is a local
  denial of service. Systemd socket activation with `SocketMode` and
  `SocketGroup` is the better answer, and belongs with the unit file.

**Tests:** soak — many concurrent disks under mixed read/write/discard load
with periodic publish/commit and randomized kill/recover, asserting content
equality and no fd/device/mount leaks over the run, and counting owner threads
and kernel `io-wq` workers against the `TasksMax` decision 9 leaves to the unit
file; fault injection — broker outage mid-delta (backpressure stalls
writes, resumes cleanly), credential expiry and replacement mid-session,
fence takeover by a concurrent session (loser terminates, winner recovers),
daemon SIGTERM under load; `client` subcommand smoke test scripted end to
end.

**Review focus:** operability — whether the daemon can be run, observed,
and trusted stand-alone.

## Deferred: Flow runtime integration

Out of scope for this effort; recorded so the daemon's API stays shaped for
it. The design's runtime half maps onto `crates/runtime-next` as follows:

- Provisioning: a `disk_template` in built specs (`crates/assemble`), whose
  per-tenant parts (stores, replication, labels) the shard passes as
  `JournalConfig` for the daemon to create `disk/{shard_id}` lazily.

  Two questions to settle then, neither foregone. **Does `activate` converge
  disk-journal specs?** It re-upserts every partition and recovery log from
  its template on each activation, but creates ops journals once and never
  updates them (`activate/src/lib.rs:786`). Converging means a template change
  reaches live disks, and means the daemon must tolerate the guards on that
  path — a refusal when the live `estuary.dev/build` label is newer, and
  `O_RDONLY` forced on a cordoned journal. Not converging means the
  creation-time spec is permanent, which argues for more `JournalConfig`
  fields. **Where does fragment tuning come from?** Only collections have a
  `journals.fragments` stanza today; tasks have none, so a disk template would
  start fully hardcoded like `recovery_log_template`. True per-tenant tuning
  has a natural home in `models::StorageDef`, already keyed on
  `catalog_prefix`: an optional `fragments` there would thread through
  `walk_prefix` into both templates, and the storage-mapping mutation already
  broadcasts `Republish` on spec change.
  Plus open-before-connector in shard startup, bind mount
  into the container (`crates/runtime-next/src/container.rs:133`), gated by
  a shard-label flag.
- Commit coordination: the returned ack rides `Persist` as `AI:{journal}`
  (the prefix already exists in `crates/runtime-next/src/shard/recovery.rs`);
  captures in their local FSM, derive/materialize via leader-protocol
  additions. The runtime does *not* apply floor labels for disk journals —
  the daemon owns that label (decision 7), and its own
  `apply_truncated_at_labels` path must stay limited to partition journals.
- Known integration questions, unresolved and parked: a Rust-visible
  recovery-log durability barrier (a synchronous RocksDB `Persist` alone
  doesn't prove journal durability), and K8s mount visibility (production
  reactors share only `/tmp` with a sidecar dockerd; the daemon's mounts
  need a shared, `rshared`-propagated volume).
- Operations: local-stack systemd unit + mise task, CI packaging, and
  production data-plane provisioning of the daemon.

## Out of scope (per the design's "Remaining work")

- Shard splits of tasks with committed disks.
- Local cleanup beyond session teardown: crash-orphan GC, local-capacity
  enforcement, task-deletion cleanup.
- libkrun / virtio-fs sandboxes (no such sandbox exists in-repo; the daemon
  returns a mounted directory and is sandbox-agnostic).
