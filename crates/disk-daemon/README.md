# disk-daemon

`disk-daemon` gives applications a mounted filesystem whose contents commit
atomically with an external transaction. It is intended to let Flow connectors
keep databases, indexes, and other working state in ordinary files, while
recovering those files alongside their checkpoint after a failure or
reassignment.

Each disk is a sparse local image, exposed through Linux `ublk` and mounted as
ext4. The daemon captures block mutations into a Gazette journal and lets a
client decide which transaction commits them. Recording below the filesystem
keeps the daemon independent of the application's storage engine and file
operations; the host kernel provides the filesystem semantics. The local image
is disposable, and recovery can rebuild it on another host.

The crate is Linux-specific but independent of Flow's runtime and connector
protocols. It provides the `flow-disk-daemon` service and the client which drives
it. The caller creates the disk's journal, names it, and coordinates transactions
and failover. Flow integration still needs to place mounts in connector sandboxes,
persist acknowledgements with checkpoints, manage journal and fragment
lifetimes, and define shard splitting.

## Where to look

A caller uses `client.rs` and the contract constants beside it in `lib.rs`; the
rest belongs to the daemon and is private. Everything below is `src/`.

| File | What it owns |
| --- | --- |
| `client.rs` | `Client`, `Standby`, `Disk`, and `Error`: the caller's side of the tenure gRPC, and the two-phase commit it drives. |
| `daemon.rs` | The process: its `Config`, the socket, the drain, and the self-signed broker client every tenure shares. |
| `args.rs` | The command line, which is the daemon's whole configuration. |
| `tenure.rs` | `Service` and `Tenure`: the RPC state machine. |
| `failure.rs` | `Failure`, and the gRPC code each tenure failure ends its stream with. |
| `serving.rs` | `Serving`: one open disk's mount, device, and writer — its bootstrap commit, the cut order of `prepare`, and its teardown. |
| `device/` | `Device`: one `ublk` device's life from `add_dev` to `del_dev`, and the `Owner` thread which serves it. `owner.rs` is that thread, `ring.rs` its `io_uring`, `request.rs` the per-tag path of one request, `admission.rs` the cut and the horizon copies. |
| `ublk/` | The `ublk` ABI: `control.rs` is the host-wide control device, `sys.rs` the generated bindings. |
| `image.rs` | `Image`: the sparse file and the bitmap of what it has allocated. |
| `bitmap.rs` | `Bitmap`: a fixed set of block indices. |
| `chunk.rs` | The durable chunk codec, and applying a chunk to an image. |
| `horizon.rs` | `Horizon` and `Policy`: when a recovery horizon opens, and how it discharges. |
| `capture.rs` | The bounded channel between an accepted mutation and the writer. |
| `wake.rs` | `Waker`: the eventfd which interrupts an owner parked on its ring. |
| `filesystem.rs` | `mkfs`, `Mount`, and `syncfs`. The only file which knows it is ext4. |
| `journal/` | One tenure's journal. `spec.rs` validates it and stores the floor, `fence.rs` claims it, `writer.rs` appends its deltas, `playback.rs` replays it, `replay.rs` holds the rules, `buffer.rs` stores the unacknowledged delta. |

## Architecture

A **tenure** serves one disk for the life of one bidirectional gRPC `Tenure`
stream, first as a standby and then, once promoted, as its sole writer. The
client controls it over a Unix socket; the workload accesses the returned mount
with normal POSIX I/O.

```text
transaction client ── Tenure RPC ──► tenure task
       │                                  │
       │ passes mount to workload         │ opens, prepares, tears down
       ▼                                  ▼
    workload ──► ext4 ──► ublk ──► owner thread + io_uring
                                      │              │
                                 image I/O      captured mutations
                                      │              │
                                      ▼              ▼
                               sparse image     journal writer ──► Gazette
```

The responsibilities follow these ownership boundaries:

- **Tenure** owns the RPC state machine and, until promotion, the playback. Once
  promoted it holds a **serving** disk: the mount, device, and writer, whose cuts
  and teardown that disk coordinates.
- **Device** is the `ublk` device itself, which a tenure creates over a rebuilt
  image and destroys as it tears down.
- **Owner** serves that device's queue and is the only thread that mutates the
  live image, its allocated bitmap, and its open recovery horizon.
- **Writer** takes captured mutations, appends them as records, prepares
  acknowledgements, and advances recovery floors. Broker I/O runs independently
  of the owner.

Before a disk is served, a **playback** owns its image and rebuilds it from the
journal. The same playback supports both an immediate open and a hot standby;
promotion hands the image to the owner.

## Transactions and the tenure protocol

A **delta** is the sequence of block changes since the previous commit, including
any unchanged blocks copied for compaction. `Prepare` makes that delta durable in
Gazette and returns its unappended `ACK_TXN` record. The client stores these opaque
bytes atomically with its own state, then hands them back for the daemon to append.
This is the bridge between the two durable stores.

Replies correspond to requests and arrive in order:

| Request | Reply | Effect |
| --- | --- | --- |
| `Open` | `Opened` | Validate the named journal, which must exist, and replay it without fencing or mounting. Reply once playback reaches the observed head. |
| `Promote` | `Promoted` | Fence, repair recovered acknowledgements, finish replay, and return the mount path. |
| `Prepare` | `Prepared` | Cut a delta and return its acknowledgement; empty bytes mean there is nothing to commit. |
| `Acknowledge` | `Acknowledged` | Append the exact prepared acknowledgement and wait for broker confirmation. |

`Open` must be first and occurs once. It carries the disk's journal name and
device size; that name identifies the disk. `Promote` may follow
immediately, even while replay is still running, and carries any acknowledgements
recovered from the client's committed state.

One request is served at a time, and its replies are sent before the next request
is read. A request queued behind `Promote` is therefore served once the disk is
promoted.

The Rust client shares one connection across disks and supports both immediate
opens and standby promotion. Acknowledgement is asynchronous so the caller can
begin its next transaction while the commit is confirmed. Confirmation is
awaited explicitly or by the next prepare or close, which also reports failures.
Until confirmation, the caller must retain the acknowledgement for recovery.

### Making a coherent cut

The client and workload must finish and flush the application state belonging to
a transaction, then stop logical writes while the daemon prepares it. The daemon
can flush writes already issued to the filesystem; it cannot flush application
buffers or infer which writes belong to a checkpoint.

Preparation establishes the boundary in this order:

1. Call `syncfs` on the mount.
2. Close mutation admission. Every admitted mutation is already in the image.
3. Take every remaining captured mutation, then flush and wait for the broker to
   confirm every record of the delta.
4. Sample compaction progress and build the acknowledgement.
5. Reopen admission and return `Prepared`.

Closing admission is the exact device cut. The owner captures a mutation and then
applies it to the image in one step, so it falls wholly before or after that cut.
The client then commits its state together with the acknowledgement, sends
`Acknowledge`, and releases the workload for its next transaction.

Recovery preserves filesystem contents at the committed boundary. Mounting and
ext4 journal replay can change filesystem bookkeeping, so the recovered block
image need not be byte-for-byte identical. Aligning that boundary with the
application's transaction remains the client's responsibility.

### One pending delta

There is at most one prepared delta awaiting acknowledgement. The writer stops
taking captured mutations until that acknowledgement is appended and confirmed.
Device admission has already reopened, so subsequent writes can fill the bounded
capture channel and then stall. Reads may also wait if parked writes occupy every
queue tag.

This barrier follows from Gazette sequencing. A tenure uses one producer for all
its deltas, and an `ACK_TXN` commits that producer's pending records through its
clock while discarding records above it. The acknowledgement's clock is fixed at
`Prepare`; letting the next delta reach the journal first would lose those records.

The daemon rejects a second `Prepare` while an acknowledgement is owed, or an
`Acknowledge` whose bytes differ from those prepared. Requests may be pipelined,
but the next cut waits for the preceding acknowledgement. `Acknowledge` also
flushes, so the acknowledgement is confirmed before any record of the next delta
can join a batch behind it.

### Failure between the two commits

| Failure point | Recovery |
| --- | --- |
| Before the client commits the acknowledgement | The delta remains unacknowledged and is discarded. |
| After that commit, before the acknowledgement reaches Gazette | The client supplies the saved bytes at promotion, which appends them verbatim. |
| After the append lands, before its confirmation reaches the client | The client supplies the same bytes; replay de-duplicates by UUID. |

Recovered acknowledgements must accompany the next promotion, before that tenure
starts writing new deltas. This also lets one coordinator commit several disks by
storing their acknowledgements in one decision.

## Serving the local disk

A disk has one `ublk` queue and one owner thread with an `io_uring`. Queue depth
lets requests wait at the device while the owner is busy, and lets it take and
return them in batches. The dedicated thread is required because `ublk` binds a
queue to the thread that arms its first fetch; a Tokio task could migrate between
workers. Single ownership also keeps image and bitmap updates free of locks.

Reads come from the image. Each write, discard, or write-zeroes request first
offers its complete mutation to the bounded capture channel. A full channel parks
the request, and every later mutation queues behind it in arrival order, ahead of
any horizon copy. Once the channel accepts the mutation, the owner applies it to the
image at once, so the image takes mutations in exactly the order the journal does,
overlapping or not. A mutation never splits across deltas.

The owner reads and writes the image directly, as blocking calls on its own
thread, rather than through its ring, and it copies each request's data through
the character device the same way. That keeps the request path simple, and it
has a cost: while the host filesystem makes one call wait, every other request of
the disk waits too. The likely waits are a large punch, a read the host page cache
misses, and the host throttling a writer when it holds too much unwritten data. The
device accepts discards of at most 16 MiB, so a large one reaches the owner as
pieces it serves other requests between.

The owner is also the one thread which cleans its disk's dirty pages, so it marks
itself an I/O flusher (`PR_SET_IO_FLUSHER`). Dirty-page throttling then judges its
image writes against the host device alone, rather than against the host-wide
dirty limit. Without that, a disk whose writeback stalls behind a slow journal
fills the host's dirty budget with pages only its owner can clean, and every other
disk's owner is throttled for them. Its memory allocations also never wait on I/O,
which could be I/O to its own disk.

The ring carries what remains: the queue's fetch and commit commands, and the
owner's wake. It is one wait for everything which can wake the owner, and it
carries commits and fetches in batches, which is what small requests are bound by.
None of it needs `io_uring`'s kernel worker threads: the driver holds the fetches,
and the wake is polled. `ublk`'s control commands, which the driver will not issue
without blocking, are the one thing of the daemon's that reaches a worker, and they
are rare.

The writer takes one mutation at a time and hands it to the appender as one
record, returning to its requests in between, so a disk under sustained write
load stays serviceable and the writer never accumulates a batch of its own.

What a live disk holds is therefore the device queue, the capture queue, the one
append in flight, and the batch accumulating behind it. Queue capacity and the
maximum device request size bound the first two; the appender's byte threshold
bounds the last two, approximately. Records are never split, so a batch ends
wherever the record that crossed the threshold ends, and the overshoot is one
record. None of this bounds a delta, whose size is the workload's, nor the
process's resident memory.

A writer whose batch is over the threshold waits for the append in flight, and
takes no mutation while it waits. The capture channel fills behind it and the
device parks: that channel remains the single seam at which a workload writing
faster than its brokers accept is slowed down.

Mutation requests complete once they are applied to the image. This keeps normal
block I/O independent of broker latency until capture backpressure applies.
The device advertises no volatile write cache and implements no flush or FUA
requests: local device completion is not the durability boundary. `Prepare`
establishes durability through Gazette.

An image write the host refuses, in practice for want of space, fails its request
and every later cut of the disk. Its mutation was captured before the image
refused it, so the delta then open holds what the image lacks. The failed cut ends
the tenure before that delta can commit, a replay drops it, and the image goes with
the tenure. A horizon run the owner cannot read out of the image fails later cuts
the same way. A failed cut leaves admission open, because the teardown which
follows unmounts, and an unmount writes.

The transport uses `UBLK_F_USER_COPY`: descriptors are mapped, and request data
moves through the character device with `pread`/`pwrite`. The crate drives the
ABI directly so it can own the queue and completion state machine.

### Sparse images and block encoding

The image is an `O_TMPFILE` with no directory entry. Its logical size is the
requested device size, and unwritten regions are holes.
Discards and write-zeroes punch holes, allowing freed filesystem space to return
to the host. An allocated bitmap tracks this exactly at the device's 4 KiB
granularity; host `st_blocks` accounting can lag writes. There is no dirty bitmap,
because mutations are captured as they arrive.

The block size is fixed at 4 KiB, avoiding another per-disk parameter that
recovery would have to reproduce. Device size is supplied by the caller and must
remain stable across tenures; it is not stored in the journal.

A durable chunk either writes data starting at a block or punches a contiguous
range. Encoding trims trailing zero bytes; replay explicitly zero-fills the
omitted suffix so an older value cannot survive there. An all-zero write remains
allocated; only a punch deallocates. Keeping these distinct preserves allocation
as well as content, which recovery horizons depend on.

### Filesystem choice

ext4 is the only filesystem implementation. Its metadata journaling and
`assume_storage_prezeroed` support keep journal traffic and initial allocation
small: unused inode tables and the filesystem journal can remain holes.
The durable format records blocks and contains no ext4-specific operations.

The mount uses `noatime,nodev,nosuid,noexec,discard`. `noatime` keeps reads from
creating deltas, and `discard` makes frees visible to the sparse image.
A sandbox that re-exports the mount must apply `nodev,nosuid,noexec` itself.

## Journal lifecycle and ownership

Whoever deploys a disk creates its journal and owns that specification, including
replication, fragment stores, and compression. Flow's activation does this for a
Flow task, as it does for a shard recovery log. The daemon creates no journal,
converges none, and deletes none: an `Open` naming a journal that does not exist
is refused as `INVALID_ARGUMENT`, because no retry of it could find one.

The daemon validates the live specification of the journal it is given, before
creating a device. A disk journal must declare the disk content type, permit reads
and writes, and use a codec Gazette defines, every one of which the reader decodes.
Age-based retention and fragment path postfix templates are refused because deletion
must follow the disk's recovery floor. The recovery-floor label is the one field the
daemon writes.

Resolving a journal lists it and then probes it, which resumes one Gazette has
suspended. A recovery needs that resumption; of a disk nobody writes it costs a
journal that idles back to sleep. Playback of an empty journal reports ready and
parks until promotion, which resolves the journal again in case another tenure has
written it. Broker auto-suspension lets idle journals relinquish their replicas.

### Fencing and records

A tenure claims a journal by appending a fence record while atomically replacing
its `author` register with a fresh tenure epoch. Every subsequent append checks
that epoch, so a tenure claims at `Promote` and before it serves anything — whether
or not the journal holds content, because a fresh disk's own format is a delta like
any other. The prior author is read at claim time, since a standby may have
opened before several intervening writers. A tenure claims once; losing its
fence is terminal, with no path to reclaim it. An ambiguous fence append is
resolved by probing for the same epoch.

The epoch is also the producer of the tenure's delta records. Each fence uses a
separate producer and installs the epoch without changing disk content. Disk
records use Gazette's fixed Protobuf framing and transaction flags:
`OUTSIDE_TXN` for fences, `CONTINUE_TXN` for block changes, and `ACK_TXN` to
commit a delta.

Records are appended through the journal appender `runtime-next` publishes
collection documents with, sharing its batching, chunking, retries and routing.
The claim is the writer's own low-level append, because it alone checks the
prior author; every record behind it checks the epoch that claim installed.

Journal offsets come from the broker's append confirmation, because a retry
after a lost reply may duplicate content which already landed. A horizon's
offset is the one thing batching could blur, so its opening record is appended
alone and confirmed.

Retries send identical bytes, and replay removes duplicate UUIDs. Fencing
establishes cooperative writer
ownership; committed records and the client's recovered acknowledgements
establish durable state. A replacement fence alone does not discard the old
writer's pending delta, because promotion may still repair its acknowledgement.
Once another producer's delta records interleave, replay refuses an
acknowledgement of that older delta.

## Recovery and hot standbys

Every tenure starts by replaying into a new sparse image, without a mounted
device or a journal claim. Playback reads from the journal's recovery floor to
the observed head, sends `Opened`, and follows new records. A tenure left in this
state is a hot standby, allowing recovery work to happen before failover.

The playback holds its unacknowledged delta in an anonymous file, applying it
only when its acknowledgement arrives. Chunks, horizon openings, and horizon
progress are held together, so all three reflect committed state.

A delta can grow beyond the disk's logical size as blocks are rewritten between
acknowledgements. Playback reads and applies it incrementally, so replay memory
scales with the largest record rather than the full delta.

This buffering is essential because the checkpoint lives outside the filesystem.
Applying uncommitted writes and relying on ext4 recovery could produce a valid
filesystem ahead of the client's checkpoint. Buffering also avoids rereading the
whole journal range at failover to remove an uncommitted tail. Such a tail is
normal while a primary is writing, so optimistic replay followed by a cleanup
pass would defeat much of a standby's benefit. Both immediate opens and standbys
use the buffered playback path.

On `Promote`, the tenure resolves the journal's current head and author. It fences
immediately, bounding what the old writer can still append even while playback
catches up. It then stops playback, appends recovered acknowledgements, obtains a
broker-confirmed head, and continues replay through that head with the same
sequencing state. Any delta still unacknowledged is discarded before the image is
mounted.

A journal holding nothing but fences, or only orphan records from a failed first
use, produces a fresh filesystem. Supplying a recovered acknowledgement for missing
data instead fails: the acknowledgement proves the broker confirmed a prepared
delta, so a replay that applies nothing cannot be treated as a new disk.

`Opened` is sent once and is not retracted if a standby later falls behind.
If pruning removes fragments a tail still needs, playback discards its image and
restarts from the current floor. Skipping the gap would leave stale blocks whose
discard records were deleted. Repeated gaps eventually fail the tenure.
Fragment pruning should leave an age margin below the floor so standbys have time
to finish backfill.

### The bootstrap commit

The daemon's own setup writes to every disk it serves: `mkfs` on a fresh one, and
the bookkeeping ext4 does at any mount. Those are ordinary mutations of a writer
that is already running, and the daemon commits them itself — the same cut a
client's `Prepare` makes, followed by its acknowledgement — before it answers
`Promoted`. A client's acknowledgements therefore cover only its own writes, a disk
nobody writes owes its client nothing on any tenure, and reopening it recovers the
formatted filesystem rather than formatting a second one over the first. The only
orphan the daemon itself can leave is a crash within the window from `mkfs` or
mount to that acknowledgement, which the next tenure discards as it discards any
uncommitted delta.

## Bounding recovery with horizons

Without compaction, recovery work would grow for the disk's lifetime. A
**recovery floor** is a journal offset from which the entire committed disk can
be rebuilt. A **horizon** is a candidate floor, completed once every allocated
block has a committed copy at or after it.

Opening a horizon snapshots the allocated bitmap into a pending bitmap.
Subsequent mutations clear the blocks they cover; punches remove the need for a
copy. The owner also copies unchanged blocks that the workload does not rewrite.
Normal writes therefore do compaction work, and cold blocks are copied
incrementally into ordinary deltas.

A horizon opens when journal history grows large relative to live allocation;
a minimum range keeps small disks from compacting constantly. Each delta earns
copy budget in proportion to its changed data, and unspent budget does not carry
forward. The opening threshold and copy ratio trade journal write amplification
against the amount of history recovery must read. The resulting recovery range
is a policy target rather than a hard size limit, since transaction sizes also
matter. When writes stop, both copying and journal growth stop.

The durable opening marker is on a delta's first record. Replay snapshots
committed allocation before applying that record, and reconstructs horizon
progress from subsequent committed chunks. A horizon therefore survives tenure
replacement without a separate checkpoint or manifest.

The live owner clears pending bits as chunks are captured, ahead of commit.
That state cannot outlive a failed tenure. `Prepare` samples whether the horizon
is complete at the cut, and only acknowledgement of that delta advances the floor
to the opening record. Sampling at acknowledgement time would incorrectly include
mutations admitted for the next delta.

### Persisting and pruning the floor

The daemon stores the floor as a journal label. It belongs with the journal so
recovery and fragment pruning can find it without the transaction client carrying
compaction state. Flow preserves it when converging journal specifications.

The label advances monotonically after a horizon commits. Updating it is
best-effort: a stale or missing floor costs replay work, and recovery can derive
and store a floor whose writer died before updating the label. A floor above the
journal head is ignored. The floor is used as a seek position, never as a filter
that might remove records from within a delta.

The daemon does not delete fragments. The client or an external pruner removes
fragments wholly below the floor, with the standby margin described above.
Advancing the floor bounds recovery work; deletion is what bounds retained storage.

## Process lifecycle and access

The daemon needs Linux with the required `ublk` support, e2fsprogs that can
leave prezeroed regions untouched, and an image filesystem supporting
`O_TMPFILE` and hole punching. Device and mount operations require
`CAP_SYS_ADMIN`, marking each disk's owner thread an I/O flusher requires
`CAP_SYS_RESOURCE`, and reassignment of a recovered mount root requires
`CAP_CHOWN`.

Clients receive a mounted directory owned by their Unix-socket peer UID/GID and
need no privilege; raw devices and image descriptors remain with the daemon.
Fresh root ownership is set during format, keeping it part of the initial
filesystem rather than a separate mutation. Recovery changes root ownership only
if the peer's UID/GID differs from the replayed one.

The daemon uses its configured brokers and data-plane signing key to mint one
refreshing token, shared by every tenure and scoped to the disk journal content
type. That scope bounds the daemon rather than its clients: it cannot reach a
collection's journal or a shard's recovery log, and a journal of some other
content type is invisible to it. Clients supply neither broker addresses nor
credentials. Socket access consequently authorizes access to disk
journals in that data plane. The socket is mode `0666`; its parent directory
must restrict traversal to authorized clients. The required content type
prevents opening other journal formats, but is not authorization between disk
clients.

EOF, cancellation, and terminal errors converge on tenure teardown. A tenure
which ends while standing by has no device or mount, and its replay stops with
it. The writer abandons appends but keeps draining mutations, because unmount
itself writes and would otherwise deadlock on capture backpressure. The tenure
unmounts before stopping and deleting the device, then drops the anonymous image.
Stopping waits for every request in flight, parked ones included, so the writer
drains through the stop as well. Only then does the kernel abort the queue's
fetches, and that abort is what ends the disk's owner thread.
In-flight broker calls are cancellable so teardown does not wait for an outage to
end. Ending or failing a tenure drops its appender, aborting any background append
which would otherwise keep retrying. Tenure failure leaves other disks running.

`SIGTERM`/`SIGINT` stop new tenures and drain existing ones. Mounts must remain
visible to the process that passes them into a sandbox. Host capacity and
placement are deployment responsibilities; the daemon neither reserves space nor
limits the number of disks it serves.

The daemon's help describes its configuration.

## Navigating and testing

`client.rs` is what a caller drives the daemon with, and the crate root holds what
the two must agree on without a request: the tenure protocol, the block size, the
disk content type, and the recovery-floor label whoever prunes the journal's
fragments reads. The crate exports those, `args`, and `daemon`; everything else is
private, so a dead-code warning here is a genuine one.

`examples/` covers committing and reopening a disk, coordinating a commit across
disks, promoting a hot standby, and exposing a mount to a rootless container.
`examples/demo-services.sh` starts the required local services.

`tests/` is the black-box suite. It exercises the shipped daemon through
`client.rs`, ordinary filesystem I/O, and an independent Gazette client, and it
imports nothing from this crate's implementation. Cases compare recovered files
against the workload's expected tree, avoiding a second replay implementation as
the test oracle. `tests/support/` is its harness: the `Fixture` which runs a data
plane and reads its journals, the `Daemon` which runs the shipped binary, a raw
`Tenure` stream for requests the client cannot express, and the `Tree` a case
holds the disk to.

Unit, property, broker, and device tests live beside the code they cover, in
`src/`, as `mod test`, `mod broker_test`, and `mod device_test`. A module whose
tests outgrew it keeps them in a sibling file of the same path, such as
`chunk.rs` with `chunk/test.rs`, so the test names do not move. `src/test_support/`
holds what those need: a privileged child process for a real device, and a data
plane for a real broker. `e2e-support` holds the journal helpers both trees use.

```console
mise run build:gazette
mise exec -- cargo nextest run -p disk-daemon
```

Privileged tests use `sudo -n` child processes, leaving Cargo unprivileged. Run
through nextest so its test groups serialize tests sharing the host's `ublk`
control device. The suite requires the host prerequisites above.
