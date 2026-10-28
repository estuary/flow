# Glossary

Terms of `disk-daemon`, its client, and its journal records. The README shows
how they fit together. Where a term has a common stand-in, the stand-in is
listed to _avoid_.

## Disks

- **Disk** — a filesystem whose durable state is one journal. Its journal's
  name identifies it. A disk outlives every attachment which serves it.
- **Disk journal** — the Gazette journal of one disk's records, with the disk
  content type (`application/x-journal-backed-disk`). The caller creates it
  and owns its spec.
- **Block** — the 4096-byte unit of a disk (`BLOCK_SIZE`). Chunks, punches,
  and ext4 all work in whole blocks.
- **Device size** — a disk's capacity in bytes, which the client gives at every
  `Open`. The journal does not record it.
- **Image** — one attachment's local copy of a disk's blocks: a sparse,
  unnamed file. It is disposable. The journal, not the image, is durable.
- **Device** — the ublk block device which serves an image. It exists only
  while an attachment is promoted.
- **Mount** — the ext4 filesystem on a device, at the path `Promoted` returns.
  The client's Unix-socket peer owns it.
- **Fresh disk** — a disk whose replay applied no committed delta. Promotion
  formats it.
- **Recovered disk** — a disk whose replay applied a committed delta.
  Promotion mounts it without formatting.

## Attachments

- **Attachment** — one `Attach` stream, and the disk it serves for that
  stream's life. It begins as a standby, and may be promoted to primary.
- **Client** — the program which drives an attachment over the daemon's Unix
  socket (`client::Client`). It commits each delta with its own transaction.
- **Workload** — the program which uses a mount through ordinary file I/O. It
  may be the client, or a sandbox the client passes the mount to.
- **Transaction** — the client's atomic update of its own durable state, such
  as a checkpoint. It stores the ACK intent of the delta it commits.
- **Standby** — an attachment which replays its journal without fencing it.
  It has no device or mount, and appends no records.
- **Ready** — a standby which has replayed through the head its `Open`
  observed. The daemon then replies `Opened`. It stays ready if it later
  falls behind. _Avoid_: warm.
- **Tail** — a standby's reading of journal content appended after it opened.
  It continues until promotion.
- **Hot standby** — a standby which tails while another attachment writes the
  disk, so that its promotion has little left to replay.
- **Promotion** — a standby's transition to primary. It fences the journal,
  re-acknowledges any recovered ACK intent, and replays through the
  fenced head. Then it starts the device and mounts it.
- **Primary** — the promoted attachment whose fence installed the journal's
  current author. Only it appends deltas. _Avoid_: writer, serving
  attachment.
- **Setup delta** — the delta of a promotion's own writes: the format of a
  fresh disk, and the mount. The primary commits it before it replies
  `Promoted`. _Avoid_: bootstrap commit.
- **Close** — the client's orderly end of an attachment (`Disk::close`). It
  waits for teardown.
- **Failure** — an end of an attachment with a status. A refused request, or a
  failed journal or device operation, causes one.
- **Cancellation** — an end of an attachment by a drain, or by a client which
  drops its stream. It gives up the attachment's broker calls.
- **Teardown** — the release of an attachment's disk: unmount, then stop and
  delete the device. Every end of an attachment tears its disk down before
  its stream closes.
- **Drain** — the daemon's shutdown on `SIGTERM` or `SIGINT`. It cancels every
  attachment and waits for each teardown.

## Deltas and commits

- **Delta** — the block changes a primary records after its previous delta,
  including horizon copies. It is the unit of commit.
- **Empty delta** — a delta with no changes. Its ACK intent is empty, and
  acknowledging it appends nothing.
- **Prepare** — the close of the primary's current delta, after a `syncfs` of
  its filesystem. The delta's records are then durable but uncommitted.
  _Avoid_: cut.
- **Prepared delta** — a delta whose prepare finished and whose
  acknowledgement has not landed. A primary has at most one.
- **ACK intent** — the opaque bytes of a delta's unappended acknowledgement,
  which `Prepared` returns. The client stores it in its transaction.
- **Acknowledgement** — a delta's appended commit record: an `ACK_TXN` record
  without a chunk.
- **Acknowledge** — the primary's append of a prepared delta's ACK intent.
- **Commit** — a delta's acknowledgement landing in the journal, after the
  delta's records. Replay applies only committed deltas.
- **Uncommitted delta** — a delta whose records are in the journal without its
  acknowledgement. Replay holds it until a commit or a rollback. _Avoid_:
  pending delta, orphan.
- **Recovered ACK intent** — the last ACK intent a client stored, which it
  hands back at `Promote`. _Avoid_: saved acknowledgement, in-doubt ack.
- **Re-acknowledge** — promotion's append of a recovered ACK intent, in its
  fence's own append. It commits that delta if it never committed.
- **Rollback** — the discard of an uncommitted delta which can never commit.
  A record of a later primary, or a re-acknowledgement of an earlier commit,
  shows it.

## Journal records

- **Record** — a `DiskRecord`, the only record type of a disk journal. Its
  UUID's flags make it a fence, a delta record, or an acknowledgement.
- **Fence** — a primary's first record: an `OUTSIDE_TXN` record at clock zero,
  without a chunk. Its append installs the primary's author. It lands only
  if the author, or with none the head, is still what an empty append read
  just before it.
- **Author** — the producer which the journal's `author` register names. Each
  of a primary's appends checks it, so a fence excludes every earlier primary.
- **Delta record** — a `CONTINUE_TXN` record, which carries one chunk.
  _Avoid_: data record.
- **Head** — the journal's write offset, as a broker confirmed it. `Open`
  observes one. Promotion observes another just before its fence, and the
  fence confirms a third.

## Chunks

- **Chunk** — a change to a range of contiguous blocks: a write, which
  allocates the range, or a punch.
- **Punch** — a chunk which deallocates its range. Unallocated blocks read as
  zeroes.
- **Allocated block** — a block whose last chunk was a write.
- **Live chunk** — a chunk the device applied while serving, which the primary
  appends. It marks whether it is a floor candidate, and any floor it names.
- **Copy** — a write chunk of a horizon's pending blocks, read unchanged from
  the image. Copies complete a horizon whose blocks nothing rewrites.

## Recovery

- **Replay** — the reading of a disk's journal from its floor into committed
  deltas, in journal order.
- **Rebuild** — a standby's application of replayed chunks to its image. It
  ends when the device starts.
- **Recovery** — the return of a disk to its last committed state from its
  journal. It preserves file contents, but not every byte of the image.
- **Spill** — replay's move of an uncommitted delta from memory to an unnamed
  file, past 1 MiB.
- **Gap** — content a read needs which the fragment store no longer holds. A
  read which has left its floor fails on one.

## Compaction and floors

- **Recovery floor** — the journal offset from which replay rebuilds the whole
  disk. Replay starts there.
- **Replay-from label** — the journal spec label which stores a disk's
  floor, as a 16-digit hex offset. The daemon only advances it. A journal
  without one has a floor of zero.
- **Recovery horizon** — an offset which becomes the floor once committed
  chunks at or after it cover every block allocated before it. A disk has at
  most one open.
- **Opener** — the chunk at which a horizon opens. Its record carries
  `opens_horizon`.
- **Pending blocks** — the blocks allocated when a horizon opened which no
  chunk has touched since. A horizon completes when none remain.
- **Floor candidate** — a served opener's record, whose offset the primary
  confirms so that it can become the floor.
- **Naming a floor** — the chunk which completes a horizon names its opener's
  record as the floor. Its delta installs that floor once it commits.
- **Installing a floor** — a commit which makes a named floor the disk's
  floor.
- **Storing a floor** — the primary's update of the replay-from label after
  a commit installs a floor.
- **Replayed floor** — the opener offset of the last horizon a rebuild
  completed. The first chunk the device serves names it again, in case its
  primary never stored it.
- **Bytes since the floor** — the encoded size of the chunks applied after the
  floor.
- **Compaction** — the opening and completion of horizons, which advances the
  floor. It trades write amplification for less replay.
- **Horizon policy** — when a horizon opens, and how fast its copies complete
  it (`device::Policy`).
  - **Open ratio** — bytes since the floor, as a multiple of allocated bytes,
    past which a horizon opens.
  - **Minimum bytes** — bytes since the floor below which no horizon opens,
    whatever the ratio.
  - **Copy ratio** — the bytes a horizon may copy per byte written while it
    is open.
- **Copy budget** — the copy bytes an open horizon has earned and not spent.
- **Pruner** — an external process which deletes a journal's fragments wholly
  below its floor. The daemon deletes no fragments.

## Daemon internals

- **Owner thread** — the thread which serves a started device's ublk queue
  from its image. It sends each chunk it applies as a live chunk.
- **Rebuilder thread** — the thread which replays a standby's journal content
  into its image.
- **Step** — work which a promoted attachment waits on the device for: its
  start, mount, or a sync. The attachment's `Phase` holds it.
