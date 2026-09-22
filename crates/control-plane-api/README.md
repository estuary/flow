# control-plane-api

## Sandboxes

Sandboxes are per-user Linux VMs that run shell commands on Fly.io Sprites:
persistent, hardware-isolated VMs. See `src/sprites.rs` for the API client,
`src/server/public/graphql/sandboxes.rs` for the GraphQL operations.

See [Sandbox API design decisions](SANDBOX_DESIGN.md) for the rationale,
tradeoffs, and limits of the current API.

The API runs once the agent has `--sprites-token` / `SPRITES_TOKEN` for a
Fly organization, and reports that sandboxes are not configured until then.

### Records and identity

`internal.sandboxes` records each sandbox: its owner, and the `handle` the
provider knows it by. Callers address a sandbox by `catalogName`, resolved
against the caller, so an unknown, retired, or foreign sandbox is simply not found. The handle is the sprite's name, `sbx-<id>`, so
the provider never sees a user identifier; clients never see the handle, since
the GraphQL `Sandbox` type exposes `catalogName` as its identity. Creation requires
`CreateSandbox` on that catalog name, included in the Admin bundle. The provider
never sees the catalog name, and it is unique among a user's live
sandboxes, so a deleted sandbox frees it. A user holds at most `SANDBOX_LIMIT`
live records, one today. `crate::sandboxes` enforces that when it inserts a
record: the count and the insert run in one transaction under a per-user
advisory lock, so raising the limit is a change to that constant alone.

Each started command has a `metadata.json` file under
`.estuary/exec/<execId>/` containing its id, command, and `requested_at`, beside
its stdout, stderr, and exit status. The wrapper writes metadata before launch;
`list_execs` includes only directories with a process group marker and metadata,
newest first. Failed launches and abandoned probes do not appear in history.
Exec ids are server-generated, using the `Id` scalar's 16 hexadecimal digits.
No exec state is stored in the database; deletion discards metadata and
output together.
Listings require reaching the sandbox, and metadata can be edited by its owner.
Existing exec directories without metadata are omitted from listings.

Sandbox operations take `catalogName`. The sandbox record supplies ownership
and authorization.

### Operations

- `sandboxCreate(catalogName)` validates the catalog name and checks
  `CreateSandbox` on it before inserting a record. It creates the sprite and
  installs flowctl before returning a sandbox that is ready to use,
  in a few seconds. A sprite that does not bootstrap is
  deleted and the error returned, leaving the record for its next command to
  provision. It refuses an invalid catalog name, a name one of the caller's
  live sandboxes already has, and a caller at their limit.
- `sandboxes` lists the caller's live sandboxes, newest first. Each carries its
  `execs`: the commands run in it, with the time each was requested and the
  output paths and observed exit result.
- `sandbox(catalogName)` fetches one of them. It is null for a sandbox that is not the
  caller's and for one since deleted, the same answer either way, so it tells
  nobody that a catalog name names anything.
- `sandboxExec(catalogName, command, stdin)` starts a command and returns its exec event
  once the command is running, then lets go of it: the command runs for as long
  as it takes, and nothing in the control plane follows it. A command that fails
  to start is omitted from history, and the mutation returns the failure. The event
  carries `stdoutPath` and `stderrPath`, which
  `sandboxFileRead(catalogName, path, offset)` reads while the command runs or
  afterwards. Its initial `exitResult` is null: startup does not wait for exit.
  A client polls `sandbox.execs` for `exitResult`, null until completion is
  observed and then the integer exit code (zero means success). Listings read
  metadata and exit results in one sandbox request, and
  nothing is written to the database. A client that wants "run and wait" does
  that polling itself, which lets it choose its own timeout and keep partial
  output. If nobody polls, the command still runs and its results wait in the
  sandbox.
- `sandboxDelete(catalogName)` deletes the sprite and its storage, then retires
  the record (`deleted_at`). The provider call goes first, so a failure leaves
  a retryable record rather than an orphan sprite. A retired record frees a
  slot under the limit; its handle is never reused.
- `sandboxFileRead(catalogName, path, offset, limit)` reads `path` relative to the
  sandbox user's home directory, from a byte offset onwards. Paths must be
  relative and contain no `..` components or trailing slash. `limit`
  bounds a read, and `READ_MAX_BYTES` bounds `limit`, so a client reading a
  longer file continues from the offset it is handed; a read that starts at the
  end of the file returns nothing and holds its offset. A client that wants a
  look at a large file passes a small `limit` rather than paying for the
  ceiling. A path that does not exist returns `exists: false` rather than an
  error, because a client watches for files the sandbox has yet to write.
  It opens no exec event. The result exposes `data` as standard padded
  base64 and `text` as lossy UTF-8, computed only when selected. Offsets and limits
  count raw file bytes for both fields. Reads can split UTF-8 characters, so clients
  needing lossless text across chunks should use `data` and a streaming decoder.

### Lifecycle

A sandbox accepts a command in every state. An idle one suspends within a second
of its last command, keeps its filesystem, and wakes on the next command.

A sprite that is waking, or booting for the first time, answers a command by
saying it is not ready. It leaves that state on its own, so every operation
waits it out rather than failing: `while_waking` in `src/sandboxes.rs` retries
with a short backoff up to `WAKE_BUDGET`, and a sprite still not ready by then
fails the operation. The refusal arrives before the command runs, so a retry
cannot run one twice.

A record whose sprite is missing, because create failed after inserting it, is
provisioned by its next command: the Sprites API reports an absent sprite
distinctly, so `crate::sandboxes` creates one, waits for it to accept commands,
bootstraps it, and retries. Only `start`, which is launching work, goes that
far; a read or a start probe against a missing sprite fails instead,
since a fresh sprite has none of the state they ask about.

For exec HTTP 503 failures, logs include the sprite handle, a fixed provider
reason category, and request duration. Retry logs report attempts, elapsed
time, and recovery or budget exhaustion; a 503 alone does not establish that
the sprite is waking. Persistent failures on file reads and other observed
operations also trigger bounded, best-effort reads of the provider's sprite
state and exec-session count. These diagnostics never log command arguments,
stdin, raw provider bodies, or session contents. An `unclassified` reason means
the response did not match the diagnostic categories, not that waking failed.

### Command input

`sandboxExec` accepts optional UTF-8 `stdin` (at most 1 MiB), followed by EOF.
Omitted or empty input gives immediate EOF; there is no interactive input API.
The Sprites exec POST carries input in its HTTP body, never in command arguments
or URL parameters, and the control plane does not store it in exec records or
log it. Supply credentials through this field rather than embedding them in the
command. Commands can still disclose their own input through output or files.

`EXEC_WRAPPER` stages the complete body in a mode-0600 file and checks its byte
length before launching. It opens and unlinks that file, giving the command the
open descriptor, so input survives detachment without retaining a named input
file. A failed upload cannot start a command with truncated input.

### Reading a file out of the sandbox

`sandboxFileRead` runs `READ_FILE`, which validates that `path` is relative,
has no `..` components or trailing slash, and passes it as a script argument. The script
resolves it against `$HOME` and reads with `dd`, seeking and counting in bytes
(`iflag=skip_bytes,count_bytes`) so one call returns at most the caller's
`limit`, itself held to `READ_MAX_BYTES` in `read_file` so the bound on a
response is this crate's. One process does the whole read, so its exit status
is the read's own and a bounded read of a long file ends by count rather than
by a closed pipe.

The script's exit status is the outcome: 0 for a read, 3 for a path that does
not exist, and 1 with its reason on stderr for anything else, such as a path
that names a directory. A missing path is an outcome rather than a failure
because it is how a client sees a file the sandbox has yet to write, which is
exactly what watching an exec's `exit` file amounts to. A seek past the end of
a file reads nothing and succeeds, so a caller that has everything sees an
empty read at the offset it passed in.

This too creates no exec metadata.

### Command output lives in the sandbox

Every exec runs under a wrapper script (`EXEC_WRAPPER` in `src/sandboxes.rs`)
that runs the command under `bash -lc` with stdout and stderr redirected to
`$HOME/.estuary/exec/<exec id>/`, announces itself with one line on the exec
stream, and writes the exit status to that directory. Output therefore outlives
the request that started the command. The announcement exists because the
Sprites API sends its response only once the command writes something; it is
what `sandboxExec` waits for before returning.

The command runs as a background job under job control (`set -m`), so it leads a
process group of its own that holds it and everything it starts, and the wrapper
records that group id in the directory as the marker that the command started.

- `sandboxExec` generates an exec id, starts the wrapper, waits for its
  announcement, and drops the connection. The Sprites API detaches a command
  whose response is abandoned rather than killing it, so the command runs on,
  and the wrapper records its output and exit status in the directory with
  nobody watching. Metadata is written before launch and is visible in history
  once the process group marker exists. A connection
  lost before the announcement proves nothing either way, so `PROBE_START`
  settles it in the sandbox: the recorded group means the command started;
  otherwise the probe creates the exec directory, which the wrapper refuses to
  find already present, so the command has not started and now cannot. Exactly
  one of them creates the directory, so the control plane can settle a lost
  launch without replaying it. If the probe also fails, the outcome remains
  uncertain. Each `sandboxExec` call generates a new execution id. Client retry
  semantics are deferred; repeating a mutation can run the command again.
- A client reads output with `sandboxFileRead`, at the `stdoutPath` and
  `stderrPath` its exec event carries, and polls `sandbox.execs` for `exitResult`.
  `ExecFile` in `src/sandboxes.rs` builds those paths, and `EXEC_WRAPPER`
  spells the same directory in shell. The wrapper atomically publishes the exit
  status after the command's last output, so a client that sees the status reads output through EOF from
  its current offset to be sure it has everything. The API states that rule,
  and each read stays a plain read of a file.
### Output framing

An exec response frames each of the sprite's writes as a one-byte stream tag
followed by that stream's payload, with no length, so a frame runs until the next
one begins. The endpoint speaks HTTP/2, whose DATA frames may split one write
across several deliveries. `Decoder` in `src/sprites.rs` threads state through
the body, so an untagged delivery continues whichever stream is open.

Text output decodes exactly, because the tags are control bytes. Binary output
split immediately before a 0x01, 0x02, or 0x03 byte joins the stream that was
already open.

Tests that talk to the live API are ignored by default:

```bash
cargo test -p control-plane-api sprites::live -- --ignored --nocapture
```

## Development

> **NOTE:** All commands below should be run from inside the Lima VM.

### Applying Changes

Restart the agent API service to pick up changes:

```bash
systemctl --user restart flow-control-agent@flow.service
```

Replace `flow` with the stack name printed by `mise run local:stack-info`.

### Updating the GraphQL Schema

The auto-generated GraphQL schema is checked into the repo. After making changes to the GraphQL API, regenerate it with:

```bash
cargo build -p flow-client --features generate
```

### Updating sqlx query cache

After adding / modifying SQL queries, regenerate the checked-in sqlx query cache so that offline compilation works:

```bash
cargo sqlx prepare --workspace
```

### Formatting

```bash
cargo fmt -p control-plane-api
```

### Running tests

Run tests with a single thread to avoid concurrent database migration conflicts:

```bash
cargo test -p control-plane-api -- --test-threads=1
```

Tests use `insta` for snapshot testing. To automatically accept updated snapshots (you'll need to run this if you've changed the output of any of the gql operations):

```bash
INSTA_UPDATE=always cargo test -p control-plane-api -- --test-threads=1
```
