# sandbox-exec

Runs one shell command inside a sandbox (a Linux VM or container where AI
agents run commands), relaying its stdio as frames over the wrapper's own
input and output streams. The control plane starts the wrapper as
`flowctl raw sandbox-exec` for each command, as the sandbox's ordinary user,
and speaks this protocol over the wrapper's stdin and stdout.

The crate holds both ends of the protocol:

- `Input` and `Output` (`src/codec.rs`) are the frames of each direction,
  with `encode` and incremental `decode` over a `bytes::BytesMut`.
  Clients encode `Input` and decode `Output`.
- `run` (`src/wrapper.rs`) is the wrapper. It's generic over
  `tokio::io::AsyncRead` / `AsyncWrite`, and takes a cancellation future in
  place of installing signal handlers. `crates/flowctl/src/raw/sandbox_exec.rs`
  passes it real stdio and a SIGTERM / SIGHUP / SIGINT future.

## Protocol

A frame is a 1-byte tag, a u32 big-endian payload length, and the payload.
Payloads are at most `MAX_PAYLOAD` (1 MiB); decoders reject a longer length.

| Client to wrapper | Tag | Payload |
| --- | --- | --- |
| `Header` | 1 | JSON `{version, workspace, command, env?}`. Must be first. |
| `Stdin` | 2 | Bytes for the command's stdin. |
| `StdinEof` | 3 | Empty. Closes the command's stdin. |

| Wrapper to client | Tag | Payload |
| --- | --- | --- |
| `Stdout` | 1 | Bytes. |
| `Stderr` | 2 | Bytes. |
| `Exit` | 3 | JSON `{code, signal}`, each an integer or null. Last frame of a run. |
| `Error` | 4 | JSON `{message}`. The command could not be run. Last frame. |

The contract follows `connector.Connector` (`crates/connector/README.md`,
"Protocol contract"). A raw EOF of the input stream is the only teardown
signal: the client hung up. It is not stdin EOF. Writes of output frames are
best effort. The wrapper doesn't detect a dead client by itself: the
control plane's providers close its input, or SIGTERM it, when the client dies.

## Lifecycle

1. The Header is validated. Its `workspace` must match
   `^[a-z0-9][a-z0-9_-]{0,62}$` (`is_valid_workspace`, which the control plane
   also checks), and names a directory under the workspace root. If it's
   missing, it's created with a copy of the skeleton directory's contents (see
   below). Failures send `Error`.
2. The command runs as `bash -lc <command>` in its own process group, with the
   workspace as cwd and `HOME`, and the wrapper's environment plus `env`.
3. `Stdin` payloads queue to a writer of the command's stdin. When 8 MiB is
   queued, the wrapper stops reading input until there's room. If the command
   closes its stdin, further `Stdin` is discarded.
4. When the command exits, its process group is SIGKILLed to reap background
   processes. Remaining output is drained for at most the drain timeout,
   because a process detached with `setsid` escapes the group kill and may
   hold the pipes open. Then `Exit` is sent.
5. Any of these cancels the command: raw input EOF, an input protocol error,
   or the cancellation future. The group is SIGTERMed, given the kill grace to
   exit, then SIGKILLed. `run` returns an error, and no `Exit` is sent.
   A failed output write doesn't cancel the command: further output is read
   and discarded.

`Header.env` may carry secrets. `Header`'s Debug shows only its keys, and its
values reach the command only through its environment, never an argv.

Processes run through `async_process`, because the workspace can't enable
tokio's `process` feature.

While the stdin queue is full, input isn't read, so a hang-up isn't observed
until the command reads its stdin or exits. A provider's own termination
bounds that: Sprites SIGTERMs the wrapper `MAX_RUN_AFTER_DISCONNECT` after its
client disconnects.

## Workspace skeleton

A new workspace starts as a copy of the skeleton directory
(`Config::workspace_skeleton`, `--workspace-skeleton`, default
`/opt/estuary/skel`), as `useradd -m` does with `/etc/skel`. A missing
skeleton means an empty workspace. An existing workspace is never modified.

`create_workspace` copies directories, regular files, and symlinks (as
symlinks). Copies keep their source's permission bits and are owned by the
wrapper's user, so they're writable where their mode says, even when the
skeleton is a read-only mount. A directory's mode is applied after its contents
are copied.

Commands may start in the same new workspace at once, and none observes a
partial copy. Each wrapper builds its copy in a hidden temporary sibling, then
renames it into place with `RENAME_NOREPLACE`. A wrapper whose rename finds the
workspace already present removes its copy and uses that workspace. A failed
copy is removed too.
