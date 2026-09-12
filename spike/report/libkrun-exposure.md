# libkrun 1.19.4: what the guest can reach in the helper

Source read for experiment 12. Nothing was run; every claim below is a claim
about code, and every claim cites the file and lines it came from.

Read at libkrun tag `v1.19.4`, commit `728df8125077d0db44265f6e997c72b81b65c015`
(`git clone --depth 1 --branch v1.19.4 https://github.com/containers/libkrun`).
Repo commit at the time of the read: `7cad9e9e866`. Paths below are relative to
the libkrun checkout.

Configuration assumed, from CONTRACTS: `krun_add_vsock(ctx, 0)`, exactly one
`krun_add_vsock_port2(ctx, 49092, "/sock/init.sock", listen=true)`, no
`krun_set_port_map`, a read-write root share, read-only `venv` and (optionally)
`deps` shares, one tap, one virtio-blk scratch disk, balloon and console on by
default.

## Three tiers of guest privilege

Every finding here belongs to exactly one of three tiers, and the tier matters
more than the finding. State the tier whenever you quote a line of this report.

- **T1 - unprivileged guest userspace.** The dropped uid the connector runs as.
  Speaks to the host only through syscalls the guest kernel translates: file
  operations on the three virtiofs mounts, socket calls, reads and writes on
  the console. Cannot choose FUSE opcodes, cannot choose virtqueue descriptors.
- **T2 - guest root.** `--run-as-root`, `--as-root-exec`, or a local privilege
  escalation inside the guest. Adds: `ioctl()` on any fd, `mount`, `sysctl`,
  raw sockets. Still goes through the guest kernel's VFS and virtio drivers.
- **T3 - guest kernel control.** A kernel bug, a loadable module, `/dev/mem`,
  or binding a virtio device to a userspace driver. Can compose arbitrary FUSE
  requests and arbitrary virtqueue descriptors. This is the tier at which the
  interesting bugs below become reachable.

libkrun says so itself, in `README.md:88`: "the libkrun security model is
primarily defined by the consideration that both the guest and the VMM pertain
to the same security context [...] To prevent the guest from accessing host's
resources, you need to use the host's OS security features to run the VMM
inside an isolated context." The helper container **is** that isolated context,
and it is the only one. Everything in this document is about what the container
has to hold.

## Table

| Surface | Guest-controllable input | Host effect | Gated by | Verdict |
|---|---|---|---|---|
| vsock TSI control ports 1024-1031 | dgram to CID 2, port 1024-1031 | none | `tsi_flags.tsi_enabled()` false with `krun_add_vsock(ctx,0)` (`muxer.rs:514-534`) | **Closed.** Falls to the `_` arm, no proxy exists, dropped |
| vsock TSI proxy-create, family field | `family` = AF_UNIX/AF_INET/AF_INET6 | would `socket()`+`connect()` a host socket | second, independent gate on `HIJACK_UNIX`/`HIJACK_INET` (`muxer.rs:286-297`, `321-332`) | **Closed twice.** Unreachable even if the first gate were wrong |
| vsock stream connect to port 49092 | OP_REQUEST to the mapped port | RST back to the guest; no host socket touched | `listen == true` branch (`muxer.rs:552-559`) | **Closed.** Guest sees ECONNRESET |
| vsock stream connect to any other port | OP_REQUEST to an unmapped port | **nothing at all** - no RST, no response | falls out of `process_op_request` (`muxer.rs:548-583`) | **Closed, but silent.** See "unmapped ports hang" below |
| vsock listening socket path | none - path comes from `unix_ipc_port_map` | bind+listen on `/sock/init.sock` at activate | host config only (`muxer_thread.rs:152-173`) | **Not guest-controllable** |
| vsock dst CID | header `dst_cid` | none | `!= VSOCK_HOST_CID (2)` dropped (`muxer.rs:509-512`, `697-700`) | **Closed** |
| virtiofs exit-code ioctl `0x7602` | `arg` (i32), on **any** inode of **any** share incl. read-only | sets the helper's process exit code | **nothing** (`augment_fs.rs:719-744`) | **Open at T2.** No new authority; see below |
| virtiofs other ioctls | `cmd` | EOPNOTSUPP | `_ =>` arm (`linux/passthrough.rs:2192`); EXPORT_FD needs an `export_table` we never set (`:2168-2174`) | **Closed** |
| virtiofs name arguments (lookup, create, mkdir, rename, link, ...) | `name` bytes, NUL-checked only | `openat(parent_fd, name, ...)` - resolves `..` and `/` | **nothing in libkrun** (`server.rs:1493-1497`, `linux/passthrough.rs:951-983`) | **Open at T3.** Confined by the guest VFS at T1/T2 and by the helper container at T3 |
| virtiofs writes on the read-only shares | any mutating opcode | EROFS | `read_only.rs:325-471` explicitly rejects 14 opcodes; unlisted ones hit ENOSYS trait defaults | **Closed** |
| virtiofs `ctx.uid`/`ctx.gid` | FUSE header uid/gid | server does the op as that uid, or as **its own root** when uid is 0 | `set_creds` (`linux/passthrough.rs:791-820`) | **Open at T3.** At T1/T2 the guest kernel fills these honestly |
| virtiofs `setxattr` on the root share | xattr name and value | `fsetxattr`/`lsetxattr` on podman's layer | `cfg.xattr` defaults true (`linux/passthrough.rs:385`); RO shares reject it | **Open at T2**, bounded to the share |
| virtiofs `mknod` on the root share | `mode`, `rdev` | `mknodat` - device nodes in podman's layer | `set_creds` only (`linux/passthrough.rs:1498-1530`) | **Open at T2**, bounded to the share |
| virtiofs DAX `setupmapping`/`removemapping` | `moffset`, `len` | `mmap(MAP_FIXED)` in the helper's address space | `(moffset + len) > shm_size`, **unchecked u64 add** (`linux/passthrough.rs:2081`, `:2118`) | **Open at T3 with `--venv-dax`.** Integer overflow, see below |
| virtio-net tap | ethernet frames | bytes moved to `/dev/net/tun` | nothing parses them (`net/tap.rs:91-128`) | **Closed.** Frame content is the egress ruleset's problem, not libkrun's |
| virtio-net control queue | - | - | there is no control queue: `NUM_QUEUES == 2` (`net/device.rs:179-180`) | **Closed.** No MAC change, no promisc, no VLAN filter |
| virtio-console control queue | `cmd.id` (u32) | `self.ports[cmd.id as usize]` | **nothing** (`console/device.rs:212`, `:224`, `:249-268`) | **Open at T3.** Guest-triggerable panic, DoS only |
| virtio-balloon free-page reporting | descriptor `addr` and `len` | `madvise(host_addr, len, MADV_DONTNEED)` | `addr` validated by `get_host_address`; **`len` is not** (`balloon/device.rs:88-107`, `queue.rs:264-266`) | **Open at T3.** Can run off the end of guest RAM |
| virtio-balloon other queues | inflate, deflate, stats, page-hint | none - events read and discarded | `balloon/event_handler.rs:23-79` | **Closed** |
| virtio-blk deps disk | write requests | rejected by the host fd | file opened without `.write()` when `read_only` (`block/device.rs:241-249`) | **Closed, if the shim passes `read_only=true`** |

## vsock

### `krun_add_vsock(ctx, 0)` does what CONTRACTS says it does

`krun_add_vsock` parses the feature word into `TsiFlags` and stores
`VsockConfig::Explicit { tsi_flags }` (`src/libkrun/src/lib.rs:2644-2667`).
`TsiFlags::from_bits(0)` is the empty set, and `tsi_enabled()` is
`!self.is_empty()` (`src/devices/src/virtio/vsock/mod.rs:37-50`), so it is
false. Three things follow:

1. The `Explicit` arm builds the device with `unix_ipc_port_map` intact and
   `host_port_map: ctx_cfg.tsi_port_map` (`lib.rs:2938-2949`). We never call
   `krun_set_port_map`, so that is `None`. **The port map still works; TSI does
   not.** This is exactly the combination the design wants.
2. `tsi_hijack` is never added to the kernel command line
   (`src/vmm/src/builder.rs:1049-1058`), so the guest kernel's AF_INET
   hijacking is off at the source as well.
3. In `send_dgram_pkt` every TSI control port carries an `if
   self.tsi_flags.tsi_enabled()` guard (`muxer.rs:514-534`). With the flags
   empty, a datagram to port 1024 does not match any arm and falls through to
   `_`, where it is either handed to `process_dgram_rw` (which finds no proxy
   for the id and logs "DGRAM ignoring OP_RW", `muxer.rs:488-500`) or logged as
   "unexpected dgram pkt". **No socket is created, no address is parsed, no
   host resource is named.**

There is a second, independent gate inside `process_proxy_create`: even with
`tsi_enabled()` true, an AF_UNIX request needs `HIJACK_UNIX` and an
AF_INET/AF_INET6 request needs `HIJACK_INET` (`muxer.rs:286-297` for
SOCK_STREAM, `:321-332` for SOCK_DGRAM). Two gates, both driven by the same
zero we pass.

Worth knowing what the gates are holding back. `TsiStreamProxy::new` creates a
socket of the guest's chosen family (`tsi_stream.rs:77-83`), and
`get_unixsock_path` (`tsi_stream.rs:433-445`) pulls a filesystem path straight
out of a guest-supplied `sockaddr_un` and stats it. With TSI on, the guest
names host sockets and host addresses directly. With TSI off, none of that code
is reachable.

One latent bug in that dead code, recorded because it argues for keeping TSI
off rather than relying on the family gate alone: `read_proxy_create` checks
`buf_size >= 6` but then reads `buf[6..8]` for the type field
(`packet.rs:627-641`). `buf()` returns a slice of exactly `buf_size` bytes
(`packet.rs:404-414`), so a 6- or 7-byte proxy-create datagram indexes out of
range and panics the VMM. Unreachable at flags=0.

### The mapped port is inbound-only

`krun_add_vsock_port2(..., listen=true)` records `(path, listen)` in
`unix_ipc_port_map` (`lib.rs:1511-1547`), and refuses at configuration time if
the path already exists (`lib.rs:1527-1533` - this is the EEXIST that CONTRACTS
warns about for stale sockets). At activate, `create_lisening_ipc_sockets`
socket/bind/listens on it (`muxer_thread.rs:152-173`); if the bind fails it
logs a warning and **continues**, so a stale socket means a helper with no
listener rather than a helper that fails.

The path is never guest-influenced. The guest chooses only `pkt.dst_port()`,
which selects a map entry. For our one entry, `listen` is true, so a guest
OP_REQUEST to 49092 gets an immediate RST and no host socket is touched
(`muxer.rs:552-559`). Had the entry been `listen=false`, the same OP_REQUEST
would have made the muxer `connect()` to the host path on the guest's behalf
(`muxer.rs:563-581`) - which is why "listen=true" is a security property of
this design and not just a convenience.

### Unmapped ports hang, they do not refuse

This is the one place where experiment 12's wording and the code disagree, and
WP10 needs to know before it writes a probe.

`process_op_request` looks the id up in `proxy_map`, then in
`unix_ipc_port_map`. If the destination port is in neither, the function
returns having done nothing: no RST, no response, no log
(`muxer.rs:539-584`). The guest's `connect()` therefore gets **silence**, not
ECONNREFUSED and not ECONNRESET. A blocking AF_VSOCK connect will sit there;
`sk_sndtimeo` defaults to no timeout.

So "connecting to any vsock port other than the one we mapped is refused" is
true in effect but false in mechanism, and the two mapped/unmapped cases look
completely different from inside the guest:

- port 49092: RST, so `ECONNRESET`, promptly.
- any other port: nothing, so the probe must impose its own timeout.

Unrelated data packets are handled better: an OP_RW for an id with no proxy
does get a Reset (`muxer.rs:633-668`).

### Everything else in the muxer

- `dst_cid != 2` is dropped on both the dgram and stream paths
  (`muxer.rs:509-512`, `:697-700`). The guest cannot address anything but the
  host CID.
- `timesync.rs` is `#[cfg(target_os = "macos")]` (`muxer.rs:14-15`,
  `mod.rs:17-18`). It does not exist in our build.
- `reaper.rs` only removes proxies from a map after five seconds
  (`reaper.rs:27-57`). No guest input reaches it beyond the ids the muxer
  hands it.

## virtiofs

### The exit-code ioctl is not scoped to anything

`AugmentFs::ioctl` intercepts `cmd == 0x7602` and stores `arg` as the VM's exit
code (`augment_fs.rs:719-744`). There is no check on the inode, no check on the
handle, no check on the calling uid, and no check that the fd belongs to the
injected `/init.krun`. Any fd on the share will do.

And it applies to **all three shares**, read-only ones included:
`fs/worker.rs:107-128` wraps every server in `AugmentFs`, so the read-only
stack is `AugmentFs<PassthroughFsRo>` with `AugmentFs` on the outside.
`PassthroughFsRo::ioctl` is one of the few methods the read-only wrapper
delegates rather than rejecting (`read_only.rs:306-321`), which is correct -
the inner passthrough answers everything but EXPORT_FD with EOPNOTSUPP - but it
means `0x7602` is reachable through `/venv` and `/opt/venv` too.

**This grants the guest no authority it did not already have.** The exit code
the helper reports *is* the workload's exit code by design, and the workload is
the guest's own code; a connector that wants to report 0 can simply exit 0. The
consequence to write down is narrower and worth writing down anyway: the
helper's exit code is a value the guest chooses, not a value the platform
observes. Nothing upstream of the helper should treat it as a trustworthy
signal about what happened inside.

> **Corrected by experiment 12 (WP10, `exp12.md`).** The interception is real:
> from guest root on a read-only share, `0x7602` returns 0 where the unclaimed
> `0x7601` is refused. But the stored value does not survive. libkrun's own
> init reports the workload's exit status through the same ioctl after
> `waitpid`, so it writes the atomic last, and a workload that asks for 42 and
> exits 7 gives a helper that exits 7. The sentence to carry forward is
> narrower than the paragraph above: the helper's exit code is the workload's
> exit status, and the ioctl grants nothing the workload did not already have.
> The conclusion stands: nothing upstream should treat the exit code as a
> signal about what happened inside. Experiment 13 adds the other half, a
> guest kernel panic exits 0.

Every other ioctl reaches `PassthroughFs::ioctl`, which handles only
`VIRTIO_IOC_EXPORT_FD_REQ` and needs `cfg.export_table` to be `Some`
(`linux/passthrough.rs:2162-2193`). We never configure one, so that arm returns
EOPNOTSUPP too, and the default arm returns EOPNOTSUPP for everything else.
This is the same EOPNOTSUPP the PLAN records as the reason a guest-side overlay
cannot copy up.

I found no "remove root dir" request in 1.19.4. `Opcode::Rmdir` is an ordinary
FUSE rmdir handled by `server.rs:417-433`, and on the read-only shares it is
EROFS (`read_only.rs:376-378`). The root inode (`fuse::ROOT_ID == 1`) is not
special-cased in `unlink` or `rmdir`; it is simply never the *name* argument,
only the parent. Whatever the 1.18 restriction was, no residue of it is
reachable here. Recorded as "none found".

### What `read_only` actually blocks

`PassthroughFsRo` rejects with EROFS: `setattr`, `symlink`, `mknod`, `mkdir`,
`unlink`, `rmdir`, `rename`, `link`, `create`, `write`, `fallocate`,
`setxattr`, `removexattr`, `copyfilerange` (`read_only.rs:325-471`). It also:

- forces `O_RDONLY` and rejects `O_TRUNC` and `O_TMPFILE` at `open`
  (`read_only.rs:39-53`, `:108-117`),
- rejects a non-RDONLY `opendir` (`read_only.rs:176-190`),
- rejects `access(W_OK)` (`read_only.rs:238-243`),
- rejects a DAX `setupmapping` that asks for `WRITE` (`read_only.rs:256-272`),
- sets `ST_RDONLY` in `statfs` (`read_only.rs:156-160`),
- strips `WRITEBACK_CACHE` at init so the guest kernel does not buffer writes
  it is about to be denied (`read_only.rs:73-77`).

It does **not** block `ioctl` (delegated, covered above), `getxattr`, or
`listxattr` (both read-only). Methods it does not override fall through to the
trait defaults, which are ENOSYS (`filesystem.rs:1162-1175`), so the wrapper
fails closed against future additions - the file says so itself at
`read_only.rs:6-10`. So: yes, it covers setattr, yes it covers xattr writes,
yes it covers fallocate, and no, it does not cover ioctls, which is fine only
because the inner ioctl surface is empty.

### The README warning, concretely: `..`, and `/` too

The warning is `README.md:96`: libkrun "does not provide any protection against
the guest attempting to access other directories in the same filesystem, or
even other filesystems in the host." Here is what it is actually about.

`PassthroughFs::lookup` takes the guest's `name` and calls
`openat(parent_fd, name, O_PATH | O_NOFOLLOW | O_CLOEXEC)`
(`linux/passthrough.rs:951-983`). The only validation the name receives on the
way there is `bytes_to_cstr`, which checks for a trailing NUL and no interior
NULs (`server.rs:1493-1497`). There is no rejection of `.`, of `..`, or of `/`.
There is no `RESOLVE_BENEATH`, no `RESOLVE_IN_ROOT`, no `openat2` anywhere in
the tree.

So `lookup(ROOT_ID, "..")` opens the parent of the shared directory, and
`lookup(ROOT_ID, "../../../etc")` opens `/etc`, because `openat` happily
resolves multi-component paths. Once looked up, the escaped inode is an
ordinary entry in the inode table and every other operation works on it. The
same unvalidated name reaches `mkdir` (`:1066`), `create` (`:1165`), `unlink`
(`:1234`), `rename` (`:1454`), `link` (`:1538`), and `symlink` (`:1580`).

So the answer to "is it about symlinks, hard links, `..`, file handles, or
mount crossings" is: **`..` and embedded `/`, in a single name argument.** Not
symlinks - `O_NOFOLLOW` is on every `openat`, and `open_inode` reopens through
`/proc/self/fd` (`linux/passthrough.rs:499-547`) rather than by path. Not file
handles - `name_to_handle_at` and `open_by_handle_at` appear nowhere. Not mount
crossings as a separate mechanism - crossing a mount point is just what `..`
and `/` get you, and `lookup` notices it only to set `ATTR_SUBMOUNT`
(`:983`). Hard links are a real but lesser case: `link` can only create a
name, and `create`/`open` never follow one anywhere new.

Upstream states the intended fix in a comment on the struct itself
(`linux/passthrough.rs:394-398`): "Users that wish to serve only a specific
directory should set up the environment so that that directory ends up as the
root of the file system process. One way to accomplish this is via a
combination of mount namespaces and the `pivot_root` system call." libkrun does
not do this for you. It does not chroot, it does not unshare a mount namespace,
it does not open the share with `RESOLVE_IN_ROOT`.

### As a write-escape question

Since WP04b the root share is podman's per-container writable layer, served
read-write by a server running as uid 0 in the helper. So: can guest writes
land outside `/rootfs`?

**At T1 and T2, no - and not because of anything libkrun does.** The Linux FUSE
client only ever sends a LOOKUP for a single path component that the VFS has
already resolved, and the VFS resolves `.` and `..` itself and cannot put `/`
inside a dentry name. An unprivileged or root guest process has no syscall that
turns into a FUSE lookup for `..`. The confinement is real but it lives
entirely in the guest kernel, on the other side of the trust boundary.

**At T3, yes.** A guest that composes FUSE requests directly can create, write,
chown, chmod, and setxattr anywhere the virtiofs server process can reach. What
actually bounds the damage then is the helper container:

- the server's own mount namespace and root - it can only name what is mounted
  into the helper: its own image layer, `/rootfs`, `/init`, `/venv`, `/sock`,
  `/scratch-backing`, and `/deps.img`. Several of those are binds out of
  `$SPIKE_REACTOR_DIR/<id>/`, so an escape reaches the reactor's per-connector
  directory. Podman's storage as a whole is *not* mounted into the helper, and
  because every path is resolved with `openat` from an fd inside the namespace,
  "same host filesystem" does not help the guest - only "same mount namespace"
  would.
- the server's credentials. `set_creds` sets the thread's euid/egid to the
  guest-supplied `ctx.uid`/`ctx.gid` via `setresuid(-1, uid, -1)` and restores
  on drop (`linux/passthrough.rs:80-131`, `:791-820`). Note the special case:
  **uid 0 is not scoped at all** (`:811-812`, and the comment at `:797-800`),
  so a request claiming uid 0 runs with whatever privilege the helper process
  itself has. That is deliberate - it is what makes a guest-root write to the
  root share work - and at T3 the uid in the header is whatever the guest says.

So the write-escape boundary is the helper container, exactly as libkrun's
security model says it should be. There is no second line of defence inside
libkrun, and the spike should not claim one.

Two lesser T2 items in the same family, both bounded to the root share and
therefore to a layer podman deletes with the container: `mknod` creates real
device nodes with a guest-chosen `rdev` (`linux/passthrough.rs:1498-1530`), and
`setxattr` is enabled by default (`cfg.xattr` at `:385`) so `security.*`
attributes including file capabilities can be written (`:1770-1815`). Neither
gives the guest anything it can then use from inside the VM; both leave objects
in podman's storage that a host-side process should not be tempted to trust.

### DAX mapping arithmetic overflows

`setupmapping` bounds-checks with `if (moffset + len) > shm_size`
(`linux/passthrough.rs:2081`) and then computes `addr = host_shm_base + moffset`
(`:2085`) before `mmap(addr, len, ..., MAP_SHARED | MAP_FIXED, fd, foffset)`
(`:2092-2101`). Both additions are plain u64. In a release build they wrap, so
`moffset = u64::MAX, len = 1` passes the check and maps at
`host_shm_base - 1`. `removemapping` has the same unchecked pattern and
additionally computes `addr` before checking (`:2109-2121`), though the check
still gates the `mmap`.

This is only reachable when a share has a DAX window - i.e. under `--venv-dax`.
The `moffset` a normal guest sends comes from the guest kernel's DAX range
allocator, not from userspace, so this is T3. The read-only wrapper rejects
`WRITE` mappings (`read_only.rs:256-272`) but not the offsets, so
`--venv-dax` on a read-only share does not close it.

## virtio-net

`Tap` is a byte mover and nothing else: `open("/dev/net/tun")`, `TUNSETIFF`
with `IFF_TAP | IFF_NO_PI | IFF_VNET_HDR`, `TUNSETVNETHDRSZ(12)`,
`TUNSETOFFLOAD`, then `read()` and `write()` on the fd
(`net/tap.rs:28-114`). `read_frame` and `write_frame` do not look at a single
byte of the frame - not the virtio-net header, not the MAC, not the ethertype.
The TX path copies descriptor bytes into a fixed `MAX_BUFFER_SIZE` buffer,
clamped with `cmp::min` (`net/worker.rs:329-347`), and hands the result to
`write()`.

The device offers `VIRTIO_NET_F_MAC` and two queues - RX and TX, no control
queue (`net/device.rs:93`, `:179-180`). There is no VIRTIO_NET_CTRL path at
all, so the guest cannot change its MAC through the device, cannot enable
promiscuous mode, and cannot program VLAN or multicast filters. Whatever
arrives on `tap0` is the egress ruleset's problem, which is where WP02 and WP07
already put it.

## virtio-console

The guest can send control messages on the control TX queue, and this is the
one place where an unchecked guest integer indexes a host-side `Vec`.

`process_control_tx` reads a `VirtioConsoleControl { id: u32, event: u16, value:
u16 }` from the descriptor (`console/device.rs:174-195`) and dispatches on
`event`:

- `VIRTIO_CONSOLE_DEVICE_READY`: iterates `0..self.ports.len()` and sends a
  PORT_ADD for each. Safe.
- `VIRTIO_CONSOLE_PORT_READY`: **`self.ports[cmd.id as usize]`**
  (`console/device.rs:212` and `:224`), with no bounds check against
  `self.ports`, which is a `Vec<Port>` (`console/device.rs:58`). An id past the
  end panics the thread.
- `VIRTIO_CONSOLE_PORT_OPEN`: pushes `cmd.id as usize` onto `ports_to_start`
  (`:249`), which is then used to index `self.queues` via
  `port_id_to_queue_idx` and `self.ports` (`:255-270`). Out of range panics; so
  does sending PORT_OPEN twice for the same port, because the queues are
  `.take()`n on the first one and the second hits `.expect("port rx queue
  should exist")` on a `None` (`:261-268`).
- anything else: a warning.

Guest-controllable, and the host effect is a panic in the VMM. It is T3 (a
normal guest kernel sends well-formed ids) and the impact is denial of service
against a VM the guest could have killed anyway - the helper dies, which is the
path WP13 is already going to exercise. Beyond that, `console_resize`,
`port_add`, `port_name` and `mark_console_port` are all host-to-guest
(`console/console_control.rs:72-126`); the guest cannot invoke them. Reads and
writes on a port go to the stdio descriptors the shim passed and nowhere else.

## virtio-balloon

Only free-page reporting is serviced. The device offers `STATS_VQ`,
`FREE_PAGE_HINT` and `REPORTING` (`balloon/device.rs:27-30`), but the event
handler reads and discards the inflate, deflate, stats and page-hint queue
events and acts only on `FRQ_INDEX` (`balloon/event_handler.rs:23-81`).

`process_frq` walks the descriptor chain and, for each descriptor, calls
`madvise(host_addr, desc.len, MADV_DONTNEED)` where `host_addr =
mem.get_host_address(desc.addr).unwrap()` (`balloon/device.rs:88-107`).

To answer the brief's question directly: **the address is validated, the length
is not.** `get_host_address` resolves `desc.addr` against the guest memory
regions and the `unwrap` turns a bad address into a panic. But nothing checks
that `desc.addr + desc.len` stays inside the region, and the virtqueue layer
does not help: `DescriptorChain::checked_new` validates only the descriptor
table offset and the `next` index, and `is_valid` is literally `!self.has_next()
|| self.next < self.queue_size` (`virtio/queue.rs:223-266`). Neither `addr` nor
`len` is bounds-checked there.

So a descriptor with a valid `addr` near the top of guest RAM and a `len` up to
4 GiB makes the helper `MADV_DONTNEED` off the end of the guest memory mapping
and into whatever the VMM has mapped next - its own heap, its thread stacks,
the DAX window. `madvise` stops with ENOMEM at the first unmapped gap, so how
far it gets depends on the helper's address-space layout, but "zeroes VMM
memory" is the right way to think about it. T3, since the descriptors come from
the guest kernel's page reporting path.

## Devices not covered by this read

The brief's file list did not include virtio-blk, virtio-rng, gpu, input or
snd. I checked one thing outside it because a wrong answer would have mattered:
a block device configured `read_only` is opened without `.write()`
(`block/device.rs:241-249`), so the host fd itself cannot write and a guest
write to the shared per-tag deps image fails in the kernel, not in a flag
check. That holds **only if the shim passes `read_only=true`** to
`krun_add_disk*`; CONTRACTS says it does, and nothing in this package verified
it. gpu, input and snd are not configured by the shim and their devices are
never attached.

---

# For WP10

## Probes to run

Numbered for citation in exp12's table. All of these are T1 or T2 - they run as
the guest workload. They cannot reach the T3 findings above, which is the point:
exp 12 is a gate on what a connector can do, and the T3 items belong in the
report's open problems, not in the gate.

1. **vsock connect to the mapped port.** `connect(AF_VSOCK, cid=2,
   port=49092)`. Expect `ECONNRESET`, promptly (`muxer.rs:552-559`). This is
   the "refused" the experiment is asking for.
2. **vsock connect to unmapped ports.** Try at least `1`, `620` (the TSI proxy
   port), `1024`, `49091`, `49093`. Expect **no response at all** - the host
   sends nothing (`muxer.rs:548-583`). **Use a non-blocking connect or
   `settimeout()`**; a blocking `connect()` on AF_VSOCK has no default timeout
   and will hang the probe forever. Record the result as "timeout", and say so
   in exp12 rather than calling it "refused" - the mechanism is different from
   probe 1 and the report should not blur them.
3. **TSI proxy-create datagram.** Bytes below. Capture `ss -tunap` in the helper
   before and after; expect zero new sockets and zero new fds. If
   `socket(AF_VSOCK, SOCK_DGRAM)` fails outright in the guest, record that as
   the result - it is a stronger negative than the one the experiment asked
   for, not a failed probe. (libkrun offers `VIRTIO_VSOCK_F_DGRAM`,
   `vsock/device.rs:34-36`, so the device side supports it; whether libkrunfw's
   kernel exposes it to userspace is what the probe finds out.)
4. **TSI datagram to the other control ports.** Same socket, ports 1025-1031,
   any 8-byte payload. Same expectation: nothing opens. One run, one line in the
   table.
5. **Device inventory.** `ls /sys/bus/virtio/devices` plus `lsblk`, as the
   experiment already specifies.
6. **Exit-code ioctl (T2, needs `--run-as-root` or `--as-root-exec`).**
   `ioctl(open("/etc/hostname"), 0x7602, 42)` - any fd on any virtiofs share,
   including `/venv` - then let the workload exit 0. If the helper exits 42, the
   report says the exit code is guest-chosen. Cheap, and it closes the question
   rather than leaving it as source reading.
7. **Escape attempt from guest userspace (T1).** `open("/../etc/passwd")`,
   `os.stat("/..")` compared against `os.stat("/")`, and a write to
   `/../../tmp/x`. Expect all of them to stay inside the share. This probe is
   the evidence that the `..` finding is not reachable from userspace, which is
   the sentence the report needs; do not skip it because the source read
   already says so.

Probes 6 and 7 are the two that turn source claims into measurements. If
session time is short, drop 4 before either of them.

## TSI proxy-create: exact bytes

Guest sends an **AF_VSOCK SOCK_DGRAM** packet to **CID 2, port 1024**
(`defs::TSI_PROXY_CREATE`, `vsock/mod.rs:84`). The muxer routes it by the
packet's `type_()` field being `VSOCK_TYPE_DGRAM` (`vsock/device.rs:159-164`)
and then by `dst_port` (`muxer.rs:514`).

Payload is `TsiProxyCreate` (`packet.rs:101-106`), parsed little-endian by
`read_proxy_create` (`packet.rs:627-641`):

| offset | size | field | value to send |
|---|---|---|---|
| 0 | 4 | `peer_port` u32 LE | any nonzero id, e.g. `0x00003039` (12345) |
| 4 | 2 | `family` u16 LE | `0x0002` = `LINUX_AF_INET` (`vsock/mod.rs:95`) |
| 6 | 2 | `_type` u16 LE | `0x0001` = `SOCK_STREAM` (`vsock/mod.rs:79`) |

**Send 8 bytes, not 6.** The parser's guard is `buf_size >= 6` but it reads
through byte 8; a 6- or 7-byte payload would panic the VMM if the code were
reachable, and we want the probe to prove the gate, not to trip an unrelated
bug.

Wire bytes for the values above:

```
39 30 00 00 02 00 01 00
```

In Python (stdlib only, per CONTRACTS):

```python
import socket, struct
VMADDR_CID_HOST = 2
TSI_PROXY_CREATE = 1024
s = socket.socket(socket.AF_VSOCK, socket.SOCK_DGRAM, 0)
s.sendto(struct.pack("<IHH", 12345, 2, 1), (VMADDR_CID_HOST, TSI_PROXY_CREATE))
```

Expected host behaviour, to assert against: `send_dgram_pkt` matches no TSI arm
because `tsi_flags.tsi_enabled()` is false, falls to `_`, and - since the
packet's op is `VSOCK_OP_RW` - reaches `process_dgram_rw`, which finds no proxy
for id `(src_port << 32) | 620` and logs "DGRAM ignoring OP_RW"
(`muxer.rs:488-500`, `:514-534`). No `socket()`, no `connect()`, no `bind()`.
`ss -tunap` in the helper is unchanged, and so is `ls /proc/<vmm-pid>/fd`.

The other control ports take different payloads, but none of them is parsed at
flags=0, so for probe 4 any 8 bytes will do - the dispatch decision happens
before the payload is read.
