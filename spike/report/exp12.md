# Experiment 12: control channel and device exposure

**PASS, with one correction to the source read.** From inside a real guest, the
two gates the design rests on hold as measurements and not just as code:
`krun_add_vsock(ctx, 0)` opens nothing when handed a TSI proxy-create datagram,
and the mapped port is inbound-only. The guest sees exactly the devices we
configured and no more, cannot write the deps disk even as root, and cannot get
`..` onto the FUSE wire. The correction is the exit-code ioctl: it is accepted
from a read-only share, as WP11 said, but **libkrun's own init overwrites it**,
so the helper's exit code is the workload's status and not the value the ioctl
asked for. The conclusion the report needs is unchanged and simpler than WP11's.

Measured at commit `0c68f442cf1` (this package's parent; the runs precede its
commit), `spike/tasks/exp12-exposure.sh`, guest
`ghcr.io/estuary/derive-python@sha256:c26548740a9e967274f6d7b9c79bed73630bf61bd187c3afca7564577ef364c7`.
Source claims are WP11's, at libkrun `v1.19.4`
(`728df8125077d0db44265f6e997c72b81b65c015`); the citations behind them are
checked by `spike/tasks/check-exposure-citations.sh`. Raw data:
`data/exp12-probes.csv`.

Every probe here is **T1 or T2** in WP11's tiering - the connector's own user, or
guest root. Nothing here reaches T3, which is the point: this experiment is a
gate on what a connector can do, and the T3 findings belong in the report's open
problems.

## The probes

| # | probe | tier | expected | observed |
|---|---|---|---|---|
| 1 | connect vsock CID 2 port 49092 | T1 | reset | `ECONNRESET`, immediately |
| 2 | connect vsock CID 2 port 1234 | T1 | no response | timeout at the probe's own 5 s |
| 3 | TSI proxy-create datagram to port 1024 | T1 | nothing opens | `ss` 0 sockets before and after; VMM fds 274 -> 265 |
| 4 | `stat("/venv/../../etc/hostname")` | T1 | resolves inside the guest | same inode as `/etc/hostname` |
| 5 | `stat("/..")` vs `stat("/")` | T1 | same file | same inode |
| 6 | device inventory | T1 | PLAN's list | `balloon:1 blk:1 console:1 fs:2 net:1 rng:1 vsock:1` |
| 7 | device inventory with `--deps-image` | T2 | one more blk | `balloon:1 blk:2 console:1 fs:2 net:1 rng:1 vsock:1` |
| 8 | write `/dev/vdb` | T2 | refused | `EPERM` |
| 9 | `ioctl(fd, 0x7602, 42)` on `/venv` | T2 | accepted | accepted, return 0 |
| 10 | `ioctl(fd, 0x7601, 42)` on `/venv` | T2 | refused | refused |

## vsock: two closed doors, two different noises

PLAN's original wording said an unmapped port "is refused". It is not, and the
difference matters to whoever writes the next probe:

- **Port 49092, the mapped one**, is `listen=true`, so the muxer answers a
  connect with a Reset (`muxer.rs:552-559`) and the guest gets a prompt
  `ECONNRESET`. The mapping is inbound-only: the reactor dials in, the guest
  cannot dial out through it.
- **Any other port** gets *nothing at all*. `process_op_request` finds the port
  in neither map and returns without a response or a log line
  (`muxer.rs:539-584`). A blocking AF_VSOCK `connect()` has no default timeout,
  so a probe without `settimeout()` hangs for ever. The 5 s in the table is the
  probe's own clock, not the kernel's.

## TSI: the gate holds, measured

The guest created an `AF_VSOCK` `SOCK_DGRAM` socket (libkrunfw's kernel does
expose it) and sent WP11's exact eight bytes - `39 30 00 00 02 00 01 00`,
`peer_port=12345, family=LINUX_AF_INET, type=SOCK_STREAM` - to CID 2 port 1024.
The host side, snapshotted inside the helper before and after with the guest
holding for eight seconds in between:

```
ss -tunap:   0 sockets before, 0 after   (and `Netid` header both times, so ss ran)
/proc/1/fd:  274 before, 265 after
```

`ss` listing **zero** TCP and UDP sockets is a stronger negative than the
experiment asked for: the helper has none at all, so a TSI proxy of any family
would have been the only row. The fd count falls over the interval as the guest
closes the files it opened at startup and the virtiofs server releases their
handles; what the probe asserts is that it does not rise.

## `..` never reaches the wire

WP11 found that `lookup` hands the guest's name straight to `openat` with no
`RESOLVE_BENEATH`, which would resolve `..` and embedded `/`. From T1 it is
unreachable, and this is the measurement: `/venv/../../etc/hostname` opens the
guest's own `/etc/hostname` (same device and inode), and `/..` is `/`. The Linux
FUSE client sends single, already-resolved components; the guest's own VFS is
what stops it, on the far side of the trust boundary. At T3 it is expressible,
and what bounds it there is the helper container's mount namespace - which is
the boundary the design claims, and an open problem rather than a gate.

## Devices

`/sys/bus/virtio/devices`, read by device id, is exactly PLAN's list: two
virtiofs (root and `venv`), one blk (scratch), one net, one vsock, one console,
one balloon, one rng. With `--deps-image` there is a second blk and nothing
else - the deps disk is a block device, not a third share. `lsblk` inside the
guest shows `vda 1G 0 disk` and nothing more.

## The deps disk is protected by the host fd

As guest root, `open("/dev/vdb", O_WRONLY)` fails `EPERM`. Which errno the guest
kernel picks is its business; what the probe establishes is that the refusal is
not negotiable from inside. libkrun opens a `read_only` block device without
`.write()` (`block/device.rs:241-249`), so the host file descriptor itself
cannot write, and the shim does pass `read_only=true` - which WP11 flagged as
the one thing its source read could not verify. It is verified now.

## The exit-code ioctl: WP11's reading, corrected

WP11 found `AugmentFs` intercepting `cmd == 0x7602` on **any** inode of **any**
share, read-only ones included, storing `arg` as the VM's exit code with no
check on inode, handle or uid (`augment_fs.rs:719-744`). All of that is true.
What the experiment adds is what happens next.

As guest root, on a file under the read-only `/venv` share:

```
ioctl(fd, 0x7602, 42)  -> 0        accepted
ioctl(fd, 0x7601, 42)  -> error    refused
then the workload exits 7
the helper exits 7
```

The control matters. `0x7601` is not claimed by `AugmentFs`, so it reaches the
passthrough and comes back `EOPNOTSUPP` (`linux/passthrough.rs:2192`). `0x7602`
returning 0 through the *read-only* wrapper is therefore proof that `AugmentFs`
intercepted it and stored the 42 - the interception is real, not a no-op.

It is overwritten, and by libkrun itself. **libkrun's init reports the
workload's exit status through the same ioctl**, on `/`, after `waitpid`
(`src/init_blob/init/init.c:1141-1152`), so it writes the atomic last and the
guest's value never survives. Run under `--debug`, the VMM says
`using vmm exit code: 0` for a workload that exited 0 after asking for 42.

So the sentence to carry into the report is narrower than "the exit code is a
value the guest chooses via an unguarded ioctl". It is: **the helper's exit code
is the guest workload's exit status, and a connector that wants to report any
value can simply exit with it.** The ioctl grants no authority the workload did
not already have, and the runtime must not treat the exit code as something the
platform observed. Experiment 13 shows the other half of that: a guest kernel
panic produces exit code 0.

## What this means for the design

- **The two gates CONTRACTS asserts are now measured, not just read.**
  `krun_add_vsock(ctx, 0)` opens nothing; the mapped port is inbound-only.
- **The deps disk's read-only-ness is the host fd**, confirmed from guest root.
- **The exit code is untrusted input.** Nothing in the runtime should branch on
  it as evidence of what happened inside the guest.
- **PLAN's experiment-12 wording about unmapped ports** ("refused") is wrong and
  is corrected above; anything that probes vsock must impose its own timeout.
