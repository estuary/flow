#!/usr/bin/env bash
# Check every source citation in spike/report/libkrun-exposure.md against a
# libkrun checkout.
#
# WP11's report makes claims about code and cites 96 line ranges to back them.
# A line range is only honest while something checks it, and libkrun moves. This
# is that check: for each citation, the file exists, the range is inside it, and
# the range still contains the text the report's claim rests on.
#
#   check-exposure-citations.sh              clone v1.19.4 into a temp dir
#   check-exposure-citations.sh --src DIR    check against an existing checkout
#
# The table below is the report transcribed, not derived from the source: the
# substring is what the report says is there, so a range that moves fails rather
# than silently re-pinning itself. Two conventions in the report need a human to
# read them, and this is where that reading is recorded:
#   - a bare `:NNN` continues the previous citation's file, except in "The README
#     warning, concretely", where the six ops named (`mkdir`, `create`, `unlink`,
#     `rename`, `link`, `symlink`) are PassthroughFs methods and the nearest
#     preceding full citation is server.rs;
#   - `1`, `620`, `1024`, `49091` and `49093` in the probe list are vsock port
#     numbers, not line numbers.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env-common.sh"

# The tag and commit the report names in its header. Checked, not assumed: a
# report that cites one tree and a checker that reads another proves nothing.
LIBKRUN_TAG=v1.19.4
LIBKRUN_COMMIT=728df8125077d0db44265f6e997c72b81b65c015
REPORT="$SPIKE_DIR/report/libkrun-exposure.md"

SRC=""
if [ "${1:-}" = "--src" ] && [ -n "${2:-}" ]; then
    SRC="$2"
elif [ $# -ne 0 ]; then
    echo "usage: check-exposure-citations.sh [--src DIR]" >&2
    exit 2
fi

for want in "$LIBKRUN_TAG" "$LIBKRUN_COMMIT"; do
    grep -qF "$want" "$REPORT" ||
        { echo "report no longer names $want; update this script with it" >&2; exit 2; }
done

if [ -z "$SRC" ]; then
    SRC="$(mktemp -d)"
    trap 'rm -rf "$SRC"' EXIT
    echo "cloning libkrun $LIBKRUN_TAG into $SRC"
    git clone --quiet --depth 1 --branch "$LIBKRUN_TAG" \
        https://github.com/containers/libkrun "$SRC"
fi

head="$(git -C "$SRC" rev-parse HEAD)"
if [ "$head" != "$LIBKRUN_COMMIT" ]; then
    echo "checkout is at $head, want $LIBKRUN_COMMIT" >&2
    exit 2
fi

# Tab-separated: path, first line, last line, the text that must be in the range.
# The check itself: one line per citation, then a verdict.
python3 - "$SRC" <<'CHECK'
import sys

SRC = sys.argv[1]

# Tab-separated: path, first line, last line, the text that must be in the range.
TABLE = """\
README.md	88	88	the guest and the VMM pertain to the same security context
README.md	96	96	does not** provide any protection against the guest attempting to access other directories
src/devices/src/virtio/vsock/muxer.rs	514	534	tsi_flags.tsi_enabled()
src/devices/src/virtio/vsock/muxer.rs	514	514	dst_port
src/devices/src/virtio/vsock/muxer.rs	286	297	HIJACK_UNIX
src/devices/src/virtio/vsock/muxer.rs	321	332	HIJACK_UNIX
src/devices/src/virtio/vsock/muxer.rs	488	500	process_dgram_rw
src/devices/src/virtio/vsock/muxer.rs	509	512	dst_cid
src/devices/src/virtio/vsock/muxer.rs	539	584	process_op_request
src/devices/src/virtio/vsock/muxer.rs	548	583	unix_ipc_port_map
src/devices/src/virtio/vsock/muxer.rs	552	559	socket that is listening, sending rst
src/devices/src/virtio/vsock/muxer.rs	563	581	pkt.dst_port()
src/devices/src/virtio/vsock/muxer.rs	633	668	fn process_stream_rw
src/devices/src/virtio/vsock/muxer.rs	697	700	dst_cid
src/devices/src/virtio/vsock/muxer.rs	14	15	#[cfg(target_os = "macos")]
src/devices/src/virtio/vsock/muxer_thread.rs	152	173	unix_ipc_port_map
src/devices/src/virtio/vsock/reaper.rs	27	57	fn check_expiration
src/devices/src/virtio/vsock/tsi_stream.rs	77	83	defs::LINUX_AF_INET => AddressFamily::Inet
src/devices/src/virtio/vsock/tsi_stream.rs	433	445	get_unixsock_path
src/devices/src/virtio/vsock/packet.rs	101	106	TsiProxyCreate
src/devices/src/virtio/vsock/packet.rs	404	414	buf_size
src/devices/src/virtio/vsock/packet.rs	627	641	read_proxy_create
src/devices/src/virtio/vsock/device.rs	34	36	VIRTIO_VSOCK_F_DGRAM
src/devices/src/virtio/vsock/device.rs	159	164	VSOCK_TYPE_DGRAM
src/devices/src/virtio/vsock/mod.rs	17	18	#[cfg(target_os = "macos")]
src/devices/src/virtio/vsock/mod.rs	37	50	!self.is_empty()
src/devices/src/virtio/vsock/mod.rs	79	79	SOCK_STREAM
src/devices/src/virtio/vsock/mod.rs	84	84	pub const TSI_PROXY_CREATE: u32 = 1024;
src/devices/src/virtio/vsock/mod.rs	95	95	LINUX_AF_INET
src/libkrun/src/lib.rs	1511	1547	pub unsafe extern "C" fn krun_add_vsock_port2
src/libkrun/src/lib.rs	1527	1533	return -libc::EEXIST
src/libkrun/src/lib.rs	2644	2667	VsockConfig::Explicit { tsi_flags }
src/libkrun/src/lib.rs	2938	2949	VsockConfig::Explicit { tsi_flags }
src/vmm/src/builder.rs	1049	1058	tsi_hijack
src/devices/src/virtio/fs/augment_fs.rs	719	744	0x7602
src/devices/src/virtio/fs/worker.rs	107	128	AugmentFs
src/devices/src/virtio/fs/filesystem.rs	1162	1175	fn ioctl(
src/devices/src/virtio/fs/server.rs	417	433	fn rmdir(&self, in_header: InHeader
src/devices/src/virtio/fs/server.rs	1493	1497	fn bytes_to_cstr
src/devices/src/virtio/fs/read_only.rs	6	10	Unoverridden methods fall back to the trait defaults
src/devices/src/virtio/fs/read_only.rs	39	53	read_only
src/devices/src/virtio/fs/read_only.rs	73	77	WRITEBACK_CACHE
src/devices/src/virtio/fs/read_only.rs	108	117	read_only
src/devices/src/virtio/fs/read_only.rs	156	160	ST_RDONLY
src/devices/src/virtio/fs/read_only.rs	176	190	O_RDONLY
src/devices/src/virtio/fs/read_only.rs	238	243	mask & (libc::W_OK as u32)
src/devices/src/virtio/fs/read_only.rs	256	272	setupmapping
src/devices/src/virtio/fs/read_only.rs	306	321	fn ioctl(
src/devices/src/virtio/fs/read_only.rs	325	471	Err(erofs())
src/devices/src/virtio/fs/read_only.rs	376	378	fn rmdir(&self, _ctx: Context, _parent: Inode, _name: &CStr)
src/devices/src/virtio/fs/linux/passthrough.rs	80	131	We want credential changes to be per-thread
src/devices/src/virtio/fs/linux/passthrough.rs	385	385	xattr: true
src/devices/src/virtio/fs/linux/passthrough.rs	394	398	pivot_root system call
src/devices/src/virtio/fs/linux/passthrough.rs	499	547	/proc/self/fd
src/devices/src/virtio/fs/linux/passthrough.rs	791	820	set_creds
src/devices/src/virtio/fs/linux/passthrough.rs	797	800	Always allow "root" accesses even if we don't have root powers
src/devices/src/virtio/fs/linux/passthrough.rs	811	812	if uid == 0 || self.my_uid == Some(uid)
src/devices/src/virtio/fs/linux/passthrough.rs	951	983	libc::O_PATH | libc::O_NOFOLLOW
src/devices/src/virtio/fs/linux/passthrough.rs	983	983	ATTR_SUBMOUNT
src/devices/src/virtio/fs/linux/passthrough.rs	1066	1066	fn mkdir(
src/devices/src/virtio/fs/linux/passthrough.rs	1165	1165	fn create(
src/devices/src/virtio/fs/linux/passthrough.rs	1234	1234	fn unlink(
src/devices/src/virtio/fs/linux/passthrough.rs	1454	1454	fn rename(
src/devices/src/virtio/fs/linux/passthrough.rs	1498	1530	set_creds
src/devices/src/virtio/fs/linux/passthrough.rs	1538	1538	fn link(
src/devices/src/virtio/fs/linux/passthrough.rs	1580	1580	fn symlink(
src/devices/src/virtio/fs/linux/passthrough.rs	1770	1815	cfg.xattr
src/devices/src/virtio/fs/linux/passthrough.rs	2081	2081	(moffset + len) > shm_size
src/devices/src/virtio/fs/linux/passthrough.rs	2085	2085	addr = host_shm_base + moffset
src/devices/src/virtio/fs/linux/passthrough.rs	2092	2101	libc::mmap(
src/devices/src/virtio/fs/linux/passthrough.rs	2109	2121	removemapping
src/devices/src/virtio/fs/linux/passthrough.rs	2118	2118	moffset
src/devices/src/virtio/fs/linux/passthrough.rs	2162	2193	VIRTIO_IOC_EXPORT_FD_REQ
src/devices/src/virtio/fs/linux/passthrough.rs	2168	2174	export_table
src/devices/src/virtio/fs/linux/passthrough.rs	2192	2192	libc::EOPNOTSUPP
src/devices/src/virtio/net/tap.rs	28	114	write_frame
src/devices/src/virtio/net/tap.rs	91	128	impl NetBackend for Tap
src/devices/src/virtio/net/device.rs	93	93	VIRTIO_NET_F_MAC
src/devices/src/virtio/net/device.rs	179	180	[_; NUM_QUEUES]
src/devices/src/virtio/net/worker.rs	329	347	cmp::min
src/devices/src/virtio/console/device.rs	58	58	Vec<Port>
src/devices/src/virtio/console/device.rs	174	195	let cmd: VirtioConsoleControl
src/devices/src/virtio/console/device.rs	212	212	self.ports[cmd.id as usize]
src/devices/src/virtio/console/device.rs	224	224	self.ports[cmd.id as usize]
src/devices/src/virtio/console/device.rs	249	249	cmd.id as usize
src/devices/src/virtio/console/device.rs	249	268	cmd.id
src/devices/src/virtio/console/device.rs	255	270	port_id_to_queue_idx
src/devices/src/virtio/console/device.rs	261	268	self.queues
src/devices/src/virtio/console/console_control.rs	72	126	mark_console_port
src/devices/src/virtio/balloon/device.rs	27	30	FREE_PAGE_HINT
src/devices/src/virtio/balloon/device.rs	88	107	get_host_address
src/devices/src/virtio/balloon/event_handler.rs	23	79	Failed to read balloon inflate queue event
src/devices/src/virtio/balloon/event_handler.rs	23	81	process_frq
src/devices/src/virtio/block/device.rs	241	249	read_only
src/devices/src/virtio/queue.rs	223	266	desc.addr
src/devices/src/virtio/queue.rs	264	266	!self.has_next() || self.next < self.queue_size
"""

failures = 0
checked = 0
cache = {}
for row in TABLE.splitlines():
    path, first, last, want = row.split("\t")
    first, last = int(first), int(last)
    checked += 1
    if path not in cache:
        with open(f"{SRC}/{path}", encoding="utf-8", errors="replace") as f:
            cache[path] = f.read().splitlines()
    lines = cache[path]
    if last > len(lines):
        print(f"FAIL  {path}:{first},{last}  file has only {len(lines)} lines")
        failures += 1
    elif want in "\n".join(lines[first - 1:last]):
        print(f"ok    {path}:{first},{last}  {want}")
    else:
        where = [i + 1 for i, line in enumerate(lines) if want in line]
        print(f"FAIL  {path}:{first},{last}  not in range: {want}"
              + (f" (found at {where})" if where else " (not in the file at all)"))
        failures += 1

print("---")
if failures:
    print(f"{failures} of {checked} CITATIONS DO NOT RESOLVE")
    sys.exit(1)
print(f"ALL CITATIONS RESOLVE ({checked} checked)")
CHECK
