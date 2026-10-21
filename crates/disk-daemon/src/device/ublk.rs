use anyhow::Context;

/// The number of requests that the kernel can give the device at the same time.
pub(super) const QUEUE_DEPTH: u16 = 16;

/// The largest request, in bytes, that the device accepts.
pub(super) const MAX_IO_BUF_BYTES: u32 = 128 * 1024;

/// The largest discard or write-zeroes request, in bytes, that the device
/// accepts.
const MAX_DISCARD_BYTES: u32 = 16 << 20;

/// The unit of the offset and length of every ublk request, whatever the logical
/// block size of the device.
const SECTOR_SIZE: u64 = 512;

/// The kernel parameters for a device of `blocks` × `crate::BLOCK_SIZE` bytes.
///
/// `attrs` does not set `UBLK_ATTR_VOLATILE_CACHE`, so the kernel sends no
/// flush or force-unit-access requests, and this crate serves neither.
fn params(blocks: u32) -> UblkParams {
    let sectors = blocks as u64 * crate::BLOCK_SIZE as u64 / SECTOR_SIZE;
    let block_shift = crate::BLOCK_SIZE.trailing_zeros() as u8;

    UblkParams {
        // Short of `size_of`, whose trailing padding the kernel would read as the
        // start of the next parameter type.
        len: (std::mem::offset_of!(UblkParams, discard) + std::mem::size_of::<UblkParamDiscard>())
            as u32,
        types: UBLK_PARAM_TYPE_BASIC | UBLK_PARAM_TYPE_DISCARD,
        basic: UblkParamBasic {
            attrs: 0,
            logical_bs_shift: block_shift,
            physical_bs_shift: block_shift,
            io_opt_shift: block_shift,
            io_min_shift: block_shift,
            max_sectors: MAX_IO_BUF_BYTES / SECTOR_SIZE as u32,
            chunk_sectors: 0,
            dev_sectors: sectors,
            virt_boundary_mask: 0,
        },
        discard: UblkParamDiscard {
            discard_alignment: 0,
            // The block size, so that each discard clears whole bits of the
            // compactor's allocation bitmap.
            discard_granularity: crate::BLOCK_SIZE,
            max_discard_sectors: MAX_DISCARD_BYTES / SECTOR_SIZE as u32,
            max_write_zeroes_sectors: MAX_DISCARD_BYTES / SECTOR_SIZE as u32,
            // A ublk request carries one range, and the kernel rejects
            // parameters that advertise more.
            max_discard_segments: 1,
            reserved0: 0,
        },
    }
}

const CONTROL_PATH: &str = "/dev/ublk-control";

/// One ublk device, through its own handle on `/dev/ublk-control`, which sends
/// the commands that add, configure, start, stop, and delete the device.
///
/// Each command is an io_uring `URING_CMD` on a private ring, and each call waits
/// until its command completes.
pub(super) struct Control {
    file: std::fs::File,
    ring: io_uring::IoUring<io_uring::squeue::Entry128, io_uring::cqueue::Entry>,
    dev_id: u32,
}

impl Control {
    /// Add a device of `blocks` × `crate::BLOCK_SIZE` bytes, and set its
    /// parameters.
    pub fn add(blocks: u32) -> anyhow::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(CONTROL_PATH)
            .with_context(|| format!("opening {CONTROL_PATH}"))?;

        // A ring entry carries a command's bytes in its tail. A standard
        // 64-byte entry has room for 16 bytes, and a `ublksrv_ctrl_cmd` is 32,
        // so the control ring uses 128-byte entries, which have room for 80.
        let ring =
            io_uring::IoUring::<io_uring::squeue::Entry128, io_uring::cqueue::Entry>::builder()
                .build(1)?;

        // A `dev_id` of `u32::MAX`, which the kernel reads as -1, tells it to
        // choose the device number. The command and its `info` payload must
        // give the same `dev_id`, and the kernel writes the number it chose back
        // into `info`.
        let mut control = Self {
            file,
            ring,
            dev_id: u32::MAX,
        };
        let mut info = UblksrvCtrlDevInfo {
            nr_hw_queues: 1,
            queue_depth: QUEUE_DEPTH,
            max_io_buf_bytes: MAX_IO_BUF_BYTES,
            dev_id: u32::MAX,
            flags: UBLK_F_USER_COPY | UBLK_F_NO_AUTO_PART_SCAN,
            ..Default::default()
        };
        () = control
            .issue(
                UBLK_U_CMD_ADD_DEV,
                &UblksrvCtrlCmd {
                    len: std::mem::size_of_val(&info) as u16,
                    addr: std::ptr::from_mut(&mut info) as u64,
                    ..command(u32::MAX)
                },
            )
            .context("adding a ublk device")?;
        control.dev_id = info.dev_id;
        tracing::debug!(dev_id = control.dev_id, "added a ublk device");

        let params = params(blocks);
        let set = control.issue(
            UBLK_U_CMD_SET_PARAMS,
            &UblksrvCtrlCmd {
                len: std::mem::size_of_val(&params) as u16,
                addr: std::ptr::from_ref(&params) as u64,
                ..command(control.dev_id)
            },
        );
        match set {
            Ok(()) => Ok(control),
            Err(err) => Err(control.abandon(
                anyhow::Error::new(err).context("setting the parameters of a ublk device"),
            )),
        }
    }

    pub fn dev_id(&self) -> u32 {
        self.dev_id
    }

    pub fn char_path(&self) -> std::path::PathBuf {
        format!("/dev/ublkc{}", self.dev_id).into()
    }

    pub fn block_path(&self) -> std::path::PathBuf {
        format!("/dev/ublkb{}", self.dev_id).into()
    }

    /// Create `/dev/ublkbN`. This call blocks until each tag of the queue has a
    /// fetch in flight, so call it from a thread other than the queue's owner
    /// thread.
    pub fn start(&mut self) -> anyhow::Result<()> {
        self.issue(
            UBLK_U_CMD_START_DEV,
            &UblksrvCtrlCmd {
                // The kernel records this as the PID of the serving process,
                // and rejects a value that is not positive.
                data: [std::process::id() as u64],
                ..command(self.dev_id)
            },
        )
        .with_context(|| format!("starting ublk device {}", self.dev_id))
    }

    /// Remove `/dev/ublkbN`. After all requests in flight complete, the kernel
    /// aborts the fetches of the queue. The owner thread must continue to
    /// complete requests until then, or this call does not return.
    pub fn stop(&mut self) -> anyhow::Result<()> {
        self.issue(UBLK_U_CMD_STOP_DEV, &command(self.dev_id))
            .with_context(|| format!("stopping ublk device {}", self.dev_id))
    }

    /// Remove `/dev/ublkcN`. The kernel reuses the device number once every
    /// reference to the device is released, so close the character device
    /// before this call.
    ///
    /// DEL_DEV waits for those references, which a filesystem still mounted
    /// elsewhere, or lazily unmounted with files open, can hold indefinitely.
    /// DEL_DEV_ASYNC, which works from Linux 6.11, does not wait.
    pub fn delete(mut self) -> anyhow::Result<()> {
        // An older kernel refuses DEL_DEV_ASYNC, unacted on, with its own ENOTSUPP,
        // or with EOPNOTSUPP where it carries a later fix, as Ubuntu's 6.8 does.
        const ENOTSUPP: i32 = 524;
        let unsupported = [ENOTSUPP, rustix::io::Errno::OPNOTSUPP.raw_os_error()];

        let deleted = match self.issue(UBLK_U_CMD_DEL_DEV_ASYNC, &command(self.dev_id)) {
            Err(err)
                if err
                    .raw_os_error()
                    .is_some_and(|errno| unsupported.contains(&errno)) =>
            {
                self.issue(UBLK_U_CMD_DEL_DEV, &command(self.dev_id))
            }
            deleted => deleted,
        };
        () = deleted.with_context(|| format!("deleting ublk device {}", self.dev_id))?;
        tracing::debug!(dev_id = self.dev_id, "deleted a ublk device");

        Ok(())
    }

    /// Delete the device, which `err` left unserved, and return `err`.
    pub fn abandon(self, err: anyhow::Error) -> anyhow::Error {
        let dev_id = self.dev_id;
        match self.delete() {
            Ok(()) => err,
            Err(deleted) => err.context(format!(
                "device {dev_id} could not be served, nor deleted ({deleted:#})"
            )),
        }
    }

    fn issue(&mut self, cmd_op: u32, command: &UblksrvCtrlCmd) -> std::io::Result<()> {
        let mut bytes = [0; 80];
        bytes[..std::mem::size_of::<UblksrvCtrlCmd>()]
            .copy_from_slice(zerocopy::IntoBytes::as_bytes(command));

        let entry = io_uring::opcode::UringCmd80::new(
            io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(&self.file)),
            cmd_op,
        )
        .cmd(bytes)
        .build();

        // SAFETY: the kernel uses the memory that `command` addresses until the
        // command completes, and the loop below neither returns nor unwinds before
        // then.
        unsafe { self.ring.submission().push(&entry) }
            .expect("the control ring holds one command at a time");

        // The kernel runs control commands on a worker thread, so a command is
        // still in flight after its submission. A signal ends a wait early: as
        // EINTR, or as a success that reports the entry submitted. Only a
        // completion ends this loop, so any other failure aborts, even one that
        // left the entry queued to go out with the next command.
        let result = loop {
            if let Some(cqe) = self.ring.completion().next() {
                break cqe.result();
            }
            match self.ring.submit_and_wait(1) {
                Ok(_) => (),
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => (),
                Err(err) => {
                    tracing::error!(?err, cmd_op, "waiting for a ublk control command");
                    std::process::abort();
                }
            }
        };

        if result < 0 {
            return Err(std::io::Error::from_raw_os_error(-result));
        }
        Ok(())
    }
}

fn command(dev_id: u32) -> UblksrvCtrlCmd {
    UblksrvCtrlCmd {
        dev_id,
        queue_id: u16::MAX,
        ..Default::default()
    }
}

/// A device request, over blocks of the `/dev/ublkbN` block device, which the
/// image mirrors.
#[derive(Debug)]
pub(super) enum Op {
    Read(std::ops::Range<u32>),
    Write(std::ops::Range<u32>),
    Punch(std::ops::Range<u32>),
}

impl Op {
    fn decode(desc: &UblksrvIoDesc) -> Self {
        let sectors_per_block = crate::BLOCK_SIZE as u64 / SECTOR_SIZE;
        let start = desc.start_sector / sectors_per_block;
        let end = start + desc.nr_sectors as u64 / sectors_per_block;
        let range = start as u32..end as u32;

        // The operation is the low 8 bits of `op_flags`. The other bits are
        // request flags.
        match desc.op_flags & 0xff {
            UBLK_IO_OP_READ => Op::Read(range),
            UBLK_IO_OP_WRITE => Op::Write(range),
            UBLK_IO_OP_DISCARD | UBLK_IO_OP_WRITE_ZEROES => Op::Punch(range),
            op => panic!("device request has op {op}, which this device does not serve"),
        }
    }
}

/// The request descriptors of the device's queue, which the kernel shares
/// read-only through an `mmap` of the character device.
pub(super) struct IoDescs {
    /// The descriptor of tag 0, which the descriptor of each other tag follows.
    base: *const UblksrvIoDesc,
}

/// The byte length of the descriptor mapping. The kernel rejects an `mmap` of any
/// other length, after both are rounded up to whole pages.
const IO_DESCS_LEN: usize = QUEUE_DEPTH as usize * std::mem::size_of::<UblksrvIoDesc>();

impl IoDescs {
    pub fn map(cdev: &std::fs::File) -> std::io::Result<Self> {
        // SAFETY: with a null address, the kernel chooses a free range, so the
        // mapping does not replace memory in use. The kernel rejects a length or
        // offset that is not exactly the descriptor array of one queue, and the
        // array of the device's only queue is at offset 0.
        let base = unsafe {
            rustix::mm::mmap(
                std::ptr::null_mut(),
                IO_DESCS_LEN,
                rustix::mm::ProtFlags::READ,
                rustix::mm::MapFlags::SHARED,
                cdev,
                0,
            )
        }?;
        Ok(Self { base: base.cast() })
    }

    /// The request that the kernel gave to `tag`.
    pub fn get(&self, tag: u16) -> Op {
        assert!(tag < QUEUE_DEPTH, "tag {tag} exceeds the queue depth");

        // SAFETY: the mapping holds a descriptor per tag and is alive for this
        // read. The read is volatile because the kernel, not this thread,
        // writes the descriptor, before it completes the fetch that gives the
        // request to `tag`.
        Op::decode(&unsafe { std::ptr::read_volatile(self.base.add(tag as usize)) })
    }
}

impl Drop for IoDescs {
    fn drop(&mut self) {
        // SAFETY: `base` is the mapping that `map` created, of `IO_DESCS_LEN`
        // bytes. `get` returns copies, so no reference into the mapping remains.
        _ = unsafe { rustix::mm::munmap(self.base as *mut std::ffi::c_void, IO_DESCS_LEN) };
    }
}

/// Fetch the next request for `tag`.
pub(super) fn fetch(cdev: &std::fs::File, tag: u16) -> io_uring::squeue::Entry {
    io_command(cdev, UBLK_U_IO_FETCH_REQ, tag, 0)
}

/// Answer the request on `tag` with `result`, and fetch its next request.
pub(super) fn commit_and_fetch(
    cdev: &std::fs::File,
    tag: u16,
    result: i32,
) -> io_uring::squeue::Entry {
    io_command(cdev, UBLK_U_IO_COMMIT_AND_FETCH_REQ, tag, result)
}

/// A command for `tag`, whose `user_data` is the tag, on the device's only queue.
/// With `UBLK_F_USER_COPY`, the kernel rejects a command that gives a buffer
/// address, so `addr` stays zero.
fn io_command(cdev: &std::fs::File, cmd_op: u32, tag: u16, result: i32) -> io_uring::squeue::Entry {
    let bytes: [u8; 16] = zerocopy::transmute!(UblksrvIoCmd {
        q_id: 0,
        tag,
        result,
        addr: 0,
    });

    io_uring::opcode::UringCmd16::new(
        io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(cdev)),
        cmd_op,
    )
    .cmd(bytes)
    .build()
    .user_data(tag as u64)
}

/// The offset in `/dev/ublkcN` where `pread` and `pwrite` move the data of the
/// request on `tag`, on the device's only queue.
pub(super) fn io_buf_offset(tag: u16) -> u64 {
    UBLKSRV_IO_BUF_OFFSET + ((tag as u64) << UBLK_TAG_OFF)
}

// The ABI below transcribes the parts of Linux's `include/uapi/linux/ublk_cmd.h`
// that this crate uses, under the kernel's names. A uapi struct only ever gains
// fields at its end, so a later kernel keeps each layout. The size checks catch a
// field dropped or mistyped in transcription, though not two fields of one type
// swapped.

const UBLK_U_CMD_ADD_DEV: u32 =
    rustix::ioctl::opcode::read_write::<UblksrvCtrlCmd>(b'u', 0x04) as u32;
const UBLK_U_CMD_DEL_DEV: u32 =
    rustix::ioctl::opcode::read_write::<UblksrvCtrlCmd>(b'u', 0x05) as u32;
const UBLK_U_CMD_START_DEV: u32 =
    rustix::ioctl::opcode::read_write::<UblksrvCtrlCmd>(b'u', 0x06) as u32;
const UBLK_U_CMD_STOP_DEV: u32 =
    rustix::ioctl::opcode::read_write::<UblksrvCtrlCmd>(b'u', 0x07) as u32;
const UBLK_U_CMD_SET_PARAMS: u32 =
    rustix::ioctl::opcode::read_write::<UblksrvCtrlCmd>(b'u', 0x08) as u32;
const UBLK_U_CMD_DEL_DEV_ASYNC: u32 =
    rustix::ioctl::opcode::read::<UblksrvCtrlCmd>(b'u', 0x14) as u32;
const UBLK_U_IO_FETCH_REQ: u32 =
    rustix::ioctl::opcode::read_write::<UblksrvIoCmd>(b'u', 0x20) as u32;
const UBLK_U_IO_COMMIT_AND_FETCH_REQ: u32 =
    rustix::ioctl::opcode::read_write::<UblksrvIoCmd>(b'u', 0x21) as u32;

/// The server moves request data itself, with `pread` and `pwrite` on
/// `/dev/ublkcN`. Without this flag, the kernel copies the data to and from a
/// buffer address that each command gives.
const UBLK_F_USER_COPY: u64 = 1 << 7;

/// The kernel does not scan the device for partitions when it starts. A disk
/// holds ext4 on the whole device, so a scan finds none. Through Linux 7.1, a
/// scan that runs after the server dies during start can also oops the kernel
/// (fixed by 1133b93fc7f6). A kernel that does not know this flag clears it.
const UBLK_F_NO_AUTO_PART_SCAN: u64 = 1 << 18;

/// The result of a fetch that gives a request to its tag.
pub(super) const UBLK_IO_RES_OK: i32 = 0;
/// The result of a fetch that the kernel aborts. The kernel aborts all fetches
/// after the device stops.
pub(super) const UBLK_IO_RES_ABORT: i32 = -rustix::io::Errno::NODEV.raw_os_error();

const UBLK_IO_OP_READ: u32 = 0;
const UBLK_IO_OP_WRITE: u32 = 1;
const UBLK_IO_OP_DISCARD: u32 = 3;
const UBLK_IO_OP_WRITE_ZEROES: u32 = 5;

const UBLK_PARAM_TYPE_BASIC: u32 = 1 << 0;
const UBLK_PARAM_TYPE_DISCARD: u32 = 1 << 1;

const UBLKSRV_IO_BUF_OFFSET: u64 = 0x8000_0000;
const UBLK_TAG_OFF: u32 = 25;

/// The `IntoBytes` derive, which copies the command into its ring entry without
/// `unsafe`, also fails the build if the struct has padding.
#[repr(C)]
#[derive(Default, zerocopy::IntoBytes, zerocopy::Immutable)]
struct UblksrvCtrlCmd {
    dev_id: u32,
    queue_id: u16,
    len: u16,
    addr: u64,
    data: [u64; 1],
    dev_path_len: u16,
    pad: u16,
    reserved: u32,
}

#[repr(C)]
#[derive(Default)]
struct UblksrvCtrlDevInfo {
    nr_hw_queues: u16,
    queue_depth: u16,
    state: u16,
    pad0: u16,
    max_io_buf_bytes: u32,
    dev_id: u32,
    ublksrv_pid: i32,
    pad1: u32,
    flags: u64,
    ublksrv_flags: u64,
    owner_uid: u32,
    owner_gid: u32,
    reserved1: u64,
    reserved2: u64,
}

/// `nr_sectors` shares a union with `nr_zones`, which serves only zoned devices.
#[repr(C)]
struct UblksrvIoDesc {
    op_flags: u32,
    nr_sectors: u32,
    start_sector: u64,
    addr: u64,
}

/// `addr` shares a union with `zone_append_lba`, which serves only zoned devices.
#[repr(C)]
#[derive(zerocopy::IntoBytes, zerocopy::Immutable)]
struct UblksrvIoCmd {
    q_id: u16,
    tag: u16,
    result: i32,
    addr: u64,
}

/// The kernel's `ublk_params` through `discard`, the last parameter type that this
/// crate sets. The kernel reads the `len` bytes that the server gives, as it does
/// for a server built on an older header.
#[repr(C)]
struct UblkParams {
    len: u32,
    types: u32,
    basic: UblkParamBasic,
    discard: UblkParamDiscard,
}

#[repr(C)]
#[derive(Debug)]
struct UblkParamBasic {
    attrs: u32,
    logical_bs_shift: u8,
    physical_bs_shift: u8,
    io_opt_shift: u8,
    io_min_shift: u8,
    max_sectors: u32,
    chunk_sectors: u32,
    dev_sectors: u64,
    virt_boundary_mask: u64,
}

#[repr(C)]
#[derive(Debug)]
struct UblkParamDiscard {
    discard_alignment: u32,
    discard_granularity: u32,
    max_discard_sectors: u32,
    max_write_zeroes_sectors: u32,
    max_discard_segments: u16,
    reserved0: u16,
}

const _: () = assert!(std::mem::size_of::<UblksrvCtrlCmd>() == 32);
const _: () = assert!(std::mem::size_of::<UblksrvCtrlDevInfo>() == 64);
const _: () = assert!(std::mem::size_of::<UblksrvIoDesc>() == 24);
const _: () = assert!(std::mem::size_of::<UblksrvIoCmd>() == 16);
const _: () = assert!(std::mem::size_of::<UblkParamBasic>() == 32);
const _: () = assert!(std::mem::size_of::<UblkParamDiscard>() == 20);

#[cfg(test)]
mod test {
    use super::{
        Op, SECTOR_SIZE, UBLK_IO_OP_DISCARD, UBLK_IO_OP_READ, UBLK_IO_OP_WRITE,
        UBLK_IO_OP_WRITE_ZEROES, UblksrvIoDesc,
    };

    #[test]
    fn test_params_advertise_block_discards_and_no_cache() {
        let params = super::params(32768);

        insta::assert_debug_snapshot!((params.len, params.types, params.basic, params.discard), @"
        (
            60,
            3,
            UblkParamBasic {
                attrs: 0,
                logical_bs_shift: 12,
                physical_bs_shift: 12,
                io_opt_shift: 12,
                io_min_shift: 12,
                max_sectors: 256,
                chunk_sectors: 0,
                dev_sectors: 262144,
                virt_boundary_mask: 0,
            },
            UblkParamDiscard {
                discard_alignment: 0,
                discard_granularity: 4096,
                max_discard_sectors: 32768,
                max_write_zeroes_sectors: 32768,
                max_discard_segments: 1,
                reserved0: 0,
            },
        )
        ");
    }

    #[test]
    fn test_a_descriptor_decodes_to_the_blocks_it_covers() {
        let sectors_per_block = crate::BLOCK_SIZE as u64 / SECTOR_SIZE;

        // (operation and any flags, first block, blocks)
        let cases = [
            (UBLK_IO_OP_READ, 0, 1),
            (UBLK_IO_OP_WRITE, 3, 2),
            (UBLK_IO_OP_DISCARD, 8, 8),
            (UBLK_IO_OP_WRITE_ZEROES, 15, 1),
            // A metadata read, as ext4 issues: UBLK_IO_F_META sits above the op.
            (UBLK_IO_OP_READ | 1 << 11, 2, 1),
        ];
        let decoded = cases.map(|(op_flags, block, blocks)| {
            Op::decode(&UblksrvIoDesc {
                op_flags,
                nr_sectors: blocks * sectors_per_block as u32,
                start_sector: block * sectors_per_block,
                addr: 0,
            })
        });

        insta::assert_debug_snapshot!(decoded, @"
        [
            Read(
                0..1,
            ),
            Write(
                3..5,
            ),
            Punch(
                8..16,
            ),
            Punch(
                15..16,
            ),
            Read(
                2..3,
            ),
        ]
        ");
    }
}
