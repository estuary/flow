use anyhow::Context;

/// The ID of the only queue of each device.
pub const QUEUE_ID: u16 = 0;

/// The number of requests that the kernel can give the device at the same time.
pub const QUEUE_DEPTH: u16 = 16;

/// The largest request, in bytes, that the device accepts.
pub const MAX_IO_BUF_BYTES: u32 = 128 * 1024;

/// The largest discard or write-zeroes request, in bytes, that the device
/// accepts.
const MAX_DISCARD_BYTES: u32 = 16 << 20;

/// The unit of the offset and length of every ublk request, whatever the logical
/// block size of the device. The kernel defines it as `SECTOR_SHIFT` in a header
/// that is not uapi, so the generated bindings do not include it.
pub const SECTOR_SIZE: u64 = 512;

/// The kernel parameters for a device of `blocks` × `crate::BLOCK_SIZE` bytes.
///
/// `attrs` does not set `UBLK_ATTR_VOLATILE_CACHE`, so the kernel sends no
/// flush or force-unit-access requests, and this crate serves neither.
pub fn params(blocks: u32) -> UblkParams {
    let sectors = blocks as u64 * crate::BLOCK_SIZE as u64 / SECTOR_SIZE;
    let block_shift = crate::BLOCK_SIZE.trailing_zeros() as u8;

    UblkParams {
        len: std::mem::size_of::<UblkParams>() as u32,
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
        ..Default::default()
    }
}

pub fn char_path(dev_id: u32) -> std::path::PathBuf {
    format!("/dev/ublkc{dev_id}").into()
}

pub fn block_path(dev_id: u32) -> std::path::PathBuf {
    format!("/dev/ublkb{dev_id}").into()
}

const CONTROL_PATH: &str = "/dev/ublk-control";

/// A handle on `/dev/ublk-control`, which sends the commands that query the
/// driver's features, and add, configure, start, stop, and delete ublk devices.
///
/// Each command is an io_uring `URING_CMD` on a private ring. The mutex lets one
/// command run at a time, and each call waits until its command completes.
pub struct Control {
    inner: std::sync::Mutex<Inner>,
}

struct Inner {
    file: std::fs::File,
    ring: io_uring::IoUring<io_uring::squeue::Entry128, io_uring::cqueue::Entry>,
}

impl Control {
    pub fn open() -> anyhow::Result<Self> {
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
                .build(4)?;

        let control = Self {
            inner: std::sync::Mutex::new(Inner { file, ring }),
        };

        // Check the features once, here, so that `add_dev` does not need to check
        // which flags the kernel accepted. Linux 6.5 added GET_FEATURES and user
        // copy together.
        let mut features = 0u64;
        control
            .issue(
                UBLK_U_CMD_GET_FEATURES,
                &UblksrvCtrlCmd {
                    dev_id: u32::MAX,
                    queue_id: u16::MAX,
                    len: std::mem::size_of_val(&features) as u16,
                    addr: std::ptr::from_mut(&mut features) as u64,
                    ..Default::default()
                },
            )
            .context("ublk GET_FEATURES was refused, so this kernel predates Linux 6.5")?;

        anyhow::ensure!(
            features & UBLK_F_USER_COPY != 0,
            "this kernel's ublk_drv does not support UBLK_F_USER_COPY",
        );
        tracing::debug!("opened the ublk control device");

        Ok(control)
    }

    /// Add a device, and return the device number that the kernel chose.
    pub fn add_dev(&self, queue_depth: u16, max_io_buf_bytes: u32) -> anyhow::Result<u32> {
        // A `dev_id` of `u32::MAX`, which the kernel reads as -1, tells it to
        // choose the device number. The command and its `info` payload must
        // give the same `dev_id`.
        let mut info = UblksrvCtrlDevInfo {
            nr_hw_queues: 1,
            queue_depth,
            max_io_buf_bytes,
            dev_id: u32::MAX,
            flags: UBLK_F_USER_COPY,
            ..Default::default()
        };
        let command = UblksrvCtrlCmd {
            dev_id: u32::MAX,
            queue_id: u16::MAX,
            len: std::mem::size_of_val(&info) as u16,
            addr: std::ptr::from_mut(&mut info) as u64,
            ..Default::default()
        };

        self.issue(UBLK_U_CMD_ADD_DEV, &command).context(
            "ublk ADD_DEV was refused. These devices are privileged, so the usual cause is a \
             missing CAP_SYS_ADMIN",
        )?;

        // The kernel writes the number it chose back into `info`.
        tracing::debug!(dev_id = info.dev_id, "added a ublk device");

        Ok(info.dev_id)
    }

    pub fn set_params(&self, dev_id: u32, params: &UblkParams) -> anyhow::Result<()> {
        self.issue(
            UBLK_U_CMD_SET_PARAMS,
            &UblksrvCtrlCmd {
                dev_id,
                queue_id: u16::MAX,
                len: std::mem::size_of_val(params) as u16,
                addr: std::ptr::from_ref(params) as u64,
                ..Default::default()
            },
        )?;
        Ok(())
    }

    /// Create `/dev/ublkbN`. This call blocks until each tag of the queue has a
    /// fetch in flight, so call it from a thread other than the queue's owner
    /// thread.
    pub fn start_dev(&self, dev_id: u32) -> anyhow::Result<()> {
        self.issue(
            UBLK_U_CMD_START_DEV,
            &UblksrvCtrlCmd {
                dev_id,
                queue_id: u16::MAX,
                // The kernel records this as the PID of the serving process,
                // and rejects a value that is not positive.
                data: [std::process::id() as u64],
                ..Default::default()
            },
        )?;
        Ok(())
    }

    /// Remove `/dev/ublkbN`. After all requests in flight complete, the kernel
    /// aborts the fetches of the queue. The owner thread must continue to
    /// complete requests until then, or this call does not return.
    pub fn stop_dev(&self, dev_id: u32) -> anyhow::Result<()> {
        self.issue(
            UBLK_U_CMD_STOP_DEV,
            &UblksrvCtrlCmd {
                dev_id,
                queue_id: u16::MAX,
                ..Default::default()
            },
        )?;
        Ok(())
    }

    /// Remove `/dev/ublkcN`. The kernel can then reuse the device number.
    ///
    /// The kernel waits until all references to the device are released, so
    /// close the character device before this call.
    pub fn del_dev(&self, dev_id: u32) -> anyhow::Result<()> {
        self.issue(
            UBLK_U_CMD_DEL_DEV,
            &UblksrvCtrlCmd {
                dev_id,
                queue_id: u16::MAX,
                ..Default::default()
            },
        )?;
        tracing::debug!(dev_id, "deleted a ublk device");

        Ok(())
    }

    fn issue(&self, cmd_op: u32, command: &UblksrvCtrlCmd) -> std::io::Result<i32> {
        let mut inner = self.inner.lock().unwrap();
        let inner = &mut *inner;

        let mut bytes = [0; 80];
        bytes[..std::mem::size_of::<UblksrvCtrlCmd>()]
            .copy_from_slice(zerocopy::IntoBytes::as_bytes(command));

        let entry = io_uring::opcode::UringCmd80::new(
            io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(&inner.file)),
            cmd_op,
        )
        .cmd(bytes)
        .build();

        // SAFETY: the kernel uses `command` and the memory it addresses only until
        // the command completes, and the wait below does not return before then.
        unsafe { inner.ring.submission().push(&entry) }
            .expect("the control ring holds one command at a time");
        inner.ring.submit_and_wait(1)?;

        let result = inner
            .ring
            .completion()
            .next()
            .expect("submit_and_wait(1) yields one completion")
            .result();

        if result < 0 {
            return Err(std::io::Error::from_raw_os_error(-result));
        }
        Ok(result)
    }
}

/// The request descriptors of one queue, which the kernel shares read-only through
/// an `mmap` of the character device.
pub struct IoDescs {
    /// The first descriptor of the mapping.
    base: *const UblksrvIoDesc,
    /// The length of the mapping, in bytes.
    len: usize,
    /// The number of tags, which is also the number of descriptors in the mapping.
    depth: u16,
}

impl IoDescs {
    pub fn map(cdev: &std::fs::File, q_id: u16, depth: u16) -> std::io::Result<Self> {
        let len = io_desc_map_len(depth);

        // SAFETY: with a null address, the kernel chooses a free range, so the
        // mapping does not replace memory in use. The kernel rejects a length or
        // offset that is not exactly the descriptor array of one queue.
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                std::os::fd::AsRawFd::as_raw_fd(cdev),
                io_desc_map_offset(q_id) as libc::off_t,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self {
            base: base.cast(),
            len,
            depth,
        })
    }

    /// The descriptor of the request that the kernel gave to `tag`.
    pub fn get(&self, tag: u16) -> UblksrvIoDesc {
        assert!(tag < self.depth, "tag {tag} exceeds the queue depth");

        // SAFETY: the mapping holds `depth` descriptors and is alive for this
        // read. The read is volatile because the kernel, not this thread,
        // writes the descriptor, before it completes the fetch that gives the
        // request to `tag`.
        unsafe { std::ptr::read_volatile(self.base.add(tag as usize)) }
    }
}

impl Drop for IoDescs {
    fn drop(&mut self) {
        // SAFETY: `base` and `len` are the mapping that `map` created. `get`
        // returns copies, so no reference into the mapping remains.
        unsafe { libc::munmap(self.base as *mut libc::c_void, self.len) };
    }
}

/// The operation of a request, which is the low 8 bits of `op_flags`. The other
/// bits are request flags.
pub fn io_desc_op(desc: &UblksrvIoDesc) -> u8 {
    (desc.op_flags & 0xff) as u8
}

/// The number of sectors that a request covers.
///
/// The header puts this field in a union with the zone count of a zoned device.
/// This device is not zoned, so the field always holds the sector count.
pub fn io_desc_sectors(desc: &UblksrvIoDesc) -> u32 {
    // SAFETY: both fields of the union are a `__u32`, so each one reads the
    // same initialized bytes.
    unsafe { desc.__bindgen_anon_1.nr_sectors }
}

/// The bytes of a fetch or commit command for `tag` on queue `q_id`. For a
/// commit, `result` is the outcome of the request.
///
/// With `UBLK_F_USER_COPY`, the kernel rejects a command that gives a buffer
/// address, so the union that holds the address stays zero. zerocopy cannot
/// derive `IntoBytes` for that union, so this function writes each field at its
/// offset.
pub fn io_cmd(q_id: u16, tag: u16, result: i32) -> [u8; 16] {
    let mut bytes = [0; std::mem::size_of::<UblksrvIoCmd>()];
    for (offset, field) in [
        (
            std::mem::offset_of!(UblksrvIoCmd, q_id),
            &q_id.to_ne_bytes()[..],
        ),
        (std::mem::offset_of!(UblksrvIoCmd, tag), &tag.to_ne_bytes()),
        (
            std::mem::offset_of!(UblksrvIoCmd, result),
            &result.to_ne_bytes(),
        ),
    ] {
        bytes[offset..offset + field.len()].copy_from_slice(field);
    }
    bytes
}

/// The offset in `/dev/ublkcN` where `pread` and `pwrite` move the data of
/// request `tag` on queue `q_id`.
pub fn io_buf_offset(q_id: u16, tag: u16) -> u64 {
    bindings::UBLKSRV_IO_BUF_OFFSET as u64
        + ((q_id as u64) << bindings::UBLK_QID_OFF)
        + ((tag as u64) << bindings::UBLK_TAG_OFF)
}

/// The byte offset in `/dev/ublkcN` of the descriptor array of queue `q_id`.
/// The kernel reserves space for the maximum queue depth in each queue,
/// whatever depth the device has.
pub fn io_desc_map_offset(q_id: u16) -> u64 {
    q_id as u64
        * bindings::UBLK_MAX_QUEUE_DEPTH as u64
        * std::mem::size_of::<UblksrvIoDesc>() as u64
}

/// The byte length of the descriptor array of a queue with `queue_depth` tags.
/// The kernel rejects an `mmap` of any other length, after both are rounded up
/// to whole pages.
pub fn io_desc_map_len(queue_depth: u16) -> usize {
    queue_depth as usize * std::mem::size_of::<UblksrvIoDesc>()
}

/// The server moves request data itself, with `pread` and `pwrite` on
/// `/dev/ublkcN`. Without this flag, the kernel copies the data to and from a
/// buffer address that each command gives.
pub const UBLK_F_USER_COPY: u64 = bindings::UBLK_F_USER_COPY as u64;

/// The result of a fetch that gives a request to its tag.
pub const UBLK_IO_RES_OK: i32 = bindings::UBLK_IO_RES_OK as i32;
/// The result of a fetch that the kernel aborts. The kernel aborts all fetches
/// after the device stops.
pub use bindings::UBLK_IO_RES_ABORT;

pub const UBLK_IO_OP_READ: u8 = bindings::UBLK_IO_OP_READ as u8;
pub const UBLK_IO_OP_WRITE: u8 = bindings::UBLK_IO_OP_WRITE as u8;
pub const UBLK_IO_OP_DISCARD: u8 = bindings::UBLK_IO_OP_DISCARD as u8;
pub const UBLK_IO_OP_WRITE_ZEROES: u8 = bindings::UBLK_IO_OP_WRITE_ZEROES as u8;

pub use bindings::{
    UBLK_IOCTL_ADD_DEV as UBLK_U_CMD_ADD_DEV,
    UBLK_IOCTL_COMMIT_AND_FETCH_REQ as UBLK_U_IO_COMMIT_AND_FETCH_REQ,
    UBLK_IOCTL_DEL_DEV as UBLK_U_CMD_DEL_DEV, UBLK_IOCTL_FETCH_REQ as UBLK_U_IO_FETCH_REQ,
    UBLK_IOCTL_GET_FEATURES as UBLK_U_CMD_GET_FEATURES,
    UBLK_IOCTL_SET_PARAMS as UBLK_U_CMD_SET_PARAMS, UBLK_IOCTL_START_DEV as UBLK_U_CMD_START_DEV,
    UBLK_IOCTL_STOP_DEV as UBLK_U_CMD_STOP_DEV, UBLK_PARAM_TYPE_BASIC, UBLK_PARAM_TYPE_DISCARD,
    ublk_param_basic as UblkParamBasic, ublk_param_discard as UblkParamDiscard,
    ublk_params as UblkParams, ublksrv_ctrl_cmd as UblksrvCtrlCmd,
    ublksrv_ctrl_dev_info as UblksrvCtrlDevInfo, ublksrv_io_cmd as UblksrvIoCmd,
    ublksrv_io_desc as UblksrvIoDesc,
};

/// Generated by bindgen from the vendored `ublk_cmd.h`, with compile-time checks of
/// the size, alignment, and field offsets of each type. This crate uses fields only
/// by name, so a newer header cannot silently move data. If a newer header removes
/// a field that this crate uses, the build fails.
mod bindings {
    #![allow(dead_code, non_camel_case_types, non_upper_case_globals)]
    include!(concat!(env!("OUT_DIR"), "/ublk_cmd.rs"));
}
