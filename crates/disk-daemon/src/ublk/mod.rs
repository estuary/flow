//! The Linux `ublk` block-device transport.
//!
//! One ephemeral device serves one disk. Two nodes make up that device.
//! `/dev/ublkcN` is the character device this crate serves, and request data moves
//! through it under `UBLK_F_USER_COPY`. `/dev/ublkbN` is the block device the
//! filesystem is mounted from. The kernel picks the number, so both nodes exist
//! only for a session and never appear in durable state.
//!
//! Devices are privileged. `UBLK_F_UNPRIVILEGED_DEV` would buy nothing. ext4 lacks
//! `FS_USERNS_MOUNT`, so a mount needs real `CAP_SYS_ADMIN` whatever the device's
//! flavor.

pub mod control;
pub mod sys;

pub use control::Control;

/// Devices are single-queue, so each disk has exactly one owner. Concurrency
/// comes from queue depth instead, and the block path has ample headroom over
/// the rate at which a delta can be appended.
pub const QUEUE_ID: u16 = 0;

/// Depth of that queue, which the daemon configures per device.
pub const QUEUE_DEPTH: u16 = 32;

/// Largest request the device accepts. Each request allocates its own buffer, so
/// an owner's transient memory is at most `QUEUE_DEPTH` times this.
pub const MAX_IO_BUF_BYTES: u32 = 512 * 1024;

pub fn char_path(dev_id: u32) -> std::path::PathBuf {
    format!("/dev/ublkc{dev_id}").into()
}

pub fn block_path(dev_id: u32) -> std::path::PathBuf {
    format!("/dev/ublkb{dev_id}").into()
}

/// Queue limits for a device of `blocks` × [`crate::BLOCK_SIZE`].
///
/// `attrs` leaves `UBLK_ATTR_VOLATILE_CACHE` clear, so the kernel issues no flush
/// or force-unit-access requests. This crate implements neither. The local image is
/// disposable, and durability belongs to the journal, so a delta captures exactly
/// the writes which completed.
pub fn params(blocks: u32) -> sys::UblkParams {
    let sectors = blocks as u64 * crate::BLOCK_SIZE as u64 / sys::SECTOR_SIZE;
    let block_shift = crate::BLOCK_SIZE.trailing_zeros() as u8;

    sys::UblkParams {
        len: std::mem::size_of::<sys::UblkParams>() as u32,
        types: sys::UBLK_PARAM_TYPE_BASIC | sys::UBLK_PARAM_TYPE_DISCARD,
        basic: sys::UblkParamBasic {
            attrs: 0,
            logical_bs_shift: block_shift,
            physical_bs_shift: block_shift,
            io_opt_shift: block_shift,
            io_min_shift: block_shift,
            max_sectors: MAX_IO_BUF_BYTES / sys::SECTOR_SIZE as u32,
            chunk_sectors: 0,
            dev_sectors: sectors,
            virt_boundary_mask: 0,
        },
        discard: sys::UblkParamDiscard {
            discard_alignment: 0,
            // Aligned to the tracking granularity, so a discard clears whole
            // allocated bits.
            discard_granularity: crate::BLOCK_SIZE,
            max_discard_sectors: sectors.try_into().unwrap_or(u32::MAX),
            max_write_zeroes_sectors: sectors.try_into().unwrap_or(u32::MAX),
            // ublk carries one range per request, and the kernel rejects params
            // which advertise more.
            max_discard_segments: 1,
            reserved0: 0,
        },
        ..Default::default()
    }
}

/// The kernel's array of pending requests for one queue, mapped read-only from
/// the character device.
pub struct IoDescs {
    base: *const sys::UblksrvIoDesc,
    len: usize,
    depth: u16,
}

// SAFETY: the mapping is read-only, and it lives until `IoDescs` is dropped. It is
// therefore sound to move it to the owner thread which reads it.
unsafe impl Send for IoDescs {}

impl IoDescs {
    pub fn map(cdev: &std::fs::File, q_id: u16, depth: u16) -> std::io::Result<Self> {
        let len = sys::io_desc_map_len(depth);

        // SAFETY: a null hint lets the kernel choose the address. The kernel also
        // rejects any length or offset which is not exactly one queue's descriptor
        // array.
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED | libc::MAP_POPULATE,
                std::os::fd::AsRawFd::as_raw_fd(cdev),
                sys::io_desc_map_offset(q_id) as libc::off_t,
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

    /// The request the kernel handed back at `tag`.
    pub fn get(&self, tag: u16) -> sys::UblksrvIoDesc {
        assert!(tag < self.depth, "tag {tag} exceeds the queue depth");

        // SAFETY: the mapping covers `depth` descriptors and outlives this read.
        // The read is volatile because the kernel writes the descriptor without
        // this thread's knowledge, before it completes the fetch which hands the
        // tag over.
        unsafe { std::ptr::read_volatile(self.base.add(tag as usize)) }
    }
}

impl Drop for IoDescs {
    fn drop(&mut self) {
        // SAFETY: `base` and `len` are the mapping this type created. No reference
        // into it outlives the drop.
        unsafe { libc::munmap(self.base as *mut libc::c_void, self.len) };
    }
}
