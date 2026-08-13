//! ABI of `include/uapi/linux/ublk_cmd.h`, and the helpers this crate layers
//! on it.
//!
//! `build.rs` generates the definitions from the vendored header, so nothing
//! here is transcribed by hand. The opcodes are the ioctl-encoded `UBLK_U_*`
//! forms; the legacy raw opcodes need `CONFIG_BLKDEV_UBLK_LEGACY_OPCODES`,
//! which current kernels do not enable.
//!
//! This module renames the generated items to Rust casing and widens a few
//! constants to the types their fields use.

/// Generated from the vendored header, with compile-time assertions on the
/// size, alignment, and field offsets of every type. Fields are reached by
/// name, so dropping in a newer header cannot silently move data: a field this
/// crate uses either still exists or fails to compile.
mod bindings {
    #![allow(dead_code, non_camel_case_types, non_upper_case_globals)]
    include!(concat!(env!("OUT_DIR"), "/ublk_cmd.rs"));
}

pub use bindings::{
    UBLK_IOCTL_ADD_DEV as UBLK_U_CMD_ADD_DEV,
    UBLK_IOCTL_COMMIT_AND_FETCH_REQ as UBLK_U_IO_COMMIT_AND_FETCH_REQ,
    UBLK_IOCTL_DEL_DEV as UBLK_U_CMD_DEL_DEV, UBLK_IOCTL_FETCH_REQ as UBLK_U_IO_FETCH_REQ,
    UBLK_IOCTL_SET_PARAMS as UBLK_U_CMD_SET_PARAMS, UBLK_IOCTL_START_DEV as UBLK_U_CMD_START_DEV,
    UBLK_IOCTL_STOP_DEV as UBLK_U_CMD_STOP_DEV, UBLK_PARAM_TYPE_BASIC, UBLK_PARAM_TYPE_DISCARD,
    ublk_param_basic as UblkParamBasic, ublk_param_discard as UblkParamDiscard,
    ublk_params as UblkParams, ublksrv_ctrl_cmd as UblksrvCtrlCmd,
    ublksrv_ctrl_dev_info as UblksrvCtrlDevInfo, ublksrv_io_cmd as UblksrvIoCmd,
    ublksrv_io_desc as UblksrvIoDesc,
};

/// Requests the kernel not copy request data through a mapped per-queue buffer,
/// so data moves by `pread` and `pwrite` against `/dev/ublkcN` instead.
pub const UBLK_F_USER_COPY: u64 = bindings::UBLK_F_USER_COPY as u64;

/// Set by the kernel on every device it creates, and read back as confirmation
/// that it accepts ioctl-encoded opcodes.
pub const UBLK_F_CMD_IOCTL_ENCODE: u64 = bindings::UBLK_F_CMD_IOCTL_ENCODE as u64;

pub const UBLK_IO_OP_READ: u8 = bindings::UBLK_IO_OP_READ as u8;
pub const UBLK_IO_OP_WRITE: u8 = bindings::UBLK_IO_OP_WRITE as u8;
pub const UBLK_IO_OP_DISCARD: u8 = bindings::UBLK_IO_OP_DISCARD as u8;
pub const UBLK_IO_OP_WRITE_ZEROES: u8 = bindings::UBLK_IO_OP_WRITE_ZEROES as u8;

/// Operation of a pending request, which its low eight bits carry.
pub fn io_desc_op(desc: &UblksrvIoDesc) -> u8 {
    (desc.op_flags & 0xff) as u8
}

/// Sectors a pending request covers.
///
/// The header overlays this with the zone count of a zoned device, which this
/// crate does not serve.
pub fn io_desc_sectors(desc: &UblksrvIoDesc) -> u32 {
    // SAFETY: both variants of the union are a `__u32`, so either reads the
    // same initialized bytes.
    unsafe { desc.__bindgen_anon_1.nr_sectors }
}

/// A command for the queue's `uring_cmd` ring.
///
/// `UBLK_F_USER_COPY` requires no buffer address, and the kernel rejects a
/// command which supplies one, so the union the header overlays there is left
/// zero.
pub fn io_cmd(q_id: u16, tag: u16, result: i32) -> UblksrvIoCmd {
    UblksrvIoCmd {
        q_id,
        tag,
        result,
        __bindgen_anon_1: bindings::ublksrv_io_cmd__bindgen_ty_1 { addr: 0 },
    }
}

/// Every ublk request is expressed in 512-byte sectors, whatever the device's
/// logical block size. The kernel keeps this as `SECTOR_SHIFT` in a header
/// which is not uapi, so there is nothing to import.
pub const SECTOR_SIZE: u64 = 512;

/// Offset within `/dev/ublkcN` holding the data of request `tag` on `q_id`.
pub fn io_buf_offset(q_id: u16, tag: u16) -> u64 {
    bindings::UBLKSRV_IO_BUF_OFFSET as u64
        + ((q_id as u64) << bindings::UBLK_QID_OFF)
        + ((tag as u64) << bindings::UBLK_TAG_OFF)
}

/// Byte offset within `/dev/ublkcN` at which queue `q_id`'s array of
/// [`UblksrvIoDesc`] is mapped. The mapping's length must be exactly
/// [`io_desc_map_len`]. Every queue is given room for the largest depth the
/// kernel allows, whatever depth the device was created with.
pub fn io_desc_map_offset(q_id: u16) -> u64 {
    q_id as u64
        * bindings::UBLK_MAX_QUEUE_DEPTH as u64
        * std::mem::size_of::<UblksrvIoDesc>() as u64
}

pub fn io_desc_map_len(queue_depth: u16) -> usize {
    let page = page_size();
    (queue_depth as usize * std::mem::size_of::<UblksrvIoDesc>()).div_ceil(page) * page
}

pub fn page_size() -> usize {
    // SAFETY: sysconf reads no user memory and always answers for _SC_PAGESIZE.
    (unsafe { libc::sysconf(libc::_SC_PAGESIZE) }) as usize
}

/// Reinterpret `value` as the command bytes of a `uring_cmd` SQE.
///
/// # Safety
///
/// `T` must be a `repr(C)` type with no interior padding, since padding bytes
/// would be read uninitialized.
pub unsafe fn cmd_bytes<T: Copy, const N: usize>(value: &T) -> [u8; N] {
    assert!(std::mem::size_of::<T>() <= N);

    let mut bytes = [0u8; N];
    // SAFETY: the assertion above bounds the copy, and `T: Copy` rules out any
    // ownership the byte copy would duplicate.
    unsafe {
        std::ptr::copy_nonoverlapping(
            std::ptr::from_ref(value).cast::<u8>(),
            bytes.as_mut_ptr(),
            std::mem::size_of::<T>(),
        )
    };
    bytes
}
