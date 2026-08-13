//! The host-wide `ublk` control plane.
//!
//! Every command is a `uring_cmd` against `/dev/ublk-control`, issued one at a
//! time and awaited: the control plane runs at session boundaries and is not on
//! any data path. `UBLK_U_CMD_START_DEV` in particular blocks in the kernel
//! until the queue's fetches are in flight, so it must be issued from a thread
//! which is not the owner serving that queue.

use super::sys;

const CONTROL_PATH: &str = "/dev/ublk-control";
const UBLKS_MAX_PATH: &str = "/sys/module/ublk_drv/parameters/ublks_max";

pub struct Control {
    inner: std::sync::Mutex<Inner>,
    ublks_max: Option<u32>,
    live: std::sync::atomic::AtomicU32,
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
            .map_err(|err| {
                anyhow::anyhow!(
                    "opening {CONTROL_PATH}: {err}. \
                     Load the module with `modprobe ublk_drv`, and grant this process \
                     ownership of the node if it does not run as root"
                )
            })?;

        // `ublksrv_ctrl_cmd` does not fit the sixteen command bytes of an
        // ordinary SQE, so the control ring is built with 128-byte entries.
        let ring =
            io_uring::IoUring::<io_uring::squeue::Entry128, io_uring::cqueue::Entry>::builder()
                .build(4)?;

        let ublks_max = read_ublks_max();
        tracing::debug!(?ublks_max, "opened the ublk control device");

        Ok(Self {
            inner: std::sync::Mutex::new(Inner { file, ring }),
            ublks_max,
            live: std::sync::atomic::AtomicU32::new(0),
        })
    }

    /// The kernel's `ublks_max`, or `None` if it could not be read.
    pub fn ublks_max(&self) -> Option<u32> {
        self.ublks_max
    }

    /// Create a device, letting the kernel choose its number.
    pub fn add_dev(
        &self,
        queue_depth: u16,
        max_io_buf_bytes: u32,
    ) -> anyhow::Result<sys::UblksrvCtrlDevInfo> {
        // A device number of -1 asks the kernel to pick, and the header must
        // repeat whatever the payload says.
        let mut info = sys::UblksrvCtrlDevInfo {
            nr_hw_queues: 1,
            queue_depth,
            max_io_buf_bytes,
            dev_id: u32::MAX,
            flags: sys::UBLK_F_USER_COPY,
            ..Default::default()
        };
        let command = sys::UblksrvCtrlCmd {
            dev_id: u32::MAX,
            queue_id: u16::MAX,
            len: std::mem::size_of_val(&info) as u16,
            addr: std::ptr::from_mut(&mut info) as u64,
            ..Default::default()
        };

        self.issue(sys::UBLK_U_CMD_ADD_DEV, &command)
            .map_err(|err| self.explain_add_dev(err))?;

        // The kernel masks out flags it does not implement, so the values it
        // copies back are the negotiated feature set.
        anyhow::ensure!(
            info.flags & sys::UBLK_F_USER_COPY != 0,
            "this kernel's ublk_drv does not support UBLK_F_USER_COPY",
        );
        anyhow::ensure!(
            info.flags & sys::UBLK_F_CMD_IOCTL_ENCODE != 0,
            "this kernel's ublk_drv does not accept ioctl-encoded commands",
        );
        anyhow::ensure!(
            info.dev_id != u32::MAX,
            "ublk_drv did not report the device number it chose",
        );

        let live = self.live.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        tracing::debug!(dev_id = info.dev_id, live, ?self.ublks_max, "added a ublk device");

        Ok(info)
    }

    pub fn set_params(&self, dev_id: u32, params: &sys::UblkParams) -> anyhow::Result<()> {
        self.issue(
            sys::UBLK_U_CMD_SET_PARAMS,
            &sys::UblksrvCtrlCmd {
                dev_id,
                queue_id: u16::MAX,
                len: std::mem::size_of_val(params) as u16,
                addr: std::ptr::from_ref(params) as u64,
                ..Default::default()
            },
        )?;
        Ok(())
    }

    /// Expose `/dev/ublkbN`. This blocks until every tag of the device's queue
    /// has a fetch in flight.
    pub fn start_dev(&self, dev_id: u32) -> anyhow::Result<()> {
        self.issue(
            sys::UBLK_U_CMD_START_DEV,
            &sys::UblksrvCtrlCmd {
                dev_id,
                queue_id: u16::MAX,
                // The kernel records this as the serving process, and rejects a
                // non-positive value.
                data: [std::process::id() as u64],
                ..Default::default()
            },
        )?;
        Ok(())
    }

    /// Remove `/dev/ublkbN` and abort the queue's outstanding fetches.
    pub fn stop_dev(&self, dev_id: u32) -> anyhow::Result<()> {
        self.issue(
            sys::UBLK_U_CMD_STOP_DEV,
            &sys::UblksrvCtrlCmd {
                dev_id,
                queue_id: u16::MAX,
                ..Default::default()
            },
        )?;
        Ok(())
    }

    /// Remove `/dev/ublkcN`, after which the kernel may reuse the number.
    ///
    /// The kernel waits for every reference to the device to be dropped, so the
    /// character device must already be closed.
    pub fn del_dev(&self, dev_id: u32) -> anyhow::Result<()> {
        self.issue(
            sys::UBLK_U_CMD_DEL_DEV,
            &sys::UblksrvCtrlCmd {
                dev_id,
                queue_id: u16::MAX,
                ..Default::default()
            },
        )?;
        let live = self.live.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
        tracing::debug!(dev_id, live, "deleted a ublk device");

        Ok(())
    }

    fn issue(&self, cmd_op: u32, command: &sys::UblksrvCtrlCmd) -> std::io::Result<i32> {
        let mut inner = self.inner.lock().unwrap();
        let inner = &mut *inner;

        // SAFETY: `UblksrvCtrlCmd` is `repr(C)` and its 32 bytes are fully
        // occupied by its fields, so the copy reads no padding.
        let bytes = unsafe { sys::cmd_bytes::<_, 80>(command) };

        let entry = io_uring::opcode::UringCmd80::new(
            io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(&inner.file)),
            cmd_op,
        )
        .cmd(bytes)
        .build();

        {
            // SAFETY: `command` and whatever it addresses outlive the wait
            // below, which does not return until the kernel is done with them.
            unsafe { inner.ring.submission().push(&entry) }
                .expect("the control ring holds one command at a time");
        }
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

    /// `ublks_max` counts unprivileged devices only, so it never binds these.
    /// It is still the first thing an operator reaches for when `ADD_DEV` is
    /// refused, so the error names it alongside the live device count.
    fn explain_add_dev(&self, err: std::io::Error) -> anyhow::Error {
        anyhow::anyhow!(
            "ublk ADD_DEV was refused ({err}) with {} device(s) live. These \
             devices are privileged, so the usual cause is a missing \
             CAP_SYS_ADMIN rather than {UBLKS_MAX_PATH}, which counts \
             unprivileged devices only and stands at {}. Root can raise that \
             limit live with `echo N > {UBLKS_MAX_PATH}`.",
            self.live.load(std::sync::atomic::Ordering::Relaxed),
            self.ublks_max
                .map_or_else(|| "an unreadable value".to_string(), |max| max.to_string()),
        )
    }
}

fn read_ublks_max() -> Option<u32> {
    let text = std::fs::read_to_string(UBLKS_MAX_PATH).ok()?;
    text.trim().parse().ok()
}
