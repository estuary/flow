//! Commands to `/dev/ublk-control`, which query the driver's features, and add,
//! configure, start, stop, and delete ublk devices. Each is an io_uring
//! `URING_CMD`, submitted on a private ring and awaited before returning, so
//! commands run one at a time. Each device opens a [`Control`] of its own as it
//! opens, which it holds to add, start, stop, and delete its kernel device.

use super::sys;
use anyhow::Context;

const CONTROL_PATH: &str = "/dev/ublk-control";

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

        // `ublksrv_ctrl_cmd` does not fit the sixteen command bytes of an ordinary
        // SQE, so the control ring uses 128-byte entries.
        let ring =
            io_uring::IoUring::<io_uring::squeue::Entry128, io_uring::cqueue::Entry>::builder()
                .build(4)?;

        let control = Self {
            inner: std::sync::Mutex::new(Inner { file, ring }),
        };

        // Checked once here, so that adding a device need not check what the kernel
        // granted. GET_FEATURES arrived with user copy, in Linux 6.5.
        let mut features = 0u64;
        control
            .issue(
                sys::UBLK_U_CMD_GET_FEATURES,
                &sys::UblksrvCtrlCmd {
                    dev_id: u32::MAX,
                    queue_id: u16::MAX,
                    len: std::mem::size_of_val(&features) as u16,
                    addr: std::ptr::from_mut(&mut features) as u64,
                    ..Default::default()
                },
            )
            .context("ublk GET_FEATURES was refused, so this kernel predates Linux 6.5")?;

        anyhow::ensure!(
            features & sys::UBLK_F_USER_COPY != 0,
            "this kernel's ublk_drv does not support UBLK_F_USER_COPY",
        );
        tracing::debug!("opened the ublk control device");

        Ok(control)
    }

    /// Create a device, letting the kernel choose its number.
    pub fn add_dev(&self, queue_depth: u16, max_io_buf_bytes: u32) -> anyhow::Result<u32> {
        // A device number of -1 asks the kernel to pick one. The header must
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

        self.issue(sys::UBLK_U_CMD_ADD_DEV, &command).context(
            "ublk ADD_DEV was refused. These devices are privileged, so the usual cause is a \
             missing CAP_SYS_ADMIN",
        )?;

        // The kernel copies back the number it chose. `Control::open` confirmed it
        // supports every flag requested here.
        tracing::debug!(dev_id = info.dev_id, "added a ublk device");

        Ok(info.dev_id)
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
    /// has a fetch in flight. Call from a thread other than the queue owner.
    pub fn start_dev(&self, dev_id: u32) -> anyhow::Result<()> {
        self.issue(
            sys::UBLK_U_CMD_START_DEV,
            &sys::UblksrvCtrlCmd {
                dev_id,
                queue_id: u16::MAX,
                // The kernel records this as the serving process. It rejects a
                // value which is not positive.
                data: [std::process::id() as u64],
                ..Default::default()
            },
        )?;
        Ok(())
    }

    /// Remove `/dev/ublkbN`, and abort the queue's fetches once every request in
    /// flight has completed. The server must go on completing them until then,
    /// or this never returns.
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
        tracing::debug!(dev_id, "deleted a ublk device");

        Ok(())
    }

    fn issue(&self, cmd_op: u32, command: &sys::UblksrvCtrlCmd) -> std::io::Result<i32> {
        let mut inner = self.inner.lock().unwrap();
        let inner = &mut *inner;

        // SAFETY: `UblksrvCtrlCmd` is `repr(C)`, and its fields fully occupy its 32
        // bytes. The copy therefore reads no padding.
        let bytes = unsafe { sys::cmd_bytes::<_, 80>(command) };

        let entry = io_uring::opcode::UringCmd80::new(
            io_uring::types::Fd(std::os::fd::AsRawFd::as_raw_fd(&inner.file)),
            cmd_op,
        )
        .cmd(bytes)
        .build();

        // SAFETY: `command` and whatever it addresses outlive the wait below.
        // That wait does not return until the kernel is done with them.
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
