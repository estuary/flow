use crate::{
    config::GuestConfig,
    util::{setup_device_mounts, setup_networking, setup_rootfs, InitError},
};
use nix::unistd::sethostname;

use tracing::{info, trace, warn};

use crate::config::ImageConfig;

// According to https://www.kernel.org/doc/html/latest/admin-guide/kernel-parameters.html
// "parameters with ‘=’ go into init’s environment", so if we want to have init use debug logging
// we should just be able to add rust_log=debug to the kernel cmdline :O
pub async fn init_firecracker(
    image_conf: &mut ImageConfig,
    guest_conf: &GuestConfig,
) -> Result<(), InitError> {
    info!("Starting init");

    trace!("found runtime config: {:?}, {:?}", image_conf, guest_conf);

    setup_rootfs(guest_conf)?;

    setup_device_mounts()?;

    // let (uid, gid, user) = setup_user_group(image_conf)?;

    match sethostname(&guest_conf.hostname) {
        Err(e) => warn!("error setting hostname: {}", e),
        Ok(_) => {}
    };

    // Some programs might prefer this
    std::fs::write("/etc/hostname", guest_conf.hostname.clone()).ok();

    setup_networking(guest_conf).await?;

    Ok(())
}
