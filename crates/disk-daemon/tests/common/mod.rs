//! What the test binaries share. This covers the journals they must create, the
//! host prerequisites the privileged ones need, and the leak checks they end
//! with.
//!
//! Each binary includes this module and uses part of it, so unused items here
//! are the rule rather than a smell.
#![allow(dead_code)]

use proto_gazette::broker;

/// Create `journal` unless it exists, as the caller of a daemon does at task
/// activation. The daemon itself never creates one, so nothing else here will.
///
/// The insert is conditioned on the journal's absence, and a lost race means
/// another writer created it, which is the outcome this wanted.
pub async fn create_journal(
    client: &gazette::journal::Client,
    spec: broker::JournalSpec,
) -> anyhow::Result<()> {
    let request = broker::ApplyRequest {
        changes: vec![broker::apply_request::Change {
            expect_mod_revision: 0,
            upsert: Some(spec),
            delete: String::new(),
        }],
    };

    match client.apply(request).await {
        Ok(_response) => Ok(()),
        Err(gazette::Error::BrokerStatus(broker::Status::EtcdTransactionFailed)) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Spec of a disk journal stored in a test broker's `file:///` root, which the
/// daemon accepts as recoverable.
///
/// `codec` is a case's, because the daemon refuses one it cannot decompress.
/// `refresh_interval_seconds` is too: it decides whether a broker can still
/// serve a fragment a horizon case deleted from the store.
pub fn journal_spec(
    journal: &str,
    codec: broker::CompressionCodec,
    refresh_interval_seconds: u64,
) -> broker::JournalSpec {
    broker::JournalSpec {
        name: journal.to_string(),
        replication: 1,
        labels: None,
        fragment: Some(broker::journal_spec::Fragment {
            length: 1 << 20,
            compression_codec: codec as i32,
            stores: vec!["file:///".to_string()],
            refresh_interval: Some(std::time::Duration::from_secs(refresh_interval_seconds).into()),
            // Long enough that only fragment length closes one, so a case decides
            // where its fragment boundaries fall.
            flush_interval: Some(std::time::Duration::from_secs(48 * 3600).into()),
            retention: None,
            path_postfix_template: String::new(),
        }),
        flags: broker::journal_spec::Flag::ORdwr as u32,
        max_append_rate: 1 << 22,
        suspend: None,
    }
}

/// Fail with what to do about it, rather than skip. A machine which cannot serve a
/// device then says so.
pub fn check_prerequisites() {
    assert!(
        std::path::Path::new("/sys/module/ublk_drv").exists(),
        "ublk_drv is not loaded, so no block device can be served. \
         Load it with `sudo modprobe ublk_drv`.",
    );
    assert!(
        std::path::Path::new("/dev/ublk-control").exists(),
        "/dev/ublk-control is absent though ublk_drv is loaded, so this kernel's \
         module was built without the control device.",
    );

    let sudo = std::process::Command::new("sudo")
        .args(["-n", "true"])
        .output()
        .expect("spawning sudo");

    assert!(
        sudo.status.success(),
        "passwordless sudo is required, because these tests serve real ublk \
         devices from `sudo -n` child processes: {}",
        String::from_utf8_lossy(&sudo.stderr),
    );
}

/// No device node and no `/sys/block` entry outlives the test which made it. A
/// nextest test group serializes these binaries, so each one sees only its own.
///
/// This waits rather than asserting outright. `devtmpfs` unlinks a node slightly
/// after the command which removed its device returns, exactly as it creates one
/// slightly after the command which added it.
pub fn assert_no_leaked_devices() {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);

    loop {
        let nodes = entries("/dev", |name| {
            name.starts_with("ublk") && name != "ublk-control"
        });
        let blocks = entries("/sys/block", |name| name.starts_with("ublkb"));

        if nodes.is_empty() && blocks.is_empty() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "leaked device nodes {nodes:?} and block devices {blocks:?}",
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// No mount of `dir` or of a served device outlives the test which made it.
pub fn assert_no_mounts_under(dir: &str) {
    let mounts = std::fs::read_to_string("/proc/mounts").unwrap();

    for line in mounts.lines() {
        assert!(
            !line.contains(dir) && !line.contains("/dev/ublkb"),
            "leaked mount: {line}",
        );
    }
}

fn entries(dir: &str, keep: impl Fn(&str) -> bool) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| Some(entry.ok()?.file_name().to_string_lossy().into_owned()))
        .filter(|name| keep(name))
        .collect()
}
