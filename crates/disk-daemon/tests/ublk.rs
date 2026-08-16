//! End-to-end tests over a real `ublk` device.
//!
//! Serving a device and mounting a filesystem need `CAP_SYS_ADMIN`, so each test
//! runs a scenario in the `disk-daemon-scenario` binary under `sudo -n` and
//! asserts against the JSON it reports. Cargo itself never runs as root, which
//! keeps the target directory the user's. These tests are in the default run:
//! a missing prerequisite fails them rather than skipping them, and a nextest
//! test group serializes them so their leak checks see only their own devices.

mod common;

/// Run `name` in the scenario helper and return its report.
fn scenario(name: &str) -> serde_json::Value {
    common::check_prerequisites();

    let output = std::process::Command::new("sudo")
        .args(["-n", env!("CARGO_BIN_EXE_disk-daemon-scenario"), name])
        .output()
        .expect("spawning sudo");

    assert!(
        output.status.success(),
        "scenario {name} failed ({}):\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|err| {
        panic!(
            "scenario {name} did not report JSON ({err}): {}",
            String::from_utf8_lossy(&output.stdout),
        )
    });

    assert_no_leaks(&report);
    report
}

/// A scenario ends with no device node, no `/sys/block` entry, and no mount of
/// its own making.
fn assert_no_leaks(report: &serde_json::Value) {
    common::assert_no_leaked_devices();
    common::assert_no_mounts_under(
        report["dir"]
            .as_str()
            .expect("every report names its directory"),
    );
}

/// The captured chunk stream, replayed onto a second image, reproduces the
/// served image byte for byte and block for block. This is what makes the local
/// image disposable.
fn assert_replays_identically(report: &serde_json::Value) {
    assert_eq!(
        report["image"], report["replay"],
        "the replayed image differs from the one served",
    );
    assert!(report["image"]["allocated"].as_u64().unwrap() > 0);
}

#[test]
fn test_device_lifecycle() {
    let report = scenario("lifecycle");

    assert_eq!(
        report["served"],
        serde_json::json!({"char": true, "block": true, "sys_block": true}),
    );
    assert_eq!(
        report["torn_down"],
        serde_json::json!({"char": false, "block": false, "sys_block": false}),
    );

    // A fresh image is all holes, and reads and the kernel's partition scan
    // mutate nothing.
    assert_eq!(report["reads_zeroes"], true);
    assert_eq!(report["mutations"], 0);
    assert_eq!(report["image"]["allocated"], 0);
    assert_eq!(report["image"]["extents"], 0);

    // The kernel's ceiling is reported rather than enforced here, and it counts
    // unprivileged devices only, so it does not bind these.
    assert!(report["ublks_max"].is_number(), "{report}");
}

#[test]
fn test_ext4_format_mount_and_file_io() {
    let report = scenario("ext4");

    assert_eq!(
        report["mismatched"],
        serde_json::json!([]),
        "files read back after a remount differ from what was written",
    );
    assert!(report["mutations"].as_u64().unwrap() > 0, "{report}");
    assert!(report["chunks"].as_u64().unwrap() > 0, "{report}");
}

#[test]
fn test_captured_stream_replays_to_an_identical_image() {
    assert_replays_identically(&scenario("ext4"));
}

#[test]
fn test_discards_become_punches_which_clear_allocated_bits() {
    let report = scenario("discard");
    let file_blocks = report["file_blocks"].as_u64().unwrap();

    let peak = report["peak_allocated"].as_u64().unwrap();
    let punched = report["punched_blocks"].as_u64().unwrap();
    let allocated = report["image"]["allocated"].as_u64().unwrap();

    assert!(report["punch_chunks"].as_u64().unwrap() > 0, "{report}");
    assert!(punched >= file_blocks, "{report}");
    assert!(peak >= file_blocks, "{report}");
    assert!(allocated * 4 < peak, "{report}");

    assert_replays_identically(&report);
}

/// The horizon invariant, at the block level: what a delta copies is rationed
/// by what it changed, blocks the device rewrites cost no copy at all, and the
/// mutations from the horizon onward rebuild the whole disk by themselves.
#[test]
fn test_a_horizon_discharges_and_bounds_recovery() {
    let report = scenario("horizon");

    let cold = report["cold_blocks"].as_u64().unwrap();
    let hot = report["hot_blocks"].as_u64().unwrap();
    let deltas = report["deltas"].as_array().unwrap();

    // A range within the configured minimum opens nothing; one beyond it opens
    // a horizon over every allocated block. The writes which allocated them are
    // before the horizon, so none of them is in the replay compared below.
    assert_eq!(report["declined"], serde_json::Value::Null, "{report}");
    assert_eq!(report["opened"], serde_json::json!(cold), "{report}");
    assert_eq!(report["filled"], serde_json::json!(cold / hot), "{report}");

    // The first delta writes nothing at all, which is a disk nobody is using:
    // an open horizon then publishes nothing and the journal does not grow.
    assert_eq!(deltas[0]["changed"], 0, "{report}");
    assert_eq!(deltas[0]["copied"], 0, "{report}");
    assert_eq!(deltas[0]["pending"], serde_json::json!(cold), "{report}");

    let mut copied = 0;
    for delta in deltas {
        // Write amplification is bounded per delta at one plus the copy ratio,
        // which is a half here.
        assert!(
            2 * delta["copied"].as_u64().unwrap() <= delta["changed"].as_u64().unwrap(),
            "{delta} exceeded its copy budget",
        );
        copied += delta["copied"].as_u64().unwrap();
    }

    // The horizon is discharged, and the hot blocks cost it nothing: the disk's
    // own rewrites of them are what discharged those.
    assert_eq!(deltas.last().unwrap()["pending"], 0, "{report}");
    assert_eq!(copied, (cold - hot) * 4096, "{report}");

    assert_replays_identically(&report);
}

#[test]
fn test_backpressure_parks_writes() {
    let report = scenario("backpressure");

    let capacity = report["capacity"].as_u64().unwrap();
    let writes = report["writes"].as_u64().unwrap();
    let stalled = report["during_stall"].as_u64().unwrap();

    // A stalled sink parks writes: only what fit the channel completed, and
    // once it drained every write completed, none dropped or errored.
    assert!(stalled <= capacity, "{report}");
    assert!(stalled < writes, "{report}");

    assert_eq!(report["failed"], 0, "{report}");
    assert_eq!(report["completed"], serde_json::json!(writes), "{report}");
    assert_eq!(report["mutations"], serde_json::json!(writes), "{report}");

    assert_replays_identically(&report);
}
