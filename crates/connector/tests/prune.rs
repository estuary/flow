#![cfg(target_os = "linux")]

use std::os::unix::fs::PermissionsExt;
use std::process::Command;

const PRUNE: &str = env!("CARGO_BIN_EXE_flow-connector-vmm-prune");

#[test]
fn command_scans_an_empty_directory_and_reports_incomplete_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let engine = root.path().join("podman");
    std::fs::write(&engine, "#!/bin/sh\necho \"$*\" >> \"$CALLS\"\n").unwrap();
    std::fs::set_permissions(&engine, std::fs::Permissions::from_mode(0o700)).unwrap();
    let calls = root.path().join("calls");
    let mut cmd = Command::new(PRUNE);
    cmd.env("CONNECTOR_VMM_STATE_DIR", &state)
        .env("CONNECTOR_VMM_PODMAN", &engine)
        .env("CONNECTOR_VMM_DISK_MIB", "invalid")
        .env("CALLS", &calls);

    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o755)).unwrap();
    let output = cmd.output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("0700"));
    assert!(!calls.exists());

    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = cmd.output().unwrap();
    assert!(output.status.success() && output.stderr.is_empty() && output.stdout.is_empty());
    let listed = std::fs::read_to_string(&calls).unwrap();
    let label = format!(
        "--filter label=dev.estuary.vmm-state-dir={}",
        state.display()
    );
    assert_eq!(
        listed,
        format!(
            "ps --all {label} --format {{{{.Names}}}}\nnetwork ls {label} --format {{{{.Name}}}}\nvolume ls {label} --format {{{{.Name}}}}\n"
        )
    );

    // Launch also uses the default executable for an empty Podman setting.
    cmd.env("PATH", root.path()).env("CONNECTOR_VMM_PODMAN", "");
    assert!(cmd.status().unwrap().success());

    let record = state.join("fv_0000000000000001.owner");
    std::fs::write(&record, "{\"version\":2}\n").unwrap();
    cmd.env("CONNECTOR_VMM_STATE_DIR", "/nonexistent/state")
        .env("CONNECTOR_VMM_PODMAN", "/nonexistent/podman")
        .arg("--state-dir")
        .arg(&state)
        .arg("--podman")
        .arg(&engine);
    let output = cmd.output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains(record.to_str().unwrap()) && stderr.contains("version 2"),
        "{stderr}"
    );
    assert!(record.exists());
    assert_eq!(std::fs::read_to_string(&calls).unwrap(), listed.repeat(3));
}

#[test]
fn another_prune_skips_the_locked_directory() {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let directory = std::fs::File::open(root.path()).unwrap();
    directory.lock().unwrap();
    let output = Command::new(PRUNE)
        .arg("--state-dir")
        .arg(root.path())
        .args(["--podman", "/nonexistent/podman"])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(output.stderr, b"prune already running\n");
}
