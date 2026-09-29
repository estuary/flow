//! The control channel to `fixtures/probes.py`: one JSON request per line
//! over `init.sock`, answered by the guest over the vsock port the VMM maps
//! there. Dialed unprivileged, which is what the VMM's `umask(0)` is for.

use std::io::{BufRead, Write};
use std::os::unix::net::UnixStream;
use std::time::Duration;

pub struct Guest {
    reader: std::io::BufReader<UnixStream>,
    writer: UnixStream,
}

pub fn dial(vmm: &crate::run::Vmm) -> Guest {
    let path = crate::run::socket_path(vmm);
    let stream = UnixStream::connect(&path)
        .unwrap_or_else(|e| panic!("dialing {} unprivileged: {e}", path.display()));
    // Longer than any single probe waits, so a stuck guest fails the call.
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .expect("setting a read timeout");

    Guest {
        reader: std::io::BufReader::new(stream.try_clone().expect("cloning the stream")),
        writer: stream,
    }
}

/// Run one probe and return its result. `arguments` is a JSON object.
pub fn call(
    guest: &mut Guest,
    probe: &str,
    op: &str,
    arguments: serde_json::Value,
) -> serde_json::Value {
    call_timed(guest, probe, op, arguments).0
}

/// As `call`, with how long the probe took as the guest measured it.
pub fn call_timed(
    guest: &mut Guest,
    probe: &str,
    op: &str,
    arguments: serde_json::Value,
) -> (serde_json::Value, Duration) {
    let mut request = arguments;
    request["probe"] = probe.into();
    request["op"] = op.into();

    let mut line = serde_json::to_vec(&request).expect("a request serializes");
    line.push(b'\n');
    guest
        .writer
        .write_all(&line)
        .unwrap_or_else(|e| panic!("{probe}: sending to the guest: {e}"));

    let mut response = String::new();
    let read = guest
        .reader
        .read_line(&mut response)
        .unwrap_or_else(|e| panic!("{probe}: reading from the guest: {e}"));
    assert!(read > 0, "{probe}: the guest closed the control channel");

    let mut response: serde_json::Value = serde_json::from_str(&response)
        .unwrap_or_else(|e| panic!("{probe}: the guest answered {response:?}: {e}"));
    assert_eq!(response["probe"], probe, "the guest answered another probe");
    let elapsed = Duration::from_millis(response["ms"].as_u64().unwrap_or_default());
    (response["result"].take(), elapsed)
}

/// Whether the guest's side of the channel has gone, within `timeout`: EOF or
/// a reset both mean the connection it was holding no longer exists.
pub fn closed_within(guest: &mut Guest, timeout: Duration) -> bool {
    guest
        .writer
        .set_read_timeout(Some(timeout))
        .expect("setting a read timeout");
    let mut rest = String::new();
    match guest.reader.read_line(&mut rest) {
        Ok(0) => true,
        Ok(_) => false,
        Err(error) => !matches!(
            error.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        ),
    }
}

/// Run one probe on the host, as root, inside the network namespace of process
/// `pid`: a VMM container's, say. That is strictly more than the VMM itself
/// may do there, which is the point when the question is what the host lets
/// out of that namespace. `probes.py once` answers on its stderr.
pub fn once_in(pid: u32, probe: &str, op: &str, arguments: serde_json::Value) -> serde_json::Value {
    let mut request = arguments;
    request["probe"] = probe.into();
    request["op"] = op.into();

    let script = crate::run::fixture("probes.py");
    let pid = pid.to_string();
    let request = request.to_string();
    let output = crate::run::sudo_output(
        &[
            "nsenter",
            "-t",
            &pid,
            "-n",
            "python3",
            script.to_str().expect("a UTF-8 path"),
            "once",
            &request,
        ],
        None,
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{probe} in {pid}: {stderr}");

    let line = stderr.lines().last().unwrap_or_default();
    let mut response: serde_json::Value = serde_json::from_str(line)
        .unwrap_or_else(|e| panic!("{probe} in {pid} answered {line:?}: {e}"));
    response["result"].take()
}
