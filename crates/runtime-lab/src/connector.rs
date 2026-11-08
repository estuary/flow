//! The serving loop shared by the lab's reference connectors (`src/bin/`), and
//! by any connector forked from them, which then writes only its `handle`.

use anyhow::Context;
use std::io::{Read, Write};

/// Environment variable naming the cgroup of the host's connectors. The
/// controller sets it on each reactor, whose `local:` connectors inherit it.
pub const CGROUP_ENV: &str = "RUNTIME_LAB_CONNECTORS_CGROUP";

/// Serve the connector protocol over stdin and stdout with the protobuf codec
/// (so its `local:` endpoint must set `protobuf: true`), mapping each request
/// to at most one response. Responses to a whole read are written at once.
///
/// It first moves the process into its host's connectors cgroup, before any
/// work, so its usage is accounted and limited apart from the reactor's:
/// connectors are children of the reactor, and would otherwise run (and be
/// throttled) within it.
///
/// An error ends the connector, logged as `{name} failed`.
pub fn serve<Request, Response>(
    name: &str,
    mut handle: impl FnMut(Request) -> anyhow::Result<Option<Response>>,
) -> std::process::ExitCode
where
    Request: prost::Message + for<'de> serde::Deserialize<'de> + Default,
    Response: prost::Message + serde::Serialize,
{
    const CODEC: connector_init::Codec = connector_init::Codec::Proto;

    let result = enter_cgroup().and_then(|()| {
        let mut stdin = std::io::stdin().lock();
        let mut stdout = std::io::stdout().lock();

        let mut buffer = Vec::with_capacity(1 << 20);
        let mut chunk = vec![0u8; 1 << 20];
        let mut out = Vec::new();

        loop {
            let n = stdin.read(&mut chunk).context("reading stdin")?;
            if n == 0 {
                return Ok(());
            }
            buffer.extend_from_slice(&chunk[..n]);

            for request in CODEC.decode::<Request>(&mut buffer)? {
                if let Some(response) = handle(request)? {
                    CODEC.encode(&response, &mut out);
                }
            }
            if !out.is_empty() {
                stdout.write_all(&out)?;
                stdout.flush()?;
                out.clear();
            }
        }
    });

    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            let line = serde_json::json!({"level": "error", "msg": format!("{name} failed"), "fields": {"error": format!("{err:#}")}});
            eprintln!("{line}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// Move this process into its host's connectors cgroup, as `serve` does first.
/// A connector with a serving loop of its own calls it before any work.
/// It's a no-op outside of the lab (as when `flowctl` validates the catalog).
pub fn enter_cgroup() -> anyhow::Result<()> {
    let Some(cgroup) = std::env::var_os(CGROUP_ENV) else {
        return Ok(());
    };
    let procs = std::path::Path::new(&cgroup).join("cgroup.procs");
    std::fs::write(&procs, "0")
        .with_context(|| format!("joining connectors cgroup {}", procs.display()))
}
