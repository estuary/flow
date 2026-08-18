//! What the examples share: where the daemon and its brokers are, and the journals a
//! client creates before it opens a disk.
//!
//! Write prose in this directory in Simplified Technical English. Use short
//! sentences and the active voice.
//!
//! Cargo builds one example at a time, so what one uses the other does not.
#![allow(dead_code)]

use anyhow::Context;
use disk_daemon::proto;
use proto_gazette::broker;

/// The socket of a running daemon, and the broker its disks append to.
///
/// The daemon reads these same variable names. Each default is what
/// `examples/demo-services.sh` starts, and `temp_dir` reads `TMPDIR` as that script
/// does, so an example of those services needs no variable.
pub fn config() -> anyhow::Result<(std::path::PathBuf, proto::Broker)> {
    let named = std::env::var_os("UDS_PATH");
    let uds = match &named {
        Some(path) => std::path::PathBuf::from(path),
        None => std::env::temp_dir().join("disk-daemon-demo/disk.sock"),
    };
    anyhow::ensure!(
        uds.exists(),
        "there is no daemon socket at {uds:?}. {}",
        match named.is_some() {
            true =>
                "UDS_PATH names that path, so unset it to use the default of \
                     `examples/demo-services.sh`.",
            false => "Start the services with `examples/demo-services.sh start`.",
        },
    );
    let broker = proto::Broker {
        endpoint: std::env::var("BROKER_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:28080".to_string()),
        credential: std::env::var("BROKER_CREDENTIAL").unwrap_or_default(),
    };

    Ok((uds, broker))
}

/// A Gazette client of `broker`, for an example to manage its own journals.
pub fn journal_client(broker: &proto::Broker) -> anyhow::Result<gazette::journal::Client> {
    let mut metadata = proto_grpc::Metadata::new();

    if !broker.credential.is_empty() {
        metadata = metadata.with_bearer_token(&broker.credential)?;
    }

    Ok(gazette::journal::Client::new(
        broker.endpoint.clone(),
        gazette::journal::Client::new_fragment_client(),
        metadata,
        gazette::Router::new("disk-daemon-example"),
    ))
}

/// A journal-name prefix unique to this run, so no run reuses the disks of another.
pub fn prefix() -> anyhow::Result<String> {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    Ok(format!("acmeCo/example/{seconds}"))
}

/// Create one disk journal. The daemon creates none, so its client does.
///
/// SNAPPY is the codec the design gives for disk journals. Each fragment closes at
/// one megabyte, so an example stores several rather than keeping every record in
/// one broker spool.
pub async fn create_journal(
    journals: &gazette::journal::Client,
    journal: &str,
) -> anyhow::Result<()> {
    journals
        .apply(broker::ApplyRequest {
            changes: vec![broker::apply_request::Change {
                expect_mod_revision: 0,
                upsert: Some(broker::JournalSpec {
                    name: journal.to_string(),
                    replication: 1,
                    fragment: Some(broker::journal_spec::Fragment {
                        length: 1 << 20,
                        compression_codec: broker::CompressionCodec::Snappy as i32,
                        stores: vec!["file:///".to_string()],
                        refresh_interval: Some(std::time::Duration::from_secs(300).into()),
                        ..Default::default()
                    }),
                    flags: broker::journal_spec::Flag::ORdwr as u32,
                    max_append_rate: 1 << 22,
                    ..Default::default()
                }),
                delete: String::new(),
            }],
        })
        .await
        .with_context(|| format!("creating {journal}"))?;

    Ok(())
}

/// Delete every journal whose name begins with `prefix`, and report how many. A disk
/// lives in its journal, so this is all an example leaves behind.
pub async fn delete_journals(
    journals: &gazette::journal::Client,
    prefix: &str,
) -> anyhow::Result<usize> {
    let listed = journals.list(broker::ListRequest::default()).await?;

    let changes: Vec<_> = listed
        .journals
        .iter()
        .filter_map(|listed| {
            let spec = listed.spec.as_ref()?;

            spec.name
                .starts_with(prefix)
                .then(|| broker::apply_request::Change {
                    expect_mod_revision: listed.mod_revision,
                    upsert: None,
                    delete: spec.name.clone(),
                })
        })
        .collect();

    let deleted = changes.len();
    _ = journals.apply(broker::ApplyRequest { changes }).await?;

    Ok(deleted)
}
