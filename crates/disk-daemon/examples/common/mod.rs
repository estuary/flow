//! What the examples share: where the daemon and its brokers are, and the journal
//! specifications a client applies before it opens a disk.
//!
//! Write prose in this directory in Simplified Technical English. Use short
//! sentences and the active voice.
//!
//! Cargo builds one example at a time, so what one uses the other does not.
#![allow(dead_code)]

use anyhow::Context;
use proto_gazette::broker;

/// The socket of a running daemon, and the broker its disks append to.
///
/// Each default is what `examples/demo-services.sh` starts, and `temp_dir` reads
/// `TMPDIR` as that script does, so an example of those services needs no variable.
///
/// These address the example's own Gazette client, which creates and reads the
/// journals of its disks. The daemon has brokers and a key of its own configuration,
/// so nothing here reaches it.
pub fn config() -> anyhow::Result<(std::path::PathBuf, Broker)> {
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
    let broker = Broker {
        endpoint: std::env::var("BROKER_ADDRESS")
            .unwrap_or_else(|_| "http://127.0.0.1:28080".to_string()),
        credential: std::env::var("BROKER_CREDENTIAL").unwrap_or_default(),
    };

    Ok((uds, broker))
}

/// Brokers an example manages its own journals through. The daemon has brokers and a
/// key of its own configuration, so this authorizes only the example itself.
pub struct Broker {
    pub endpoint: String,
    pub credential: String,
}

/// A Gazette client of `broker`, for an example to manage its own journals.
pub fn journal_client(broker: &Broker) -> anyhow::Result<gazette::journal::Client> {
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

/// Specification of one disk journal, which the example creates before it opens.
///
/// The daemon creates no journal. It validates the live specification of the one an
/// `Open` names, so this is the example standing in for whoever deploys a disk.
///
/// SNAPPY is the codec the design gives for disk journals. Each fragment closes at
/// one megabyte, so an example stores several rather than keeping every record in
/// one broker spool.
pub fn journal_spec(journal: &str) -> broker::JournalSpec {
    broker::JournalSpec {
        name: journal.to_string(),
        replication: 1,
        fragment: Some(broker::journal_spec::Fragment {
            length: 1 << 20,
            compression_codec: broker::CompressionCodec::Snappy as i32,
            stores: vec!["file:///".to_string()],
            refresh_interval: Some(std::time::Duration::from_secs(300).into()),
            ..Default::default()
        }),
        // A journal must declare that it holds a disk, as one must declare that it
        // holds a recovery log.
        labels: Some(labels::build_set([(
            labels::CONTENT_TYPE,
            disk_daemon::CONTENT_TYPE_DISK,
        )])),
        flags: broker::journal_spec::Flag::ORdwr as u32,
        max_append_rate: 1 << 22,
        ..Default::default()
    }
}

/// Create the journal of `spec`, unless a journal of that name exists. Report whether
/// this call created it.
///
/// The daemon creates no journal. This is the act of whoever deploys a disk, and it is
/// the request which an activation makes: one upsert, on the condition that the
/// journal does not exist.
pub async fn create_journal(
    client: &gazette::journal::Client,
    spec: broker::JournalSpec,
) -> anyhow::Result<bool> {
    let journal = spec.name.clone();

    let request = broker::ApplyRequest {
        changes: vec![broker::apply_request::Change {
            // Zero requires that no journal of this name exists.
            expect_mod_revision: 0,
            upsert: Some(spec),
            delete: String::new(),
        }],
    };

    match client.apply(request).await {
        Ok(_response) => Ok(true),
        Err(gazette::Error::BrokerStatus(broker::Status::EtcdTransactionFailed)) => Ok(false),
        Err(err) => Err(anyhow::Error::new(err).context(format!("creating {journal}"))),
    }
}

/// Recovery floor the daemon stored on `journal`, or `None` when it holds none.
///
/// The daemon keeps a disk's floor in the journal's own label, so this is where a
/// client looks to find one. It is also what tells whatever deletes fragments which
/// of them a recovery still needs.
///
/// A journal which the daemon has not yet completed a recovery horizon for stores no
/// floor at all.
pub async fn floor_of(
    client: &gazette::journal::Client,
    journal: &str,
) -> anyhow::Result<Option<u64>> {
    let listing = client
        .get_journal(journal)
        .await
        .with_context(|| format!("listing {journal}"))?;

    let Some(set) = listing
        .and_then(|listed| listed.spec)
        .and_then(|spec| spec.labels)
    else {
        return Ok(None);
    };

    match labels::maybe_one(&set, disk_daemon::DISK_RECOVERY_FLOOR)? {
        "" => Ok(None),
        value => Ok(Some(disk_daemon::parse_recovery_floor(value)?)),
    }
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
