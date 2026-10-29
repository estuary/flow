use proto_gazette::broker;

/// Daemon socket and the example's own broker configuration. Defaults match
/// `demo-services.sh`, including `TMPDIR`. These broker credentials create and
/// inspect journals; the daemon has its own configured credentials.
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
            false =>
                "Run the services in another terminal with \
                     `mise exec -- crates/disk-daemon/examples/demo-services.sh`.",
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
    endpoint: String,
    credential: String,
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

/// A journal-name prefix unique to this run. Deleting a journal leaves its
/// fragments in the store, and a journal re-created under the same name would
/// index them and replay the earlier run's disk.
pub fn prefix() -> anyhow::Result<String> {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    Ok(format!("acmeCo/example/{seconds}"))
}

/// Spec of a disk journal. Fragments roll at 1 MiB, so each 1 MiB payload of
/// `two_phase_commit` reaches the fragment store; the other examples write less.
pub fn journal_spec(journal: &str) -> broker::JournalSpec {
    broker::JournalSpec {
        name: journal.to_string(),
        replication: 1,
        fragment: Some(broker::journal_spec::Fragment {
            length: 1 << 20, // 1 MiB
            compression_codec: broker::CompressionCodec::Snappy as i32,
            stores: vec!["file:///".to_string()],
            refresh_interval: Some(std::time::Duration::from_secs(300).into()),
            ..Default::default()
        }),
        labels: Some(labels::build_set([(
            labels::CONTENT_TYPE,
            disk_daemon::CONTENT_TYPE_DISK,
        )])),
        flags: broker::journal_spec::Flag::ORdwr as u32,
        max_append_rate: 1 << 22, // 4 MiB/s
        ..Default::default()
    }
}

/// Create the journal if absent, returning whether this call created it.
/// The example performs the deployment step, which the daemon never does.
pub async fn create_journal(
    client: &gazette::journal::Client,
    spec: broker::JournalSpec,
) -> anyhow::Result<bool> {
    let journal = spec.name.clone();

    let request = broker::ApplyRequest {
        changes: vec![broker::apply_request::Change {
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

/// Delete every journal whose name begins with `prefix`.
pub async fn delete_journals(
    journals: &gazette::journal::Client,
    prefix: &str,
) -> anyhow::Result<()> {
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

    _ = journals.apply(broker::ApplyRequest { changes }).await?;

    Ok(())
}
