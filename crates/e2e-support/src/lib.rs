use anyhow::Context;

pub mod etcd;
pub mod gazette;

pub use etcd::EtcdInstance;
pub use gazette::GazetteCluster;

/// Arguments for starting a DataPlane.
pub struct DataPlaneArgs {
    pub broker_count: usize,
}

impl Default for DataPlaneArgs {
    fn default() -> Self {
        Self { broker_count: 3 }
    }
}

/// A hermetic data-plane with etcd and a gazette broker cluster.
///
/// Call [`DataPlane::stop`] for graceful SIGTERM shutdown.
/// If simply dropped, child processes are SIGKILL'd.
pub struct DataPlane {
    pub tracing_guard: tracing::subscriber::DefaultGuard,
    pub etcd: EtcdInstance,
    pub gazette: GazetteCluster,
    pub journal_client: ::gazette::journal::Client,
}

impl DataPlane {
    /// Start a hermetic data-plane with the given configuration.
    pub async fn start(args: DataPlaneArgs) -> anyhow::Result<Self> {
        let etcd = EtcdInstance::start().await?;
        let gazette = GazetteCluster::start(&etcd, args.broker_count).await?;
        let journal_client = gazette.journal_client()?;

        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(
                tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
                    .from_env()
                    .context("parsing RUST_LOG filter")?,
            )
            .finish();

        let tracing_guard = tracing::subscriber::set_default(subscriber);

        Ok(Self {
            tracing_guard,
            journal_client,
            gazette,
            etcd,
        })
    }

    /// Reset the data-plane to its initial state.
    ///
    /// Deletes all journal specs and clears the broker fragment store so
    /// that subsequent journal creations start with a clean offset history.
    pub async fn reset(&self) -> anyhow::Result<()> {
        let listed = self
            .journal_client
            .list(proto_gazette::broker::ListRequest::default())
            .await
            .context("failed to list journals")?;

        let changes: Vec<_> = listed
            .journals
            .iter()
            .filter_map(|j| {
                let spec = j.spec.as_ref()?;
                Some(proto_gazette::broker::apply_request::Change {
                    expect_mod_revision: j.mod_revision,
                    upsert: None,
                    delete: spec.name.clone(),
                })
            })
            .collect();

        if !changes.is_empty() {
            self.journal_client
                .apply(proto_gazette::broker::ApplyRequest { changes })
                .await
                .context("failed to delete journals")?;
        }

        // Clear persisted fragments so recreated journals start fresh.
        let root = &self.gazette.fragment_root;
        std::fs::remove_dir_all(root).context("failed to remove fragment root")?;
        std::fs::create_dir_all(root).context("failed to recreate fragment root")?;

        Ok(())
    }

    /// Gracefully stop the data-plane.
    pub async fn graceful_stop(self) -> anyhow::Result<()> {
        self.reset().await?;

        let Self {
            tracing_guard,
            etcd,
            gazette,
            journal_client: _, // Dropped; releases client connections.
        } = self;

        let etcd_endpoint = etcd.endpoint();
        let _guard = etcd.tempdir;

        stop_children(gazette.brokers.into_iter().map(|b| (b.endpoint, b.process))).await;
        stop_children(std::iter::once((etcd_endpoint, etcd.process))).await;
        drop(tracing_guard);

        Ok(())
    }
}

/// Poll for the existence of a UDS socket file, with exponential backoff.
async fn wait_for_socket(path: &std::path::Path) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);

    for i in 1u32.. {
        if tokio::fs::metadata(&path).await.is_ok() {
            return Ok(());
        }
        if tokio::time::Instant::now() > deadline {
            anyhow::bail!("timed out waiting for socket {} to appear", path.display());
        }
        tokio::time::sleep(std::time::Duration::from_millis(20 * i as u64)).await;
    }
    unreachable!()
}

/// SIGTERM all children, then await their exits concurrently.
async fn stop_children(children: impl Iterator<Item = (String, async_process::Child)>) {
    let children: Vec<_> = children.collect();

    for (endpoint, child) in &children {
        let pid = child.id() as libc::pid_t;
        tracing::info!(%endpoint, pid, "sending SIGTERM");
        unsafe { libc::kill(pid, libc::SIGTERM) };
    }

    let mut set = tokio::task::JoinSet::new();
    for (endpoint, mut child) in children {
        set.spawn(async move {
            let status = child.wait().await;
            tracing::info!(%endpoint, ?status, "process exited");
        });
    }
    while set.join_next().await.is_some() {}
}
