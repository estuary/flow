use crate::etcd::EtcdInstance;

/// A single gazette broker process.
pub struct GazetteBroker {
    pub process: async_process::Child,
    pub endpoint: String,
}

/// A cluster of gazette brokers sharing Etcd and HMAC auth.
pub struct GazetteCluster {
    pub brokers: Vec<GazetteBroker>,
    pub encode_key: tokens::jwt::EncodingKey,
    /// The same key as `encode_key`, base64 as a `--broker.auth-keys` value, for a
    /// component under test which parses its keys from configuration.
    pub auth_keys: String,
    /// Subdirectory used as `--broker.file-root` for fragment storage.
    /// Lives under the tempdir so it can be blown away on reset without
    /// disturbing UDS sockets.
    pub fragment_root: std::path::PathBuf,
}

impl GazetteCluster {
    /// Start a cluster of `broker_count` gazette brokers connected to the given etcd.
    pub async fn start(etcd: &EtcdInstance, broker_count: usize) -> anyhow::Result<Self> {
        // Fixed HMAC key for test auth. Pre-computed base64 of "test-data-plane-hmac-secret!".
        let base64_key = "dGVzdC1kYXRhLXBsYW5lLWhtYWMtc2VjcmV0IQ==".to_string();

        let (encode_key, _decode_keys) = tokens::jwt::parse_base64_hmac_keys([&base64_key])
            .map_err(|status| anyhow::anyhow!("failed to parse HMAC key: {status}"))?;

        // `mise run build:gazette` installs the broker into this stack's GOBIN,
        // which is per-checkout (see mise/tasks/local/stack-env) and no longer
        // the shared ~/go/bin. Prefer GOBIN when set (the mise path); fall back
        // to the default Go bin dir for invocations outside a mise stack.
        let gazette_bin = match std::env::var_os("GOBIN") {
            Some(gobin) => std::path::Path::new(&gobin).join("gazette"),
            None => std::path::Path::new(&std::env::var("HOME")?).join("go/bin/gazette"),
        };
        let etcd_endpoint = etcd.endpoint();
        let tempdir = etcd.tempdir.path();

        // Use a subdirectory for fragment storage so it can be cleared on
        // reset without disturbing UDS sockets in the parent tempdir.
        let fragment_root = tempdir.join("fragments");
        std::fs::create_dir_all(&fragment_root)?;

        let mut brokers = Vec::with_capacity(broker_count);

        // Start brokers sequentially — each needs etcd and the allocator.
        for i in 0..broker_count {
            let sock_name = format!("gazette-{i}.sock");
            let endpoint = format!("unix://localhost{}/{sock_name}", tempdir.display());
            let broker_id = format!("broker-{i}");

            let process: async_process::Child = async_process::Command::new(&gazette_bin)
                .args([
                    "serve",
                    "--broker.port",
                    &endpoint,
                    "--broker.id",
                    &broker_id,
                    "--broker.file-root",
                ])
                .arg(&fragment_root)
                .args([
                    "--broker.file-only",
                    "--broker.max-replication",
                    &broker_count.to_string(),
                    "--broker.watch-delay",
                    "10ms",
                    "--broker.auth-keys",
                    &base64_key,
                    "--etcd.address",
                    &etcd_endpoint,
                    "--log.level",
                    "info",
                ])
                .current_dir(tempdir)
                .env("TMPDIR", tempdir)
                .stdout(async_process::Stdio::inherit())
                .stderr(async_process::Stdio::inherit())
                .spawn()
                .map_err(|err| {
                    anyhow::anyhow!(
                        "failed to spawn gazette broker {i} from {} (run 'mise run build:gazette'): {err}",
                        gazette_bin.display(),
                    )
                })?
                .into();

            let sock_path = tempdir.join(&sock_name);
            crate::wait_for_socket(&sock_path).await?;

            tracing::info!(%endpoint, "gazette broker {i} socket is ready");

            brokers.push(GazetteBroker { process, endpoint });
        }

        tracing::info!(broker_count, "gazette cluster is ready");

        Ok(Self {
            brokers,
            encode_key,
            auth_keys: base64_key,
            fragment_root,
        })
    }

    /// Deliver `signal` to every broker process.
    ///
    /// A test simulates an outage with SIGSTOP and SIGCONT: a stopped broker answers
    /// nothing at all while it is stopped, which is what a client must park through
    /// rather than fail.
    pub fn signal(&self, signal: libc::c_int) {
        for broker in &self.brokers {
            let pid = broker.process.id() as libc::pid_t;

            // SAFETY: `pid` is a child of this process, which has not been reaped.
            assert_eq!(unsafe { libc::kill(pid, signal) }, 0, "signalling {pid}");
        }
    }

    /// Build a journal client authenticated with the cluster's HMAC key.
    pub fn journal_client(&self) -> anyhow::Result<gazette::journal::Client> {
        // Broad claims, self-signed as a data-plane component signs its own.
        let claims = proto_gazette::Claims {
            cap: proto_gazette::capability::LIST
                | proto_gazette::capability::APPLY
                | proto_gazette::capability::READ
                | proto_gazette::capability::APPEND,
            exp: 0,
            iat: 0,
            iss: "e2e-support".to_string(),
            sel: proto_gazette::broker::LabelSelector::default(),
            sub: "e2e-test".to_string(),
        };

        // The base client exists only to be derived from: `with_signed_claims` is
        // the one constructor the client offers for a self-signed token, as Go
        // offers only the wrapper form.
        let client = gazette::journal::Client::new(
            self.brokers[0].endpoint.clone(),
            gazette::journal::Client::new_fragment_client(),
            proto_grpc::Metadata::new(),
            gazette::Router::new("local"),
        );

        Ok(client.with_signed_claims(
            claims,
            self.encode_key.clone(),
            tokens::TimeDelta::seconds(70), // Max refresh cadence in `tokens` is every 60s.
            self.brokers[0].endpoint.clone(),
        ))
    }
}
