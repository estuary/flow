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

        let gazette_bin = format!("{}/go/bin/gazette", std::env::var("HOME")?);
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
                        "failed to spawn gazette broker {i} (is ~/go/bin/gazette built?): {err}"
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
            fragment_root,
        })
    }

    /// Build a journal client authenticated with the cluster's HMAC key.
    pub fn journal_client(&self) -> anyhow::Result<gazette::journal::Client> {
        // Start a self-signed tokens source with broad claims.
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
        let source = tokens::jwt::SignedSource {
            claims,
            set_time_claims: Box::new(|claims, iat, exp| {
                (claims.iat, claims.exp) = (iat.timestamp() as u64, exp.timestamp() as u64);
            }),
            duration: tokens::TimeDelta::seconds(30),
            key: self.encode_key.clone(),
        };
        let default_endpoint = self.brokers[0].endpoint.clone();

        Ok(gazette::journal::Client::new_with_tokens(
            move |token| {
                Ok((
                    proto_grpc::Metadata::new().with_bearer_token(&token)?,
                    default_endpoint.clone(),
                ))
            },
            gazette::journal::Client::new_fragment_client(),
            gazette::Router::new("local"),
            tokens::watch(source),
        ))
    }
}
