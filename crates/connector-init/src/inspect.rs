use std::collections::BTreeMap;

/// Image label or environment variable which identifies the codec that
/// the connector wishes to use. A value of "json" selects the JSON codec;
/// any other value (or absence) selects protobuf.
pub const FLOW_RUNTIME_CODEC: &str = "FLOW_RUNTIME_CODEC";

/// Image is the object returned by `docker inspect` over an image.
#[derive(Debug, serde::Deserialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Image {
    pub id: String,
    pub config: ImageConfig,
    /// Every local tag of the image, fully qualified. It lists all names the
    /// image is tagged under, and not only the name it was run under.
    #[serde(default)]
    pub repo_tags: Vec<String>,
}

#[derive(Debug, serde::Deserialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct ImageConfig {
    pub cmd: Option<Vec<String>>,
    pub entrypoint: Option<Vec<String>>,
    pub labels: BTreeMap<String, String>,
    pub env: Vec<String>,
    // Unlike `labels` and `env`, which every connector image carries,
    // an image publishing no port omits `ExposedPorts` entirely.
    #[serde(default)]
    pub exposed_ports: BTreeMap<String, serde_json::Value>,
}

impl Image {
    /// Parse `docker inspect` output (a single-element JSON array) from bytes.
    pub fn parse_from_json_slice(content: &[u8]) -> anyhow::Result<Self> {
        let [out] = serde_json::from_slice::<[Image; 1]>(content)?;
        Ok(out)
    }

    /// Resolve the codec that the connector Image wishes to use.
    pub fn runtime_codec(&self) -> crate::Codec {
        match self.get_label_or_env(FLOW_RUNTIME_CODEC) {
            Some("json") => crate::Codec::Json,
            _ => crate::Codec::Proto,
        }
    }

    /// Is this a frozen `derive-typescript` or `derive-python` `:dev` image?
    /// These images serve only one request per process. The rule mirrors
    /// `models::is_frozen_derive_image`, applied to every local tag of the image.
    // TODO(johnny): Remove with retirement of the V1 runtime for derivations.
    // TODO(johnny): VMM rebase: this applies to the podman inspect JSON which
    // the VMM mounts, so the VMM's own `spec_rpc` arm is deleted. A tag-pulled
    // `linux/amd64` image's `podman image inspect` lists its fully qualified
    // `:dev` tag in `RepoTags`.
    pub fn is_frozen_derive(&self) -> bool {
        self.repo_tags.iter().any(|tag| {
            matches!(
                tag.as_str(),
                "ghcr.io/estuary/derive-typescript:dev" | "ghcr.io/estuary/derive-python:dev"
            )
        })
    }

    /// Find the arguments required to invoke the connector,
    /// as indicated by either an ENTRYPOINT or CMD of the Dockerfile.
    pub fn get_argv(&self) -> anyhow::Result<Vec<String>> {
        if let Some(a) = &self.config.entrypoint {
            Ok(a.clone())
        } else if let Some(a) = &self.config.cmd {
            Ok(a.clone())
        } else {
            anyhow::bail!("image config has neither entrypoint nor cmd")
        }
    }

    /// Fetch the value of a LABEL or ENV of the image which has the given name.
    pub fn get_label_or_env(&self, name: &str) -> Option<&str> {
        if let Some(value) = self.config.labels.get(name) {
            return Some(value);
        }
        let env_prefix = format!("{name}=");

        for env in self.config.env.iter() {
            if env.starts_with(&env_prefix) {
                return Some(&env[env_prefix.len()..]);
            }
        }
        None
    }
}

#[cfg(test)]
mod test {
    use super::Image;

    #[test]
    fn detects_frozen_derive_images_by_any_tag() {
        let outcomes = [
            serde_json::json!([]),
            serde_json::json!(["ghcr.io/estuary/source-http-ingest:dev"]),
            serde_json::json!(["ghcr.io/estuary/derive-typescript:v1"]),
            serde_json::json!([
                "acmeCo/derive:local",
                "ghcr.io/estuary/derive-typescript:dev"
            ]),
            serde_json::json!(["ghcr.io/estuary/derive-python:dev"]),
        ]
        .map(|tags| {
            let fixture = serde_json::json!([{
                "Id": "test-image-id",
                "RepoTags": tags,
                "Config": {"Labels": {}, "Env": []},
            }]);
            Image::parse_from_json_slice(fixture.to_string().as_bytes())
                .unwrap()
                .is_frozen_derive()
        });

        assert_eq!(outcomes, [false, false, false, true, true]);
    }

    #[test]
    fn parses_docker_and_podman_inspections() {
        let outcomes: Vec<_> = [
            (
                "docker",
                include_bytes!("fixtures/docker-image-inspect.json").as_slice(),
            ),
            (
                "podman",
                include_bytes!("fixtures/podman-image-inspect.json").as_slice(),
            ),
        ]
        .into_iter()
        .map(|(engine, fixture)| (engine, Image::parse_from_json_slice(fixture).unwrap()))
        .collect();

        insta::assert_debug_snapshot!(outcomes, @r#"
        [
            (
                "docker",
                Image {
                    id: "sha256:c57faed797fce979d79ab84b76f7655b7d1b34a048fb1a9f518b2657e0ca54b7",
                    config: ImageConfig {
                        cmd: None,
                        entrypoint: Some(
                            [
                                "/connector/source-http-ingest",
                            ],
                        ),
                        labels: {
                            "CONNECTOR_PROTOCOL": "flow-capture",
                            "FLOW_RUNTIME_CODEC": "json",
                            "FLOW_RUNTIME_PROTOCOL": "capture",
                            "dev.estuary.port-public.8080": "true",
                            "org.opencontainers.image.revision": "262fcee1dab22b57a234e247f8b65b93a7bf0032",
                        },
                        env: [
                            "PATH=/connector:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                            "GOMEMLIMIT=900MiB",
                            "DOCS_URL=https://go.estuary.dev/source-http-ingest",
                            "CONNECTOR_NAME=source-http-ingest",
                        ],
                        exposed_ports: {
                            "8080/tcp": Object {},
                        },
                    },
                    repo_tags: [
                        "ghcr.io/estuary/source-http-ingest:dev",
                    ],
                },
            ),
            (
                "podman",
                Image {
                    id: "9e544cbf7d80edc92a2b9e8b1d5ec447d1e16d4e16e82c8bf77bc7407fece984",
                    config: ImageConfig {
                        cmd: None,
                        entrypoint: Some(
                            [
                                "/connector/source-http-ingest",
                            ],
                        ),
                        labels: {
                            "CONNECTOR_PROTOCOL": "flow-capture",
                            "FLOW_RUNTIME_CODEC": "json",
                            "FLOW_RUNTIME_PROTOCOL": "capture",
                            "dev.estuary.port-public.8080": "true",
                            "org.opencontainers.image.revision": "262fcee1dab22b57a234e247f8b65b93a7bf0032",
                        },
                        env: [
                            "PATH=/connector:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                            "GOMEMLIMIT=900MiB",
                            "DOCS_URL=https://go.estuary.dev/source-http-ingest",
                            "CONNECTOR_NAME=source-http-ingest",
                        ],
                        exposed_ports: {
                            "8080/tcp": Object {},
                        },
                    },
                    repo_tags: [
                        "ghcr.io/estuary/source-http-ingest:dev",
                    ],
                },
            ),
        ]
        "#);
    }
}
