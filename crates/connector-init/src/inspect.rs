use std::collections::BTreeMap;

/// Image label or environment variable which identifies the codec that
/// the connector wishes to use. A value of "json" selects the JSON codec;
/// any other value (or absence) selects protobuf.
pub const FLOW_RUNTIME_CODEC: &str = "FLOW_RUNTIME_CODEC";

/// Image is the object returned by `docker inspect` over an image.
#[derive(Debug, serde::Deserialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Image {
    pub config: ImageConfig,
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

        insta::assert_debug_snapshot!(outcomes, @r###"
        [
            (
                "docker",
                Image {
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
                },
            ),
            (
                "podman",
                Image {
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
                },
            ),
        ]
        "###);
    }
}
