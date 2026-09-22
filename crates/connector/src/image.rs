//! Estuary image connector integration.
//!
//! `container` owns Docker and Podman mechanics. This module maps their shared
//! inspection model into declarations and connects image endpoints.

use anyhow::Context;
use futures::StreamExt;
use proto_flow::flow;
use std::collections::{BTreeMap, BTreeSet};

/// Required image label selecting the connector protocol spoken at runtime.
const RUNTIME_PROTO_LABEL: &str = "FLOW_RUNTIME_PROTOCOL";
/// Optional image label selecting the usage rate after first-party validation.
pub(crate) const USAGE_RATE_LABEL: &str = "dev.estuary.usage-rate";
/// Prefix of per-port labels opting a connector port into public exposure.
const PORT_PUBLIC_LABEL_PREFIX: &str = "dev.estuary.port-public.";
/// Prefix of per-port labels describing the application protocol being exposed.
const PORT_PROTO_LABEL_PREFIX: &str = "dev.estuary.port-proto.";
/// Optional image label declaring the non-sibling secrets the connector may
/// access: a JSON object mapping each secret name to the JSON pointer of the
/// endpoint configuration location where it must be merged.
pub(crate) const SECRETS_LABEL: &str = "dev.estuary.secrets";

/// Estuary declarations carried by an inspected connector image.
#[derive(Debug, serde::Serialize)]
struct Declarations {
    pub runtime_protocol: crate::RuntimeProtocol,
    #[serde(skip)]
    pub codec: connector_init::Codec,
    pub network_ports: Vec<flow::NetworkPort>,
    /// A rate declared by the image. [`crate::policy`] decides whether it is trusted.
    pub declared_usage_rate: Option<f32>,
    /// Declared image-owned secrets, and the location each must merge at.
    pub secrets: BTreeMap<String, String>,
}

impl Declarations {
    fn parse(inspected: &connector_init::inspect::Image) -> anyhow::Result<Self> {
        let labels = &inspected.config.labels;
        let mut network_ports = Vec::new();

        for exposed_port in inspected.config.exposed_ports.keys() {
            // Connector networking does not support UDP.
            if exposed_port.ends_with("/udp") {
                continue;
            }
            let exposed_port = exposed_port.strip_suffix("/tcp").unwrap_or(exposed_port);
            let number = exposed_port.parse::<u16>().with_context(|| {
                format!("invalid key in inspected Config.ExposedPorts '{exposed_port}'")
            })?;

            let protocol_label = format!("{PORT_PROTO_LABEL_PREFIX}{number}");
            let protocol = labels.get(&protocol_label).cloned().unwrap_or_default();

            let public_label = format!("{PORT_PUBLIC_LABEL_PREFIX}{number}");
            let public = labels
                .get(&public_label)
                .map(String::as_str)
                .unwrap_or("false");
            let public = public.parse::<bool>().with_context(|| {
                format!(
                    "invalid '{public_label}' label value: '{public}', must be either 'true' or 'false'"
                )
            })?;

            network_ports.push(flow::NetworkPort {
                number: number as u32,
                protocol,
                public,
            });
        }

        let Some(runtime_protocol) = labels.get(RUNTIME_PROTO_LABEL) else {
            anyhow::bail!("image is missing required '{RUNTIME_PROTO_LABEL}' label");
        };
        let runtime_protocol =
            crate::RuntimeProtocol::from_image_label(runtime_protocol).map_err(|unknown| {
                anyhow::anyhow!(
                    "image labels specify unknown protocol {RUNTIME_PROTO_LABEL}={unknown}"
                )
            })?;

        let declared_usage_rate = labels
            .get(USAGE_RATE_LABEL)
            .map(|value| {
                value
                    .parse::<f32>()
                    .with_context(|| format!("invalid '{USAGE_RATE_LABEL}' value {value:?}"))
            })
            .transpose()?;

        Ok(Self {
            runtime_protocol,
            codec: inspected.runtime_codec(),
            network_ports,
            declared_usage_rate,
            secrets: parse_secrets_label(labels.get(SECRETS_LABEL).map(String::as_str))?,
        })
    }
}

/// Connect an image endpoint after its reference-only policy has been checked.
pub(super) async fn connect<P: crate::protocol::Protocol>(
    ctx: &crate::protocol::StartContext,
    image: String,
    sealed_config: models::RawValue,
    policy: &crate::policy::Image,
    env: BTreeMap<String, String>,
    mount: &std::path::Path,
    secrets: &std::collections::BTreeMap<String, String>,
    connector_type: i32,   // TODO(johnny): remove with V1 derivations.
    spec_on_own_rpc: bool, // TODO(johnny): remove.
    requests: futures::stream::BoxStream<'static, P::Request>,
) -> anyhow::Result<crate::Transport<P>> {
    let inspected = crate::container::pull_and_inspect(&image, &ctx.log_sink).await?;
    let Declarations {
        codec,
        declared_usage_rate,
        network_ports,
        runtime_protocol,
        secrets: declared_secrets,
    } = Declarations::parse(&inspected.inspection)?;

    if !matches!(
        (runtime_protocol, P::TASK_TYPE),
        (crate::RuntimeProtocol::Capture, ops::TaskType::Capture)
            | (crate::RuntimeProtocol::Derive, ops::TaskType::Derivation)
            | (
                crate::RuntimeProtocol::Materialize,
                ops::TaskType::Materialization
            )
    ) {
        anyhow::bail!(
            "connector protocol {runtime_protocol:?} does not match requested type {:?}",
            P::TASK_TYPE,
        );
    }

    crate::policy::check_secrets(
        &ctx.task_name,
        secrets
            .iter()
            .map(|(name, pointer)| (name.as_str(), pointer.as_str())),
        crate::policy::SecretIdentity::Image {
            image: &image,
            repository: policy.repository(),
            declared: &declared_secrets,
        },
    )?;
    let usage_rate = policy.usage_rate(runtime_protocol, declared_usage_rate)?;

    let labels = BTreeMap::from([
        ("image".to_string(), image.clone()),
        ("task-name".to_string(), ctx.task_name.clone()),
        (
            "task-type".to_string(),
            P::TASK_TYPE.as_str_name().to_string(),
        ),
    ]);

    let quoted_task_name: bytes::Bytes = format!("\"{}\"", ctx.task_name).into();
    let running = crate::container::run(
        inspected,
        crate::container::RunParams {
            env,
            labels,
            log_sink: ctx.log_sink.clone(),
            mount: mount.to_owned(),
            network: ctx.container_network.clone(),
            publish_ports: matches!(ctx.plane, crate::Plane::Local),
        },
        move |log| crate::policy::sanitize_connector_log(&quoted_task_name, log),
    )
    .await?;

    let container = crate::Container {
        ip_addr: running.ip_addr.to_string(),
        network_ports,
        mapped_host_ports: running.mapped_host_ports,
        usage_rate: usage_rate.value,
    };
    ctx.log_sink
        .send(crate::build_log(
            ops::LogLevel::Info,
            "started connector container",
            [
                ("image", crate::json_field(&image)),
                ("container", crate::json_field(&container)),
            ],
        ))
        .await;

    // Drive the Spec to completion on its own RPC before opening the real one,
    // so the connector sees one request per invocation.
    let spec = if spec_on_own_rpc {
        Some(crate::protocol::spec_rpc::<P>(running.channel.clone(), connector_type).await?)
    } else {
        None
    };
    let connector_rx = P::open_rpc(running.channel, requests).await?.into_inner();

    Ok(crate::Transport {
        connector_rx: connector_rx.boxed(),
        container: Some(container),
        codec,
        process: Some(running.process),
        sealed_config,
        spec,
    })
}

/// Parse the secrets declaration of an image: a JSON object of secret catalog
/// names to JSON pointers, like the `secrets` stanza of a task. A blank label
/// declares nothing, as Docker offers no way to remove a label inherited from a
/// base image other than overriding it.
fn parse_secrets_label(label: Option<&str>) -> anyhow::Result<BTreeMap<String, String>> {
    let Some(label) = label.map(str::trim).filter(|label| !label.is_empty()) else {
        return Ok(BTreeMap::new());
    };

    let declared: BTreeMap<models::Secret, models::JsonPointer> = serde_json::from_str(label)
        .with_context(|| {
            format!(
                "image label '{SECRETS_LABEL}' must be a JSON object of secret names to JSON pointers: {label:?}"
            )
        })?;

    declared
        .into_iter()
        .map(|(name, pointer)| {
            // `err` renders as ": {value} doesn't match pattern ...", restating the value.
            if let Err(err) = validator::Validate::validate(&name) {
                anyhow::bail!("image label '{SECRETS_LABEL}' has an invalid secret name{err}");
            }
            if let Err(err) = validator::Validate::validate(&pointer) {
                anyhow::bail!("image label '{SECRETS_LABEL}' has an invalid JSON pointer{err}");
            }
            Ok((name.into(), pointer.into()))
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::{Declarations, parse_secrets_label};
    use proto_flow::flow;
    use serde_json::json;

    #[test]
    fn parses_flow_image_declarations() {
        let fixture = json!([{
            "Id": "test-image-id",
            "Created": "2024-02-02T14:39:11.958Z",
            "Config": {
                "Cmd": ["connector"],
                "Env": [],
                "ExposedPorts": {"567/tcp": {}, "123/udp": {}, "789": {}},
                "Labels": {
                    "FLOW_RUNTIME_CODEC": "json",
                    "FLOW_RUNTIME_PROTOCOL": "derive",
                    "dev.estuary.port-public.567": "true",
                    "dev.estuary.port-proto.789": "h2",
                    "dev.estuary.usage-rate": "1.3",
                    "dev.estuary.secrets": r#" {"acmeVendor/oauth/connectors/ghcr.io/acmeVendor/source-widgets/oauth-client": "/credentials", "acmeVendor/other": ""} "#
                }
            }
        }]);
        let inspected =
            connector_init::inspect::Image::parse_from_json_slice(fixture.to_string().as_bytes())
                .unwrap();
        let declarations = Declarations::parse(&inspected).unwrap();

        assert_eq!(
            declarations.network_ports,
            [
                flow::NetworkPort {
                    number: 567,
                    protocol: String::new(),
                    public: true,
                },
                flow::NetworkPort {
                    number: 789,
                    protocol: "h2".to_string(),
                    public: false,
                },
            ]
        );
        assert_eq!(Some(1.3), declarations.declared_usage_rate);
        assert_eq!(connector_init::Codec::Json, declarations.codec);
        insta::assert_debug_snapshot!(declarations.secrets, @r###"
        {
            "acmeVendor/oauth/connectors/ghcr.io/acmeVendor/source-widgets/oauth-client": "/credentials",
            "acmeVendor/other": "",
        }
        "###);
    }

    #[test]
    fn parses_secrets_declaration() {
        let cases = [
            None,
            Some(""),
            Some("   "),
            Some("{}"),
            Some(r#"{"acmeCo/one": "/credentials"}"#),
            Some(r#" {"acmeCo/one": "/credentials", "acmeCo/two": ""} "#),
            // Two secrets may merge at one location.
            Some(r#"{"acmeCo/one": "/credentials", "acmeCo/two": "/credentials"}"#),
            // The former comma-delimited form of names alone.
            Some("acmeCo/one,acmeCo/two"),
            Some(r#"["acmeCo/one"]"#),
            Some(r#"{"acmeCo/one": 42}"#),
            Some(r#"{"acmeCo/bad name": "/credentials"}"#),
            Some(r#"{"acmeCo/one": "credentials"}"#),
        ];
        let outcomes: Vec<_> = cases
            .into_iter()
            .map(|label| {
                (
                    label,
                    parse_secrets_label(label).map_err(|err| format!("{err:#}")),
                )
            })
            .collect();

        insta::assert_debug_snapshot!(outcomes);
    }

    #[test]
    fn rejects_invalid_flow_declarations() {
        let fixtures = [
            json!([{"Config": {"Labels": {}, "Env": []}}]),
            json!([{"Config": {
                "Labels": {"FLOW_RUNTIME_PROTOCOL": "derive"},
                "Env": [],
                "ExposedPorts": {"whoops": {}}
            }}]),
            json!([{"Config": {
                "Labels": {
                    "FLOW_RUNTIME_PROTOCOL": "derive",
                    "dev.estuary.port-public.111": "whoops"
                },
                "Env": [],
                "ExposedPorts": {"111/tcp": {}}
            }}]),
        ];
        let outcomes: Vec<_> = fixtures
            .into_iter()
            .map(|fixture| {
                let inspected = connector_init::inspect::Image::parse_from_json_slice(
                    fixture.to_string().as_bytes(),
                )
                .unwrap();
                Declarations::parse(&inspected).map_err(|err| format!("{err:#}"))
            })
            .collect();

        insta::assert_debug_snapshot!(outcomes);
    }
}
