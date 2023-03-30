use super::{Connectors, Error, Scope};
use anyhow::Context;
use proto_flow::flow::NetworkPort;
use std::collections::BTreeMap;

pub struct Image {
    image: String,
    inspection: anyhow::Result<Vec<NetworkPort>>,
}

/// Walks every docker image that is used by a validated specification,
/// attempting to inspect each one and memoizing success or an error.
/// The returned Vec<Image> is sorted on ascending image.
pub async fn walk_all_images<C: Connectors>(
    connectors: &C,
    captures: &[tables::Capture],
    collections: &[tables::Collection],
    materializations: &[tables::Materialization],
) -> Vec<Image> {
    let mut used_images = Vec::new();

    for capture in captures {
        let models::CaptureEndpoint::Connector(config) = &capture.spec.endpoint;
        used_images.push(config.image.clone());
    }
    for collection in collections {
        if let models::CollectionDef {
            derive:
                Some(models::Derivation {
                    using: models::DeriveUsing::Connector(config),
                    ..
                }),
            ..
        } = &collection.spec
        {
            used_images.push(config.image.clone());
        }
    }
    for materialization in materializations {
        if let models::MaterializationEndpoint::Connector(config) = &materialization.spec.endpoint {
            used_images.push(config.image.clone());
        }
    }

    used_images.sort();
    used_images.dedup();

    let inspect_results = used_images.into_iter().map(|image| async move {
        Image {
            image: image.clone(),
            inspection: connectors
                .inspect_image(image)
                .await
                .and_then(parse_image_inspection),
        }
    });

    futures::future::join_all(inspect_results).await
}

pub fn walk_image_network_ports(
    scope: Scope,
    disabled: bool,
    image: &str,
    images: &[Image],
    errors: &mut tables::Errors,
) -> Vec<NetworkPort> {
    let index = images
        .binary_search_by_key(&image, |i| &i.image)
        .expect("all images were fetched and are in sorted order");
    match (disabled, &images[index].inspection) {
        // When disabled, we ignore the outcome of image inspection.
        // It may be disabled *because* the image is broken.
        (true, _) => Vec::new(),
        (false, Ok(ports)) => ports.clone(),
        (false, Err(error)) => {
            Error::ImageInspectFailed {
                image: image.to_string(),
                // `error` is not Clone and multiple specs could be using this image,
                // so we must round-trip through a String encoding.
                detail: anyhow::anyhow!("{error:#}"),
            }
            .push(scope, errors);

            Vec::new()
        }
    }
}

fn parse_image_inspection(content: Vec<u8>) -> anyhow::Result<Vec<NetworkPort>> {
    let deserialized: Vec<InspectJson> = serde_json::from_slice(&content).with_context(|| {
        format!(
            "failed to parse `docker inspect` output: {}",
            String::from_utf8_lossy(&content)
        )
    })?;

    if deserialized.len() != 1 {
        anyhow::bail!("expected 1 image, got {}", deserialized.len());
    }

    let mut ports = Vec::new();
    for (exposed_port, _) in deserialized[0].config.exposed_ports.iter() {
        // We're unable to support UDP at this time.
        if exposed_port.ends_with("/udp") {
            continue;
        }
        // Technically, the ports are allowed to appear without the '/tcp' suffix, though
        // I haven't actually observed that in practice.
        let exposed_port = exposed_port.strip_suffix("/tcp").unwrap_or(exposed_port);
        let number = exposed_port.parse::<u16>().with_context(|| {
            format!("invalid key in inspected Config.ExposedPorts '{exposed_port}'")
        })?;

        let protocol_label = format!("dev.estuary.port-proto.{number}");
        let protocol = deserialized[0].config.labels.get(&protocol_label).cloned();

        let public_label = format!("dev.estuary.port-public.{number}");
        let public = deserialized[0]
            .config
            .labels
            .get(&public_label)
            .map(String::as_str)
            .unwrap_or("false");
        let public = public.parse::<bool>()
        .with_context(||  format!("invalid '{public_label}' label value: '{public}', must be either 'true' or 'false'"))?;

        ports.push(NetworkPort {
            number: number as u32,
            protocol: protocol.unwrap_or_default(),
            public,
        });
    }

    Ok(ports)
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InspectConfig {
    /// According to the [OCI spec](https://github.com/opencontainers/image-spec/blob/d60099175f88c47cd379c4738d158884749ed235/config.md?plain=1#L125)
    /// `ExposedPorts` is a map where the keys are in the format `1234/tcp`, `456/udp`, or `789` (implicit default of tcp), and the values are
    /// empty objects. The choice of `serde_json::Value` here is meant to convey that the actual values are irrelevant.
    #[serde(default)]
    exposed_ports: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    labels: BTreeMap<String, String>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InspectJson {
    config: InspectConfig,
}
