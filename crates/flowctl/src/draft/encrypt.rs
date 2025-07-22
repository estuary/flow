use crate::Client;
use anyhow::Context;
use models::RawValue;
use serde::Deserialize;
use std::collections::HashMap;

/// Encrypts any task endpoint configurations that are not already encrypted.
/// The encryption is performed by calling the config encryption service endpoint,
/// passing the `connector_tags.endpoint_spec_schema` for the connector image. If
/// no `connector_tags` row is found, then encryption will be skipped.
pub async fn encrypt_endpoint_configs(
    draft: &mut tables::DraftCatalog,
    client: &Client,
) -> anyhow::Result<()> {
    // Simple cache of `connector_tags.endpoint_spec_schema` values, keyed on
    // the full image + tag. This is just to avoid repeated calls to fetch the
    // schemas for catalogs with many tasks.
    let mut schema_cache: HashMap<String, Option<RawValue>> = HashMap::new();

    for capture in draft.captures.iter_mut() {
        if let Some(models::CaptureEndpoint::Connector(connector)) =
            capture.model.as_mut().map(|model| &mut model.endpoint)
        {
            if !is_encrypted(&connector.config) {
                let schema =
                    fetch_or_cache_schema(&connector.image, &mut schema_cache, client).await?;

                if let Some(endpoint_spec_schema) = schema {
                    connector.config = encrypt_config(
                        client,
                        capture.capture.as_str(),
                        models::CatalogType::Capture,
                        &connector.config,
                        &endpoint_spec_schema,
                    )
                    .await?;
                } else {
                    tracing::warn!(
                        capture = %capture.capture,
                        image = %connector.image,
                        "Unable to encrypt the endpoint configuration for this task because no endpoint spec schema was found, continuing with plain-text"
                    );
                }
            }
        }
    }

    for materialization in draft.materializations.iter_mut() {
        if let Some(models::MaterializationEndpoint::Connector(connector)) = materialization
            .model
            .as_mut()
            .map(|model| &mut model.endpoint)
        {
            if !is_encrypted(&connector.config) {
                let schema =
                    fetch_or_cache_schema(&connector.image, &mut schema_cache, client).await?;

                if let Some(endpoint_spec_schema) = schema {
                    connector.config = encrypt_config(
                        client,
                        materialization.materialization.as_str(),
                        models::CatalogType::Materialization,
                        &connector.config,
                        &endpoint_spec_schema,
                    )
                    .await?;
                } else {
                    tracing::warn!(
                        materialization = %materialization.materialization,
                        image = %connector.image,
                        "Unable to encrypt the endpoint configuration for this task because no endpoint spec schema was found, continuing with plain-text"
                    );
                }
            }
        }
    }

    Ok(())
}

async fn encrypt_config(
    client: &crate::Client,
    task_name: &str,
    task_type: models::CatalogType,
    config: &RawValue,
    schema: &RawValue,
) -> anyhow::Result<RawValue> {
    tracing::debug!(?task_name, %task_type, "encrypting task endpoint config");
    let encrypted = client
        .encrypt_endpoint_config(config, schema)
        .await
        .with_context(|| format!("encrypting endpoint config for {task_type} '{task_name}'"))?;
    tracing::info!(%task_name, %task_type, "successfully encrypted endpoint configuration");
    Ok(encrypted)
}

fn is_encrypted(config: &RawValue) -> bool {
    // Check if the config contains a "sops" property with any object value
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(config.get()) {
        if let Some(obj) = value.as_object() {
            if let Some(sops_value) = obj.get("sops") {
                return sops_value.is_object();
            }
        }
    }
    false
}

async fn fetch_or_cache_schema(
    image: &str,
    cache: &mut HashMap<String, Option<RawValue>>,
    client: &Client,
) -> anyhow::Result<Option<RawValue>> {
    if let Some(cached) = cache.get(image) {
        return Ok(cached.clone());
    }

    let (image_name, image_tag) = models::split_image_tag(image);

    // Query the connector_tags table via PostgREST with a join
    let response = client
        .pg_client()
        .from("connectors")
        .select("connector_tags(endpoint_spec_schema)")
        .eq("image_name", &image_name)
        .eq("connector_tags.image_tag", &image_tag)
        .single()
        .execute()
        .await?;

    if response.status().is_success() {
        let body = response.text().await?;
        let row: ConnectorRow =
            serde_json::from_str(&body).context("failed to parse connector response")?;

        // Extract the endpoint_spec_schema from the first (and only) connector_tag
        let schema = row
            .connector_tags
            .into_iter()
            .next()
            .and_then(|tag| tag.endpoint_spec_schema);

        cache.insert(image.to_string(), schema.clone());
        Ok(schema)
    } else if response.status() == 404 {
        // No schema found for this connector
        cache.insert(image.to_string(), None);
        Ok(None)
    } else {
        anyhow::bail!(
            "Failed to fetch connector schema: {} {}",
            response.status(),
            response.text().await?
        )
    }
}

#[derive(Debug, Deserialize)]
struct ConnectorRow {
    connector_tags: Vec<ConnectorTagRow>,
}

#[derive(Debug, Deserialize)]
struct ConnectorTagRow {
    endpoint_spec_schema: Option<RawValue>,
}
