use crate::Client;
use crate::graphql::*;
use anyhow::Context;
use models::RawValue;
use std::collections::HashMap;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/draft/fetch-connector-endpoint-schema.graphql",
    response_derives = "Serialize,Clone",
    variables_derives = "Clone"
)]
struct FetchConnectorEndpointSchema;

/// Encrypts any task endpoint configurations that are not already encrypted.
/// The encryption is performed by calling the config encryption service endpoint,
/// passing the `connector_tags.endpoint_spec_schema` for the connector image. If
/// no `connector_tags` row is found, then encryption will be skipped.
pub async fn encrypt_configs(
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
                let maybe_schema =
                    fetch_or_cache_schema(&connector.image, &mut schema_cache, client).await?;
                let endpoint_spec_schema = require_schema(&capture.scope, maybe_schema)?;
                connector.config = encrypt_config(
                    client,
                    capture.capture.as_str(),
                    models::CatalogType::Capture,
                    &connector.config,
                    &endpoint_spec_schema,
                )
                .await?;
            }
        }
    }

    let triggers_schema = RawValue::from_value(&models::triggers_schema());

    for materialization in draft.materializations.iter_mut() {
        let Some(model) = materialization.model.as_mut() else {
            continue;
        };

        // Encrypt endpoint config if not already encrypted.
        if let models::MaterializationEndpoint::Connector(connector) = &mut model.endpoint {
            if !is_encrypted(&connector.config) {
                let maybe_schema =
                    fetch_or_cache_schema(&connector.image, &mut schema_cache, client).await?;
                let endpoint_spec_schema = require_schema(&materialization.scope, maybe_schema)?;
                connector.config = encrypt_config(
                    client,
                    materialization.materialization.as_str(),
                    models::CatalogType::Materialization,
                    &connector.config,
                    &endpoint_spec_schema,
                )
                .await?;
            }
        }

        // Encrypt trigger configs if present and not already encrypted.
        if let Some(triggers) = &mut model.triggers
            && triggers.sops.is_none()
        {
            *triggers = encrypt_triggers(
                client,
                materialization.materialization.as_str(),
                triggers,
                &triggers_schema,
            )
            .await?;
        }
    }

    Ok(())
}

/// Encrypt trigger configs, substituting placeholder values for fields that
/// should not be HMAC-protected by SOPS. This allows users to modify these
/// fields without causing HMAC mismatches that would require re-entering secret
/// header values.
async fn encrypt_triggers(
    client: &crate::Client,
    task_name: &str,
    triggers: &models::Triggers,
    schema: &RawValue,
) -> anyhow::Result<models::Triggers> {
    let mut to_encrypt = triggers.clone();
    let originals = models::triggers::strip_hmac_excluded_fields(&mut to_encrypt);

    let stripped_json = serde_json::to_string(&to_encrypt).context("serializing triggers")?;
    let encrypted_raw = encrypt_config(
        client,
        task_name,
        models::CatalogType::Materialization,
        &RawValue::from_string(stripped_json).context("triggers JSON is invalid")?,
        schema,
    )
    .await?;

    let mut encrypted: models::Triggers =
        serde_json::from_str(encrypted_raw.get()).context("deserializing encrypted triggers")?;
    models::triggers::restore_hmac_excluded_fields(&mut encrypted, originals);

    Ok(encrypted)
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

fn require_schema(
    scope: &url::Url,
    schema: Option<models::RawValue>,
) -> anyhow::Result<models::RawValue> {
    if let Some(s) = schema {
        Ok(s)
    } else {
        Err(anyhow::format_err!(
            "Unable to encrypt the endpoint configuration for task {scope} because the connector is not known to Estuary. Please check the spelling of the image, or reach out to Estuary support"
        ))
    }
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
    anyhow::ensure!(
        !image_tag.is_empty(),
        "invalid connector image name '{image}', must be in the form of 'registry/name:version' or 'registry/name@sha265:hash'"
    );

    let vars = fetch_connector_endpoint_schema::Variables {
        image_name,
        image_tag: image_tag.clone(),
    };
    let resp = post_graphql::<FetchConnectorEndpointSchema>(client, vars)
        .await
        .context("failed to fetch connector endpoint schema")?;
    let Some(tag) = resp.connector.and_then(|c| c.connector_tag) else {
        anyhow::bail!(
            "connector image '{image}' is unknown to Estuary, so the endpoint configuration cannot be encrypted. Use a different connector or reach out to Estuary support for help"
        );
    };

    // This commonly happens when users use a `:dev` tag, but
    if tag.endpoint_spec_schema.is_some() && tag.image_tag != image_tag {
        tracing::warn!(connector_image = %image, default_image_tag = %tag.image_tag, "connector image tag is unknown to Estuary, so the default image tag will be used to provide the schema for endpoint config encryption");
    }
    cache.insert(image.to_string(), tag.endpoint_spec_schema.clone());

    Ok(tag.endpoint_spec_schema)
}
