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
/// passing the connector's `endpointSpecSchema` to identify secret fields.
pub async fn encrypt_configs(
    draft: &mut tables::DraftCatalog,
    ctx: &crate::CliContext,
) -> anyhow::Result<()> {
    // Simple cache of endpoint spec schemas, keyed on the full image + tag.
    // Avoids repeated GraphQL calls for catalogs with many tasks.
    let mut schema_cache: HashMap<String, Option<RawValue>> = HashMap::new();

    for capture in draft.captures.iter_mut() {
        if let Some(models::CaptureEndpoint::Connector(connector)) =
            capture.model.as_mut().map(|model| &mut model.endpoint)
        {
            if !is_encrypted(&connector.config) {
                let maybe_schema =
                    fetch_or_cache_schema(&connector.image, &mut schema_cache, ctx).await?;
                let endpoint_spec_schema = require_schema(&capture.scope, maybe_schema)?;
                connector.config = encrypt_config(
                    ctx,
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
                    fetch_or_cache_schema(&connector.image, &mut schema_cache, ctx).await?;
                let endpoint_spec_schema = require_schema(&materialization.scope, maybe_schema)?;
                connector.config = encrypt_config(
                    ctx,
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
                ctx,
                materialization.materialization.as_str(),
                triggers,
                &triggers_schema,
            )
            .await?;
        }
    }

    Ok(())
}

/// Encrypt trigger configs. The triggers schema marks header values as
/// `secret` (encrypted) and tunables as `nonsensitive` (modifiable after
/// encryption via a `sops.overlay`, without re-entering secret headers).
async fn encrypt_triggers(
    ctx: &crate::CliContext,
    task_name: &str,
    triggers: &models::Triggers,
    schema: &RawValue,
) -> anyhow::Result<models::Triggers> {
    let plaintext = serde_json::to_string(triggers).context("serializing triggers")?;
    let encrypted_raw = encrypt_config(
        ctx,
        task_name,
        models::CatalogType::Materialization,
        &RawValue::from_string(plaintext).context("triggers JSON is invalid")?,
        schema,
    )
    .await?;

    serde_json::from_str(encrypted_raw.get()).context("deserializing encrypted triggers")
}

async fn encrypt_config(
    ctx: &crate::CliContext,
    task_name: &str,
    task_type: models::CatalogType,
    config: &RawValue,
    schema: &RawValue,
) -> anyhow::Result<RawValue> {
    tracing::debug!(?task_name, %task_type, "encrypting task endpoint config");
    let encrypted = encrypt_endpoint_config(
        &ctx.rest.http_client,
        ctx.config.get_config_encryption_url(),
        config,
        schema,
    )
    .await
    .with_context(|| format!("encrypting endpoint config for {task_type} '{task_name}'"))?;
    tracing::info!(%task_name, %task_type, "successfully encrypted endpoint configuration");
    Ok(encrypted)
}

/// Calls the config encryption service to encrypt `plaintext` using `schema` to
/// identify secret fields. Port of the former
/// `flow_client::Client::encrypt_endpoint_config`.
async fn encrypt_endpoint_config(
    http_client: &reqwest::Client,
    config_encryption_url: &url::Url,
    plaintext: &RawValue,
    schema: &RawValue,
) -> anyhow::Result<RawValue> {
    #[derive(serde::Serialize)]
    struct EncryptRequest<'a> {
        config: &'a RawValue,
        schema: &'a RawValue,
    }

    let encrypt_endpoint = format!("{config_encryption_url}v1/encrypt-config");

    // The encryption service does not currently require any sort of
    // authentication, so there's no auth header added here.
    let response = http_client
        .post(&encrypt_endpoint)
        .header("Content-Type", "application/json")
        .json(&EncryptRequest {
            config: plaintext,
            schema,
        })
        .send()
        .await?;

    if !response.status().is_success() {
        anyhow::bail!(
            "Config encryption failed: {} {}",
            response.status(),
            response.text().await?
        );
    }

    let bytes = response.bytes().await?;
    let encrypted: Box<RawValue> = serde_json::from_slice(&bytes)?;
    Ok(*encrypted)
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
    ctx: &crate::CliContext,
) -> anyhow::Result<Option<RawValue>> {
    if let Some(cached) = cache.get(image) {
        return Ok(cached.clone());
    }

    let (_image_name, image_tag) = models::split_image_tag(image);
    anyhow::ensure!(
        !image_tag.is_empty(),
        "invalid connector image name '{image}', must be in the form of 'registry/name:version' or 'registry/name@sha256:hash'"
    );

    let vars = fetch_connector_endpoint_schema::Variables {
        full_image_name: image.to_string(),
    };
    let resp = post_graphql::<FetchConnectorEndpointSchema>(
        &ctx.rest,
        ctx.access_token().as_deref(),
        vars,
    )
    .await
    .context("failed to fetch connector endpoint schema")?;
    let Some(spec) = resp.connector_spec else {
        anyhow::bail!(
            "connector image '{image}' is unknown to Estuary, so the endpoint configuration cannot be encrypted. Use a different connector or reach out to Estuary support for help"
        );
    };

    // This commonly happens when users use a `:dev` tag or a custom PR tag.
    if spec.endpoint_spec_schema.is_some() && spec.image_tag != image_tag {
        tracing::warn!(connector_image = %image, default_image_tag = %spec.image_tag, "connector image tag is unknown to Estuary, so the default image tag will be used to provide the schema for endpoint config encryption");
    }
    cache.insert(image.to_string(), spec.endpoint_spec_schema.clone());

    Ok(spec.endpoint_spec_schema)
}
