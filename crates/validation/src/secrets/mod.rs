//! Publication-time validation of a task's `secrets` stanza.
//!
//! Checks split into two phases around the connector round-trip: everything
//! decidable from the model runs before it, and the plaintext invariant runs
//! after, against the configuration schema of the connector's Spec response.

mod plaintext;

use super::{Error, Scope};
use std::collections::BTreeMap;

/// A task's `secrets` stanza: JSON pointers within the endpoint configuration,
/// mapped to the catalog names of the secrets which supply them.
pub type Stanza = BTreeMap<models::JsonPointer, models::Secret>;

/// Model context related to secrets validation.
pub struct Context<'a> {
    // `secrets` stanza of the model.
    secrets: &'a Stanza,
    // Endpoint config for comparison against `secret` annotations. Raw
    // connector configs are borrowed, while typed built-ins are serialized.
    config: std::borrow::Cow<'a, models::RawValue>,
}

impl<'a> Context<'a> {
    pub fn of_capture(secrets: &'a Stanza, endpoint: &'a models::CaptureEndpoint) -> Self {
        let config = match endpoint {
            models::CaptureEndpoint::Connector(config) => &config.config,
            models::CaptureEndpoint::Local(config) => &config.config,
        };
        Self {
            secrets,
            config: std::borrow::Cow::Borrowed(config),
        }
    }

    pub fn of_materialization(
        secrets: &'a Stanza,
        endpoint: &'a models::MaterializationEndpoint,
    ) -> Self {
        let config = match endpoint {
            models::MaterializationEndpoint::Connector(config) => &config.config,
            models::MaterializationEndpoint::Local(config) => &config.config,
            models::MaterializationEndpoint::Dekaf(config) => &config.config,
        };
        Self {
            secrets,
            config: std::borrow::Cow::Borrowed(config),
        }
    }

    pub fn of_derivation(secrets: &'a Stanza, using: &'a models::DeriveUsing) -> Self {
        let config = match using {
            models::DeriveUsing::Connector(config) => std::borrow::Cow::Borrowed(&config.config),
            models::DeriveUsing::Local(config) => std::borrow::Cow::Borrowed(&config.config),
            models::DeriveUsing::Typescript(config) => std::borrow::Cow::Owned(
                models::RawValue::from_string(serde_json::to_string(config).unwrap()).unwrap(),
            ),
            models::DeriveUsing::Python(config) => std::borrow::Cow::Owned(
                models::RawValue::from_string(serde_json::to_string(config).unwrap()).unwrap(),
            ),
            models::DeriveUsing::Sqlite(config) => std::borrow::Cow::Owned(
                models::RawValue::from_string(serde_json::to_string(config).unwrap()).unwrap(),
            ),
        };
        Self { secrets, config }
    }
}

/// Validate a task's `secrets` stanza against its model. A task without a
/// stanza is unaffected by any of these rules, and is not examined.
pub fn walk_model(
    scope: Scope,
    entity: &'static str,
    name: &str,
    task_type: models::CatalogType,
    shards: &models::ShardTemplate,
    ctx: &Context<'_>,
    errors: &mut tables::Errors,
) {
    if ctx.secrets.is_empty() {
        return; // Doesn't use secrets; no further validation.
    }

    // Secrets are only applied on the V2 runtime path.
    if !shards.uses_runtime_v2(task_type) {
        Error::RequireRuntimeV2 {
            entity,
            name: name.to_string(),
            capability: "secrets",
            flag: models::ENABLE_RUNTIME_V2,
        }
        .push(scope, errors);
    }

    // `sops` is incompatible with secrets: it marks the config as sealed
    // and is reserved in a secrets-using plaintext config.
    if ctx.config.is_sops() {
        Error::SecretsWithSops {
            entity,
            name: name.to_string(),
        }
        .push(scope, errors);
    }

    // A task may use only secrets which are siblings.
    let task_prefix = parent_prefix(name).unwrap_or("");
    for secret in ctx.secrets.values() {
        if task_prefix == parent_prefix(secret).unwrap_or("") {
            continue;
        }
        Error::SecretNotSibling {
            entity,
            name: name.to_string(),
            secret: secret.to_string(),
            prefix: task_prefix.to_string(),
        }
        .push(scope, errors);
    }
}

/// Validate that no `secret: true` location of `config` holds a plaintext
/// value, against `config_schema_json` of the connector's Spec response.
///
/// This deliberately collects annotations without schema validation:
/// a raw configuration is legitimately schema-invalid, because required
/// properties arrive through the merge which has not happened yet.
pub fn walk_plaintext(
    scope: Scope,
    entity: &'static str,
    name: &str,
    ctx: &Context<'_>,
    config_schema_json: &[u8],
    errors: &mut tables::Errors,
) {
    if ctx.secrets.is_empty() {
        return;
    }
    let plaintext = match plaintext::find(config_schema_json, &ctx.config.to_value()) {
        Ok(plaintext) => plaintext,
        Err(detail) => {
            Error::Connector {
                detail: detail.context("cannot check for plaintext secrets"),
            }
            .push(scope, errors);
            return;
        }
    };

    for ptr in plaintext {
        Error::SecretsPlaintextValue {
            entity,
            name: name.to_string(),
            ptr: if ptr.is_empty() {
                "(the configuration root)".to_string()
            } else {
                ptr
            },
        }
        .push(scope, errors);
    }
}

/// The catalog prefix which directly contains `name`, or None if `name` has no
/// prefix at all. Two names are siblings when their prefixes are equal.
fn parent_prefix(name: &str) -> Option<&str> {
    name.rfind('/').map(|index| &name[..index + 1])
}
