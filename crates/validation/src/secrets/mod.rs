//! Publication-time validation of a task's `secrets` stanza.
//!
//! Checks split into two phases around the connector round-trip: everything
//! decidable from the model runs before it, and the plaintext invariant runs
//! after, against the configuration schema of the connector's Spec response.

mod plaintext;

use super::{Error, Scope};
use std::collections::BTreeMap;

/// A task's `secrets` stanza: catalog secret names mapped to JSON pointers
/// within the endpoint configuration where their values are merged.
pub type Stanza = BTreeMap<models::Secret, models::JsonPointer>;

/// Model context related to secrets validation.
pub struct Context<'a> {
    // `secrets` stanza of the model.
    secrets: &'a Stanza,
    // Endpoint config for comparison against `secret` annotations. Raw
    // connector configs are borrowed, while typed built-ins are serialized.
    config: std::borrow::Cow<'a, models::RawValue>,
    // Image repository of an explicit connector image, tag and digest
    // stripped, under which the image rule may admit a non-sibling secret.
    // None for every other endpoint: built-in derivations, Dekaf, and Local
    // connectors have no image identity, and get siblings only.
    image: Option<String>,
}

impl<'a> Context<'a> {
    pub fn of_capture(secrets: &'a Stanza, endpoint: &'a models::CaptureEndpoint) -> Self {
        let (config, image) = match endpoint {
            models::CaptureEndpoint::Connector(config) => (&config.config, Some(&config.image)),
            models::CaptureEndpoint::Local(config) => (&config.config, None),
        };
        Self {
            secrets,
            config: std::borrow::Cow::Borrowed(config),
            image: image.map(|image| models::split_image_tag(image).0),
        }
    }

    pub fn of_materialization(
        secrets: &'a Stanza,
        endpoint: &'a models::MaterializationEndpoint,
    ) -> Self {
        let (config, image) = match endpoint {
            models::MaterializationEndpoint::Connector(config) => {
                (&config.config, Some(&config.image))
            }
            models::MaterializationEndpoint::Local(config) => (&config.config, None),
            // Dekaf runs in-process under an image name no image answers to.
            models::MaterializationEndpoint::Dekaf(config) => (&config.config, None),
        };
        Self {
            secrets,
            config: std::borrow::Cow::Borrowed(config),
            image: image.map(|image| models::split_image_tag(image).0),
        }
    }

    pub fn of_derivation(secrets: &'a Stanza, using: &'a models::DeriveUsing) -> Self {
        // Built-in derivations run frozen first-party images which will never
        // have first-party secrets, so only an explicit image opts in.
        let image = match using {
            models::DeriveUsing::Connector(config) => {
                Some(models::split_image_tag(&config.image).0)
            }
            _ => None,
        };
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
        Self {
            secrets,
            config,
            image,
        }
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

    // A task may use secrets which are its siblings, or -- when it runs an
    // explicit connector image -- ones named for that image's repository.
    //
    // This mirrors what the reactor will decide at Validate, and is here only
    // for fail-fast ergonomics: the reactor also holds the image to the
    // secrets it actually declares, which publication cannot see.
    let task_prefix = parent_prefix(name).unwrap_or("");

    for secret in ctx.secrets.keys() {
        let secret_prefix = parent_prefix(secret).unwrap_or("");

        if task_prefix == secret_prefix {
            continue;
        }
        // The image rule: `<non-empty prefix>/<repo>/<leaf>`.
        if ctx.image.as_ref().is_some_and(|image| {
            secret_prefix
                .strip_suffix('/')
                .and_then(|prefix| prefix.strip_suffix(image.as_str()))
                .is_some_and(|prefix| prefix.ends_with('/'))
        }) {
            continue;
        }

        match &ctx.image {
            Some(image) => Error::SecretNotSiblingOrImage {
                entity,
                name: name.to_string(),
                secret: secret.to_string(),
                prefix: task_prefix.to_string(),
                image: image.clone(),
            },
            None => Error::SecretNotSibling {
                entity,
                name: name.to_string(),
                secret: secret.to_string(),
                prefix: task_prefix.to_string(),
            },
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
