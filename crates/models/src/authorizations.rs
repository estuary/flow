use std::cmp::max;
use validator::Validate;

use crate::authz;

/// ControlClaims are claims encoded within control-plane access tokens.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ControlClaims {
    // Note that many more fields, such as additional user metadata,
    // are available if we choose to parse them.

    // Audience for which the token is intended.
    pub aud: String,
    // Unix timestamp, in seconds, at which the token was issued.
    pub iat: u64,
    // Unix timestamp, in seconds, at which the token expires.
    pub exp: u64,
    // Authorized User ID.
    pub sub: uuid::Uuid,
    // PostgreSQL role to be used for the token.
    pub role: String,
    // Authorized user email, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    // Capability bundles limiting which granted capabilities the token may use.
    // Omitted or null means no mask; an empty list means identity only.
    // Bundles are unioned, and the token may exercise only the capability bits
    // of that union.
    //
    // A verifier rejects a token naming a bundle it doesn't know. Bundle names
    // are stable, and a new bundle is accepted by every replica before any
    // minter may name it, so a verifier never legitimately meets an unknown
    // name. Clients decode these as strings, so they needn't know every bundle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_mask: Option<Vec<String>>,
    /// Limits the user's reachable authority to the union of these prefixes.
    /// Omitted or null is unrestricted; an empty list denies all access.
    /// A verifier rejects a token with a prefix that isn't a catalog prefix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_mask: Option<Vec<String>>,
}

impl ControlClaims {
    /// Empty masks still restrict access; only absent masks are unrestricted.
    pub fn is_restricted(&self) -> bool {
        self.capability_mask.is_some() || self.prefix_mask.is_some()
    }

    /// Parses these claims into the authorization subject used by grant
    /// evaluation. A mask with an unknown bundle or an invalid prefix is an
    /// error: the token is malformed, and is not narrowed to its valid parts.
    pub fn parse_subject(&self) -> Result<authz::Subject, String> {
        Ok(authz::Subject {
            user_id: self.sub,
            capability_mask: self
                .capability_mask
                .as_deref()
                .map(authz::parse_capability_mask)
                .transpose()?,
            prefix_mask: self
                .prefix_mask
                .as_deref()
                .map(authz::parse_prefix_mask)
                .transpose()?,
        })
    }

    pub fn time_remaining(&self) -> time::Duration {
        let now = time::OffsetDateTime::now_utc();
        let exp = time::OffsetDateTime::from_unix_timestamp(self.exp as i64).unwrap();

        max(exp - now, time::Duration::ZERO)
    }
}

// Data-plane claims are represented by proto_gazette::Claims,
// which is not re-exported by this crate.

/// TaskAuthorizationRequest is sent by data-plane reactors to request
/// an authorization to a collection which is sourced or produced.
#[derive(
    Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema, validator::Validate,
)]
#[serde(rename_all = "camelCase")]
pub struct TaskAuthorizationRequest {
    /// # JWT token to be authorized and signed.
    /// JWT is signed by the requesting data-plane for authorization of a
    /// task to a collection.
    pub token: String,
}

/// TaskAuthorization is an authorization granted to a task for the purpose of
/// interacting with collection journals which it sources or produces.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskAuthorization {
    /// # JWT token which has been authorized for use.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub token: String,
    /// # Address of Gazette brokers for the issued token.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_address: String,
    /// # Number of milliseconds to wait before retrying the request.
    /// Non-zero if and only if token is not set.
    pub retry_millis: u64,
}

/// UserCollectionAuthorizationRequest requests an authorization to interact
/// with a collection within its data-plane on behalf of a user.
/// It must be accompanied by a control-plane Authorization token.
#[derive(
    Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema, validator::Validate,
)]
#[serde(rename_all = "camelCase")]
pub struct UserCollectionAuthorizationRequest {
    /// # Collection name to be authorized.
    #[validate(nested)]
    pub collection: crate::Collection,
    /// # Requested capability bundle of the authorization.
    #[serde(
        default = "requested_bundle_default",
        deserialize_with = "deserialize_requested_bundle",
        rename = "capability"
    )]
    #[schemars(schema_with = "requested_bundle_schema")]
    pub capability_bundle: authz::CapabilityBundle,
    /// # Unix timestamp, in seconds, at which the operation started.
    /// If this is non-zero, it lower-bounds the time of an authorization
    /// snapshot required to definitively reject an authorization.
    ///
    /// Snapshots taken prior to this time point that reject the request
    /// will return a Response asking for the operation to be retried.
    ///
    /// If zero, the request will block server-side until it can be
    /// definitively rejected.
    #[serde(default)]
    pub started_unix: u64,
}

/// UserCollectionAuthorization is an authorization granted to a user for the
/// purpose of interacting with collection journals within its data-plane.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UserCollectionAuthorization {
    /// # Address of Gazette brokers for the issued token.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_address: String,
    /// # JWT token which has been authorized for use with brokers.
    /// The token is authorized for journal operations of the
    /// requested collection and capability bundle.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_token: String,
    /// # Prefix of collection Journal names.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub journal_name_prefix: String,
    /// # Number of milliseconds to wait before retrying the request.
    /// Non-zero if and only if other fields are not set.
    #[serde(default)]
    pub retry_millis: u64,
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema, validator::Validate,
)]
#[serde(rename_all = "camelCase")]
pub struct UserPrefixAuthorizationRequest {
    /// # Prefix to be authorized.
    #[validate(nested)]
    pub prefix: crate::Prefix,
    /// # Name of the data-plane to be authorized.
    #[validate(nested)]
    pub data_plane: crate::Name,
    /// # Requested capability bundle of the authorization.
    #[serde(
        default = "requested_bundle_default",
        deserialize_with = "deserialize_requested_bundle",
        rename = "capability"
    )]
    #[schemars(schema_with = "requested_bundle_schema")]
    pub capability_bundle: authz::CapabilityBundle,
    /// # Unix timestamp, in seconds, at which the operation started.
    /// This timestamp lower-bounds the time of an authorization
    /// snapshot required to definitively reject an authorization.
    ///
    /// Snapshots taken prior to this time point that reject the request
    /// will return a Response asking for the operation to be retried.
    pub started_unix: u64,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UserPrefixAuthorization {
    /// # Address of Gazette brokers for the issued token.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_address: String,
    /// # JWT token which has been authorized for use with brokers.
    /// The token is authorized for journal operations over the
    /// requested prefix and capability bundle.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_token: String,
    /// # Address of Reactors for the issued token.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reactor_address: String,
    /// # JWT token which has been authorized for use with reactors.
    /// The token is authorized for shard operations over the
    /// requested prefix and capability bundle.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reactor_token: String,
    /// # Number of milliseconds to wait before retrying the request.
    /// Non-zero if and only if token is not set.
    pub retry_millis: u64,
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema, validator::Validate,
)]
#[serde(rename_all = "camelCase")]
pub struct UserTaskAuthorizationRequest {
    /// # Task name to be authorized.
    #[validate(nested)]
    pub task: crate::Name,
    /// # Requested capability bundle of the authorization.
    #[serde(
        default = "requested_bundle_default",
        deserialize_with = "deserialize_requested_bundle",
        rename = "capability"
    )]
    #[schemars(schema_with = "requested_bundle_schema")]
    pub capability_bundle: authz::CapabilityBundle,
    /// # Unix timestamp, in seconds, at which the operation started.
    /// If this is non-zero, it lower-bounds the time of an authorization
    /// snapshot required to definitively reject an authorization.
    ///
    /// Snapshots taken prior to this time point that reject the request
    /// will return a Response asking for the operation to be retried.
    ///
    /// If zero, the request will block server-side until it can be
    /// definitively rejected.
    #[serde(default)]
    pub started_unix: u64,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UserTaskAuthorization {
    /// # Address of Gazette brokers for the issued token.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_address: String,
    /// # JWT token which has been authorized for use with brokers.
    /// The token is capable of LIST and READ of task ops journals.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_token: String,
    /// # Name of the journal holding task logs.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ops_logs_journal: String,
    /// # Name of the journal holding task stats.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ops_stats_journal: String,
    /// # Address of Reactors for the issued token.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reactor_address: String,
    /// # JWT token which has been authorized for use with reactors.
    /// The token is authorized for shard operations of the
    /// requested task and capability bundle.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reactor_token: String,
    /// # Number of milliseconds to wait before retrying the request.
    /// Non-zero if and only if token is not set.
    pub retry_millis: u64,
    /// # Prefix of task Shard IDs.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub shard_id_prefix: String,
}

/// UserDecryptAuthorizationRequest asks for authority to decrypt a named secret,
/// authorized by a user's accompanying control-plane bearer token.
#[derive(
    Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema, validator::Validate,
)]
#[serde(rename_all = "camelCase")]
pub struct UserDecryptAuthorizationRequest {
    /// # Catalog name of the secret.
    #[validate(nested)]
    pub name: crate::Name,
}

/// DecryptAuthorization is an authorized disclosure of a secret's wrapped
/// document, granted to a user or task subject who may decrypt it. The document
/// remains sops-wrapped in this response under a key held only by
/// config-encryption, which is the intended recipient of this response and
/// proxies the decryption back to the user.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DecryptAuthorization {
    /// # The sops-wrapped document of the secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document: Option<serde_json::Value>,
    /// # Lifecycle identity of the disclosed document.
    /// Every change to a secret mints a new `secretId`, and ids are
    /// time-ordered, so comparing two observations tells you which is newer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_id: Option<crate::Id>,
    /// # Number of milliseconds to wait before retrying the request.
    /// Non-zero if and only if `document` is not set.
    pub retry_millis: u64,
}

/// SecretDecryption is config-encryption's response to `/secret/decrypt`:
/// the plaintext value of a secret, having been authorized by the control-plane
/// and unwrapped under the KMS key which only config-encryption holds.
#[derive(Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretDecryption {
    /// # Decrypted value of the secret.
    #[serde(
        default,
        deserialize_with = "deserialize_secret_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub value: Option<serde_json::Value>,
    /// # Lifecycle identity of the decrypted document.
    /// Every change to a secret mints a new `secretId`, and ids are
    /// time-ordered, so comparing two observations tells you which is newer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_id: Option<crate::Id>,
    /// # Number of milliseconds to wait before retrying the request.
    /// Non-zero if and only if `value` is not set. It is passed through from
    /// the control-plane, which alone decides when a denial becomes terminal.
    pub retry_millis: u64,
}

fn deserialize_secret_value<'de, D>(deserializer: D) -> Result<Option<serde_json::Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // Only an absent field means no value (via serde's default). A present
    // JSON null is a valid secret and must survive the transport as Some(Null).
    <serde_json::Value as serde::Deserialize>::deserialize(deserializer).map(Some)
}

impl std::fmt::Debug for SecretDecryption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `value` is plaintext, and is deliberately not rendered.
        f.debug_struct("SecretDecryption")
            .field("value", &self.value.as_ref().map(|_| "<redacted>"))
            .field("secret_id", &self.secret_id)
            .field("retry_millis", &self.retry_millis)
            .finish()
    }
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DekafAuthResponse {
    /// # Control plane access token with the requested role
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub token: String,
    // Name of the journal that contains the logs for the specified task
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ops_logs_journal: String,
    // Name of the journal that contains the stats for the specified task
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ops_stats_journal: String,
    // The built spec of the materialization. This is actually proto_flow::flow::MaterializationSpec
    // but we can't depend on `proto_flow` here, so `RawValue` it is
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_spec: Option<crate::RawValue>,
    /// # Number of milliseconds to wait before retrying the request.
    /// Non-zero if and only if token is not set.
    pub retry_millis: u64,
    /// # Target dataplane FQDN for redirect when task has been migrated
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redirect_dataplane_fqdn: Option<String>,
    /// # Target Dekaf Kafka URI for redirect when task has been migrated.
    /// Used for Kafka protocol redirects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redirect_dekaf_address: Option<String>,
    /// # Target redirect Dekaf instance's schema registry URL.
    /// Used for serving schema registry HTTP redirects when a task has been migrated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redirect_dekaf_registry_address: Option<String>,
}

/// TaskSetSecretRequest asks the control-plane to store a wrapped secret
/// document on behalf of the task named by `token`, which is how a connector
/// rotates a credential it manages.
#[derive(
    Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema, validator::Validate,
)]
#[serde(rename_all = "camelCase")]
pub struct TaskSetSecretRequest {
    /// # Catalog name of the secret to set (a sibling of the task).
    #[validate(nested)]
    pub name: crate::Name,
    /// # The sops-wrapped document to store.
    pub document: serde_json::Value,
    /// # JWT token signed by the requesting data-plane.
    /// It carries TASK_UPDATE and names the task in its selector.
    pub token: String,
}

/// TaskSetSecret is the outcome of [`TaskSetSecretRequest`].
#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskSetSecret {
    /// # Lifecycle identity of the secret's post-set document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_id: Option<crate::Id>,
    /// # Whether the document changed.
    /// False when the provided document was structurally identical to the
    /// stored one, in which case `secretId` is the id the secret already had.
    pub changed: bool,
    /// # Number of milliseconds to wait before retrying the request.
    /// Non-zero if and only if `secretId` is not set.
    pub retry_millis: u64,
}

/// TaskUpdateConfigRequest asks the control-plane to record a new endpoint
/// configuration and `secrets` stanza for the task named by `token`.
#[derive(
    Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema, validator::Validate,
)]
#[serde(rename_all = "camelCase")]
pub struct TaskUpdateConfigRequest {
    /// # Complete plaintext endpoint configuration.
    pub config: crate::RawValue,
    /// # Complete `secrets` stanza: secret catalog names to JSON pointers.
    pub secrets: std::collections::BTreeMap<crate::Secret, crate::JsonPointer>,
    /// # Message presented to users, which may use Markdown formatting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// # JWT token signed by the requesting data-plane.
    /// It carries TASK_UPDATE, names the task in its selector, and
    /// must name the build the task is currently running.
    pub token: String,
}

/// TaskUpdateConfig is the outcome of [`TaskUpdateConfigRequest`]: the update
/// was recorded, and the task's controller has been woken to publish it.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskUpdateConfig {
    /// # Number of milliseconds to wait before retrying the request.
    /// Non-zero if and only if the update was not recorded.
    pub retry_millis: u64,
}

const fn requested_bundle_default() -> authz::CapabilityBundle {
    authz::CapabilityBundle::Viewer
}

// Deployed clients request `read`, `write`, or `admin`: the legacy
// tiers. `admin` is also a bundle name, and `read` and `write` are old names
// of the `viewer` and `writer` bundles. Only these requests accept the old
// names. A token capability mask rejects them (`authz::parse_capability_mask`).
fn deserialize_requested_bundle<'de, D>(
    deserializer: D,
) -> Result<authz::CapabilityBundle, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let name = <String as serde::Deserialize>::deserialize(deserializer)?;

    match name.as_str() {
        "read" => Ok(authz::CapabilityBundle::Viewer),
        "write" => Ok(authz::CapabilityBundle::Writer),
        name => authz::parse_capability_bundle(name).map_err(serde::de::Error::custom),
    }
}

fn requested_bundle_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let names: Vec<&str> = authz::public_bundles()
        .map(<&str>::from)
        .chain(["read", "write"])
        .collect();

    serde_json::from_value(serde_json::json!({
        "type": "string",
        "enum": names,
        "description": "Name of a capability bundle. `read` and `write` are \
            deprecated names of `viewer` and `writer`.",
    }))
    .unwrap()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::authz::{CapabilityBit, CapabilityBits, CapabilityBundle, Subject};

    #[test]
    fn user_authorization_request_capability_bundles() {
        let mut results = Vec::new();
        for name in [
            None,
            Some("read"),
            Some("write"),
            Some("admin"),
            Some("viewer"),
            Some("editor"),
            Some("billing"),
            Some("none"),
            Some("Viewer"),
        ] {
            let mut request = serde_json::json!({
                "collection": "acmeCo/anvils",
                "prefix": "acmeCo/",
                "dataPlane": "ops/dp/public/plane-one",
                "task": "acmeCo/source-anvils",
                "startedUnix": 0,
            });
            if let Some(name) = name {
                request["capability"] = name.into();
            }
            let collection =
                serde_json::from_value::<UserCollectionAuthorizationRequest>(request.clone())
                    .map(|request| request.capability_bundle);
            let prefix = serde_json::from_value::<UserPrefixAuthorizationRequest>(request.clone())
                .map(|request| request.capability_bundle);
            let task = serde_json::from_value::<UserTaskAuthorizationRequest>(request)
                .map(|request| request.capability_bundle);

            assert_eq!(
                (prefix.as_ref().ok(), task.as_ref().ok()),
                (collection.as_ref().ok(), collection.as_ref().ok()),
                "requests parse {name:?} alike",
            );
            results.push((name, collection.map_err(|err| err.to_string())));
        }
        insta::assert_json_snapshot!(results, @r#"
        [
          [
            null,
            {
              "Ok": "viewer"
            }
          ],
          [
            "read",
            {
              "Ok": "viewer"
            }
          ],
          [
            "write",
            {
              "Ok": "writer"
            }
          ],
          [
            "admin",
            {
              "Ok": "admin"
            }
          ],
          [
            "viewer",
            {
              "Ok": "viewer"
            }
          ],
          [
            "editor",
            {
              "Ok": "editor"
            }
          ],
          [
            "billing",
            {
              "Ok": "billing"
            }
          ],
          [
            "none",
            {
              "Err": "Unknown capability bundle \"none\": use any of viewer, writer, editor, admin, billing, team_admin, manage_service_accounts, manage_data_plane, create_sandbox, delegate, assume, view_billing, edit_billing, edit_secret, view_data_plane_private_networking, modify_data_plane_private_networking, catalog_read."
            }
          ],
          [
            "Viewer",
            {
              "Err": "Unknown capability bundle \"Viewer\": use any of viewer, writer, editor, admin, billing, team_admin, manage_service_accounts, manage_data_plane, create_sandbox, delegate, assume, view_billing, edit_billing, edit_secret, view_data_plane_private_networking, modify_data_plane_private_networking, catalog_read."
            }
          ]
        ]
        "#);

        // New clients send the bundle's own name.
        let request = UserCollectionAuthorizationRequest {
            collection: crate::Collection::new("acmeCo/anvils"),
            capability_bundle: CapabilityBundle::Writer,
            started_unix: 0,
        };
        insta::assert_json_snapshot!(request, @r#"
        {
          "collection": "acmeCo/anvils",
          "capability": "writer",
          "startedUnix": 0
        }
        "#);
    }

    #[test]
    fn user_authorization_request_schema() {
        let schema = schemars::schema_for!(UserCollectionAuthorizationRequest);
        insta::assert_json_snapshot!(schema.as_value()["properties"]["capability"], @r#"
        {
          "default": "viewer",
          "description": "Name of a capability bundle. `read` and `write` are deprecated names of `viewer` and `writer`.",
          "enum": [
            "viewer",
            "writer",
            "editor",
            "admin",
            "billing",
            "team_admin",
            "manage_service_accounts",
            "manage_data_plane",
            "create_sandbox",
            "delegate",
            "assume",
            "view_billing",
            "edit_billing",
            "edit_secret",
            "view_data_plane_private_networking",
            "modify_data_plane_private_networking",
            "catalog_read",
            "read",
            "write"
          ],
          "title": "Requested capability bundle of the authorization.",
          "type": "string"
        }
        "#);
    }

    #[test]
    fn restricted_token_masks() {
        let mut results = Vec::new();
        for capability_mask in [None, Some(vec![]), Some(vec!["viewer"])] {
            for prefix_mask in [None, Some(vec![]), Some(vec!["acmeCo/"])] {
                let claims: ControlClaims = serde_json::from_value(serde_json::json!({
                    "aud": "authenticated", "iat": 0, "exp": 0,
                    "sub": uuid::Uuid::nil(), "role": "authenticated",
                    "capability_mask": capability_mask, "prefix_mask": prefix_mask,
                }))
                .unwrap();
                results.push(claims.is_restricted());
            }
        }
        insta::assert_debug_snapshot!(results, @r#"
        [
            false,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
        ]
        "#);
    }

    #[test]
    fn prefix_mask_from_token_payload() {
        let mut payload = serde_json::json!({
            "aud": "authenticated", "iat": 1700000000, "exp": 1700003600,
            "sub": "d4b7c5a0-9e2f-4c1b-8a3d-6f5e4d3c2b1a", "role": "authenticated",
        });
        assert!(
            serde_json::from_value::<ControlClaims>(payload.clone())
                .unwrap()
                .parse_subject()
                .unwrap()
                .prefix_mask
                .is_none()
        );
        let mut results = Vec::new();
        for prefix_mask in [
            serde_json::Value::Null,
            serde_json::json!([]),
            serde_json::json!(["acmeCo/team/", "otherCo/"]),
            serde_json::json!(["acmeCo/team/", "acmeCo/", "acmeCo/team/"]),
            serde_json::json!(["acmeCo/teammate/", "acmeCo/team/nested/", "acmeCo/team/"]),
            // Any invalid prefix fails the whole mask.
            serde_json::json!([""]),
            serde_json::json!(["acmeCo/team", "otherCo/"]),
            serde_json::json!(["otherCo/", "/"]),
            serde_json::json!(["otherCo/", "acme Co/"]),
        ] {
            payload["prefix_mask"] = prefix_mask;
            let claims: ControlClaims = serde_json::from_value(payload.clone()).unwrap();
            results.push(claims.parse_subject().map(|subject| subject.prefix_mask));
            let round_trip: ControlClaims =
                serde_json::from_value(serde_json::to_value(&claims).unwrap()).unwrap();
            assert_eq!(round_trip.parse_subject(), claims.parse_subject());
        }
        insta::assert_json_snapshot!(results, @r#"
        [
          {
            "Ok": null
          },
          {
            "Ok": []
          },
          {
            "Ok": [
              "acmeCo/team/",
              "otherCo/"
            ]
          },
          {
            "Ok": [
              "acmeCo/"
            ]
          },
          {
            "Ok": [
              "acmeCo/team/",
              "acmeCo/teammate/"
            ]
          },
          {
            "Err": "Invalid prefix \"\": use a nonempty catalog prefix ending in '/'."
          },
          {
            "Err": "Invalid prefix \"acmeCo/team\": use a nonempty catalog prefix ending in '/'."
          },
          {
            "Err": "Invalid prefix \"/\": use a nonempty catalog prefix ending in '/'."
          },
          {
            "Err": "Invalid prefix \"acme Co/\": use a nonempty catalog prefix ending in '/'."
          }
        ]
        "#);
        for malformed in [
            serde_json::json!("acmeCo/"),
            serde_json::json!(42),
            serde_json::json!([null]),
            serde_json::json!([42]),
        ] {
            payload["prefix_mask"] = malformed;
            assert!(serde_json::from_value::<ControlClaims>(payload.clone()).is_err());
        }
    }

    #[test]
    fn subject_from_deserialized_token_payload() {
        let user_id = uuid::Uuid::parse_str("d4b7c5a0-9e2f-4c1b-8a3d-6f5e4d3c2b1a").unwrap();
        let viewer = CapabilityBundle::Viewer.bits();

        let cases: &[(&str, Option<CapabilityBits>)] = &[
            // No claim at all: unmasked.
            (
                r#"{
                    "aud": "authenticated",
                    "iat": 1700000000,
                    "exp": 1700003600,
                    "sub": "d4b7c5a0-9e2f-4c1b-8a3d-6f5e4d3c2b1a",
                    "role": "authenticated",
                    "email": "user@example.com"
                }"#,
                None,
            ),
            // Explicit null is equivalent to omission.
            (
                r#"{
                    "aud": "authenticated",
                    "iat": 1700000000,
                    "exp": 1700003600,
                    "sub": "d4b7c5a0-9e2f-4c1b-8a3d-6f5e4d3c2b1a",
                    "role": "authenticated",
                    "capability_mask": null
                }"#,
                None,
            ),
            // Empty list: masked to nothing.
            (
                r#"{
                    "aud": "authenticated",
                    "iat": 1700000000,
                    "exp": 1700003600,
                    "sub": "d4b7c5a0-9e2f-4c1b-8a3d-6f5e4d3c2b1a",
                    "role": "authenticated",
                    "capability_mask": []
                }"#,
                Some(CapabilityBits::empty()),
            ),
            // Bundles are unioned.
            (
                r#"{
                    "aud": "authenticated",
                    "iat": 1700000000,
                    "exp": 1700003600,
                    "sub": "d4b7c5a0-9e2f-4c1b-8a3d-6f5e4d3c2b1a",
                    "role": "authenticated",
                    "capability_mask": ["viewer", "delegate"]
                }"#,
                Some(viewer | CapabilityBit::Delegate),
            ),
            (
                r#"{
                    "aud": "authenticated",
                    "iat": 1700000000,
                    "exp": 1700003600,
                    "sub": "d4b7c5a0-9e2f-4c1b-8a3d-6f5e4d3c2b1a",
                    "role": "authenticated",
                    "capability_mask": ["create_sandbox"]
                }"#,
                Some(CapabilityBit::CreateSandbox.into()),
            ),
            // Unrelated claims that we don't model are tolerated.
            (
                r#"{
                    "aud": "authenticated",
                    "iat": 1700000000,
                    "exp": 1700003600,
                    "sub": "d4b7c5a0-9e2f-4c1b-8a3d-6f5e4d3c2b1a",
                    "role": "authenticated",
                    "session_id": "6c0e1f2a-3b4c-4d5e-8f60-718293a4b5c6",
                    "app_metadata": {"provider": "google"},
                    "capability_mask": ["team_admin"]
                }"#,
                Some(CapabilityBundle::TeamAdmin.bits()),
            ),
            // Verifying deduplication
            (
                r#"{
                    "aud": "authenticated",
                    "iat": 1700000000,
                    "exp": 1700003600,
                    "sub": "d4b7c5a0-9e2f-4c1b-8a3d-6f5e4d3c2b1a",
                    "role": "authenticated",
                    "session_id": "6c0e1f2a-3b4c-4d5e-8f60-718293a4b5c6",
                    "app_metadata": {"provider": "google"},
                    "capability_mask": ["team_admin", "team_admin"]
                }"#,
                Some(CapabilityBundle::TeamAdmin.bits()),
            ),
        ];

        for (payload, expected_mask) in cases {
            let claims: ControlClaims = serde_json::from_str(payload).unwrap();
            assert_eq!(
                claims.parse_subject(),
                Ok(Subject {
                    user_id,
                    capability_mask: *expected_mask,
                    prefix_mask: None,
                }),
                "{payload}"
            );
        }

        // An unknown name fails the whole mask, even alongside known names.
        // So do names in any case but snake_case.
        let mut rejections = Vec::new();
        for mask in [
            serde_json::json!(["future_bundle"]),
            serde_json::json!(["viewer", "delegate", "future_bundle"]),
            serde_json::json!(["TeamAdmin"]),
        ] {
            let claims: ControlClaims = serde_json::from_value(serde_json::json!({
                "aud": "authenticated", "iat": 0, "exp": 0,
                "sub": user_id, "role": "authenticated", "capability_mask": mask,
            }))
            .unwrap();
            rejections.push(claims.parse_subject().unwrap_err());
        }
        insta::assert_json_snapshot!(rejections, @r#"
        [
          "Unknown capability bundle \"future_bundle\": use any of viewer, writer, editor, admin, billing, team_admin, manage_service_accounts, manage_data_plane, create_sandbox, delegate, assume, view_billing, edit_billing, edit_secret, view_data_plane_private_networking, modify_data_plane_private_networking, catalog_read.",
          "Unknown capability bundle \"future_bundle\": use any of viewer, writer, editor, admin, billing, team_admin, manage_service_accounts, manage_data_plane, create_sandbox, delegate, assume, view_billing, edit_billing, edit_secret, view_data_plane_private_networking, modify_data_plane_private_networking, catalog_read.",
          "Unknown capability bundle \"TeamAdmin\": use any of viewer, writer, editor, admin, billing, team_admin, manage_service_accounts, manage_data_plane, create_sandbox, delegate, assume, view_billing, edit_billing, edit_secret, view_data_plane_private_networking, modify_data_plane_private_networking, catalog_read."
        ]
        "#);

        // The old names of the `viewer` and `writer` bundles, which user
        // authorization requests accept, are not bundle names.
        let rejections: Vec<_> = ["read", "write"]
            .into_iter()
            .map(|name| authz::parse_capability_mask(&[name.to_string()]).unwrap_err())
            .collect();
        insta::assert_json_snapshot!(rejections, @r#"
        [
          "Unknown capability bundle \"read\": use any of viewer, writer, editor, admin, billing, team_admin, manage_service_accounts, manage_data_plane, create_sandbox, delegate, assume, view_billing, edit_billing, edit_secret, view_data_plane_private_networking, modify_data_plane_private_networking, catalog_read.",
          "Unknown capability bundle \"write\": use any of viewer, writer, editor, admin, billing, team_admin, manage_service_accounts, manage_data_plane, create_sandbox, delegate, assume, view_billing, edit_billing, edit_secret, view_data_plane_private_networking, modify_data_plane_private_networking, catalog_read."
        ]
        "#);

        // A mask that isn't a list of strings is a malformed token.
        let malformed = r#"{
            "aud": "authenticated",
            "iat": 1700000000,
            "exp": 1700003600,
            "sub": "d4b7c5a0-9e2f-4c1b-8a3d-6f5e4d3c2b1a",
            "role": "authenticated",
            "capability_mask": "viewer"
        }"#;
        assert!(serde_json::from_str::<ControlClaims>(malformed).is_err());
    }
}
