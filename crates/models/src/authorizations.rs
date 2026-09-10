use std::cmp::max;
use validator::Validate;

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
    // Catalog prefix confining this token's authority, if any.
    //
    // Every authorization decision made with the token is intersected with the
    // authority reachable from this prefix through role grants, so the token can
    // only ever do less than the user could do unscoped. Named `scope_prefix`
    // rather than `scope` to avoid colliding with the OAuth 2.0 `scope` claim,
    // which is a space-delimited list of scope strings and means something else.
    //
    // The claim carries only the prefix, never a materialized list of authorized
    // prefixes. Authority is still derived from the grant tables at request time,
    // so revoking a grant takes effect on the next Snapshot regardless of how
    // long the token lives; freezing the scope for a token's lifetime is safe
    // because a scope can only narrow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_prefix: Option<String>,
}

impl ControlClaims {
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
    /// # Requested capability level of the authorization.
    #[serde(default = "capability_read")]
    pub capability: crate::Capability,
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
    /// requested collection and capability.
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
    /// # Requested capability level of the authorization.
    #[serde(default = "capability_read")]
    pub capability: crate::Capability,
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
    /// requested prefix and capability.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub broker_token: String,
    /// # Address of Reactors for the issued token.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reactor_address: String,
    /// # JWT token which has been authorized for use with reactors.
    /// The token is authorized for shard operations over the
    /// requested prefix and capability.
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
    /// # Requested capability level of the authorization.
    #[serde(default = "capability_read")]
    pub capability: crate::Capability,
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
    /// requested task and capability.
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
    pub document: Option<crate::RawValue>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<crate::RawValue>,
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

const fn capability_read() -> crate::Capability {
    crate::Capability::Read
}
