use std::cmp::max;
use validator::Validate;

/// ControlClaims are claims encoded within control-plane access tokens.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ControlClaims {
    // Note that many more fields, such as additional user metadata,
    // are available if we choose to parse them.

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
#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
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
    #[validate]
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
    #[validate]
    pub prefix: crate::Prefix,
    /// # Name of the data-plane to be authorized.
    #[validate]
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
    #[validate]
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
}

const fn capability_read() -> crate::Capability {
    crate::Capability::Read
}
