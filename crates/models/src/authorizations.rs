use validator::Validate;

/// ControlClaims are claims encoded within control-plane access tokens.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ControlClaims {
    // Note that many more fields, such as additional user metadata,
    // are available if we choose to parse them.
    pub sub: uuid::Uuid,
    pub email: Option<String>,
    pub iat: u64,
    pub exp: u64,
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
    /// The token is capable of LIST and READ for journals
    /// of the requested collection.
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
pub struct UserTaskAuthorizationRequest {
    /// # Task name to be authorized.
    #[validate]
    pub task: crate::Name,
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
    /// The token is capable of LIST, READ, and NETWORK_PROXY of task shards.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reactor_token: String,
    /// # Number of milliseconds to wait before retrying the request.
    /// Non-zero if and only if token is not set.
    pub retry_millis: u64,
    /// # Prefix of task Shard IDs.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub shard_id_prefix: String,
}
