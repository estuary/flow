//! Resolution of a task's `secrets` stanza into plaintext values, under either
//! a data-plane's or the current user's authorization.
//!
//! Callers thread one of these `SecretResolver` trait implementations into
//! `unseal::resolve`, to power secret decryption.

/// Resolves a secret for the task identity authorized on a connector stream.
///
/// `image` is the repository of the connector image which is asking, with tag
/// and digest stripped, or None for a local or in-process connector. It's the
/// runtime's attestation of which image runs, and is what lets a secret be
/// admitted under the image rule rather than as a sibling of the task.
///
/// A returned decryption is always a success: its `value` and `secret_id` are
/// set, and its `retry_millis` was consumed while fetching.
#[tonic::async_trait]
pub trait SecretResolver: Send + Sync + 'static {
    async fn decrypt(
        &self,
        task_type: proto_flow::ops::TaskType,
        task_name: &str,
        image: Option<&str>,
        name: models::Secret,
    ) -> anyhow::Result<models::authorizations::SecretDecryption>;
}

/// Resolves secrets on behalf of tasks running in a data plane.
pub struct Task {
    client: crate::rest::Client,
    data_plane_fqdn: String,
    data_plane_signing_key: tokens::jwt::EncodingKey,
}

impl Task {
    pub fn new(
        client: crate::rest::Client,
        data_plane_fqdn: String,
        data_plane_signing_key: tokens::jwt::EncodingKey,
    ) -> Self {
        Self {
            client,
            data_plane_fqdn,
            data_plane_signing_key,
        }
    }
}

#[tonic::async_trait]
impl SecretResolver for Task {
    async fn decrypt(
        &self,
        task_type: proto_flow::ops::TaskType,
        task_name: &str,
        image: Option<&str>,
        name: models::Secret,
    ) -> anyhow::Result<models::authorizations::SecretDecryption> {
        let signed_source = crate::workflows::task_secret_decrypt::new_signed_source(
            &name,
            task_type,
            task_name,
            image,
            self.data_plane_fqdn.clone(),
            self.data_plane_signing_key.clone(),
        );

        Ok(tokens::fetch_once(crate::workflows::TaskSecretDecrypt {
            client: self.client.clone(),
            name,
            signed_source,
        })
        .await
        .map_err(proto_grpc::status_to_anyhow)?)
    }
}

/// Resolves secrets with the current flowctl user's authorization.
pub struct User {
    client: crate::rest::Client,
    user_tokens: tokens::PendingWatch<crate::user_auth::UserToken>,
}

impl User {
    pub fn new(
        client: crate::rest::Client,
        user_tokens: tokens::PendingWatch<crate::user_auth::UserToken>,
    ) -> Self {
        Self {
            client,
            user_tokens,
        }
    }
}

#[tonic::async_trait]
impl SecretResolver for User {
    async fn decrypt(
        &self,
        _task_type: proto_flow::ops::TaskType,
        _task_name: &str,
        // The user path authorizes against the user's own grants, which the
        // image rule has nothing to say about.
        _image: Option<&str>,
        name: models::Secret,
    ) -> anyhow::Result<models::authorizations::SecretDecryption> {
        Ok(tokens::fetch_once(crate::workflows::UserSecretDecrypt {
            client: self.client.clone(),
            user_tokens: self.user_tokens.clone(),
            name,
        })
        .await
        .map_err(proto_grpc::status_to_anyhow)?)
    }
}

/// Fails every secret, for local contexts which hold no control-plane
/// credentials with which to decrypt one -- chiefly tests.
pub struct NoOp;

#[tonic::async_trait]
impl SecretResolver for NoOp {
    async fn decrypt(
        &self,
        _task_type: proto_flow::ops::TaskType,
        _task_name: &str,
        _image: Option<&str>,
        name: models::Secret,
    ) -> anyhow::Result<models::authorizations::SecretDecryption> {
        anyhow::bail!("secret '{name}' cannot be resolved in this local test context")
    }
}
