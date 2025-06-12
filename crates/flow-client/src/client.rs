use crate::{api_exec, parse_jwt_claims};
use anyhow::Context;
use models::authorizations::ControlClaims;
use url::Url;

/// Client encapsulates sub-clients for various control-plane
/// and data-plane  services that `flowctl` interacts with.
#[derive(Clone)]
pub struct Client {
    // URL of the control-plane agent API.
    agent_endpoint: url::Url,
    // HTTP client to use for REST requests.
    http_client: reqwest::Client,
    // User's access token, if authenticated.
    user_access_token: Option<String>,
    // Base shard client which is cloned to build token-specific clients.
    shard_client: gazette::shard::Client,
    // Base journal client which is cloned to build token-specific clients.
    journal_client: gazette::journal::Client,
    // Keep a single Postgrest and hand out clones of it in order to maintain
    // a single connection pool. The clones can have different headers while
    // still re-using the same connection pool, so this will work across refreshes.
    pg_parent: postgrest::Postgrest,
}

impl Client {
    /// Build a new Client from the Config.
    pub fn new(
        agent_endpoint: Url,
        pg_api_token: String,
        pg_url: Url,
        user_access_token: Option<String>,
    ) -> Self {
        // Build journal and shard clients with an empty default service address.
        // We'll use their with_endpoint_and_metadata() routines to cheaply clone
        // new clients using dynamic addresses and access tokens, while re-using
        // underlying connections.
        let router = gazette::Router::new("local");

        let journal_client = gazette::journal::Client::new(
            String::new(),
            gazette::Metadata::default(),
            router.clone(),
        );
        let shard_client = gazette::shard::Client::new(
            String::new(),
            gazette::Metadata::default(),
            router.clone(),
        );

        Self {
            agent_endpoint,
            http_client: reqwest::Client::new(),
            pg_parent: postgrest::Postgrest::new(pg_url.as_str())
                .insert_header("apikey", pg_api_token.as_str()),
            journal_client,
            shard_client,
            user_access_token,
        }
    }

    pub fn with_user_access_token(self, user_access_token: Option<String>) -> Self {
        Self {
            user_access_token,
            ..self
        }
    }

    /// Build a fresh `gazette::journal::Client` and `gazette::shard::Client`
    /// There is a bug that causes these clients to hang under heavy/varied load,
    /// so until that bug is found+fixed, this is the work-around.
    #[deprecated]
    pub fn with_fresh_gazette_client(self) -> Self {
        let router = gazette::Router::new("local");

        let journal_client = gazette::journal::Client::new(
            String::new(),
            gazette::Metadata::default(),
            router.clone(),
        );
        let shard_client = gazette::shard::Client::new(
            String::new(),
            gazette::Metadata::default(),
            router.clone(),
        );
        Self {
            journal_client,
            shard_client,
            ..self
        }
    }

    pub fn pg_client(&self) -> postgrest::Postgrest {
        if let Some(token) = &self.user_access_token {
            return self
                .pg_parent
                .clone()
                .insert_header("Authorization", &format!("Bearer {token}"));
        }

        self.pg_parent.clone()
    }

    pub fn from(&self, table: &str) -> postgrest::Builder {
        self.pg_client().from(table)
    }

    pub fn rpc(&self, function: &str, params: String) -> postgrest::Builder {
        self.pg_client().rpc(function, params)
    }

    pub fn is_authenticated(&self) -> bool {
        self.user_access_token.is_some()
    }

    /// Performs a GET request to the control-plane agent HTTP API and returns a
    /// result with either the deserialized response or an error.
    pub async fn api_get<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        raw_query: &[(String, String)],
    ) -> anyhow::Result<T> {
        let url = self.agent_endpoint.join(path)?;
        let mut builder = self.http_client.get(url).query(raw_query);
        if let Some(token) = &self.user_access_token {
            builder = builder.bearer_auth(token);
        }
        let request = builder.build()?;
        tracing::debug!(url = %request.url(), method = "GET", "sending request");

        let response = self.http_client.execute(request).await?;
        let status = response.status();

        if status.is_success() {
            Ok(response.json().await?)
        } else {
            let body = response.text().await?;
            anyhow::bail!("GET {path}: {status}: {body}");
        }
    }

    pub async fn agent_unary<Request, Response>(
        &self,
        path: &str,
        request: &Request,
    ) -> anyhow::Result<Response>
    where
        Request: serde::Serialize,
        Response: serde::de::DeserializeOwned,
    {
        let mut builder = self
            .http_client
            .post(self.agent_endpoint.join(path)?)
            .json(request);

        if let Some(token) = &self.user_access_token {
            builder = builder.bearer_auth(token);
        }

        let response = self.http_client.execute(builder.build()?).await?;
        let status = response.status();

        if status.is_success() {
            Ok(response.json().await?)
        } else {
            let body = response.text().await?;
            anyhow::bail!("POST {path}: {status}: {body}");
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct RefreshToken {
    pub id: models::Id,
    pub secret: String,
}

#[tracing::instrument(skip(client, data_plane_signer), err)]
pub async fn fetch_task_authorization(
    client: &Client,
    shard_template_id: &str,
    data_plane_fqdn: &str,
    data_plane_signer: &jsonwebtoken::EncodingKey,
    capability: u32,
    selector: gazette::broker::LabelSelector,
) -> anyhow::Result<(gazette::journal::Client, proto_gazette::Claims)> {
    let request_token = build_task_authorization_request_token(
        shard_template_id,
        data_plane_fqdn,
        data_plane_signer,
        capability,
        selector,
    )?;

    let models::authorizations::TaskAuthorization {
        broker_address,
        token,
        retry_millis: _,
    } = loop {
        let response: models::authorizations::TaskAuthorization = client
            .agent_unary(
                "/authorize/task",
                &models::authorizations::TaskAuthorizationRequest {
                    token: request_token.clone(),
                },
            )
            .await?;

        if response.retry_millis != 0 {
            tracing::warn!(
                secs = response.retry_millis as f64 / 1000.0,
                "authorization service tentatively rejected our request, but will retry before failing"
            );
            () = tokio::time::sleep(std::time::Duration::from_millis(response.retry_millis)).await;
            continue;
        }
        break response;
    };

    tracing::debug!(broker_address, "resolved task data-plane and authorization");

    let parsed_claims: proto_gazette::Claims = parse_jwt_claims(&token)?;

    let mut md = gazette::Metadata::default();
    md.bearer_token(&token)?;

    let journal_client = client
        .journal_client
        .with_endpoint_and_metadata(broker_address, md);

    Ok((journal_client, parsed_claims))
}

pub fn build_task_authorization_request_token(
    shard_template_id: &str,
    data_plane_fqdn: &str,
    data_plane_signer: &jsonwebtoken::EncodingKey,
    capability: u32,
    selector: gazette::broker::LabelSelector,
) -> anyhow::Result<String> {
    let access_token_claims = proto_gazette::Claims {
        cap: capability,
        sub: shard_template_id.to_string(),
        iss: data_plane_fqdn.to_string(),
        iat: jsonwebtoken::get_current_timestamp(),
        exp: jsonwebtoken::get_current_timestamp() + 60 * 60,
        sel: selector,
    };

    let signed_request_token = jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &access_token_claims,
        &data_plane_signer,
    )?;

    Ok(signed_request_token)
}

#[tracing::instrument(skip(client), err)]
pub async fn fetch_user_task_authorization(
    client: &Client,
    task: &str,
) -> anyhow::Result<(
    String,
    String,
    String,
    gazette::shard::Client,
    gazette::journal::Client,
)> {
    let started_unix = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let models::authorizations::UserTaskAuthorization {
        broker_address,
        broker_token,
        ops_logs_journal,
        ops_stats_journal,
        reactor_address,
        reactor_token,
        shard_id_prefix,
        retry_millis: _,
    } = loop {
        let response: models::authorizations::UserTaskAuthorization = client
            .agent_unary(
                "/authorize/user/task",
                &models::authorizations::UserTaskAuthorizationRequest {
                    started_unix,
                    task: models::Name::new(task),
                    capability: models::Capability::Read,
                },
            )
            .await?;

        if response.retry_millis != 0 {
            tracing::warn!(
                secs = response.retry_millis as f64 / 1000.0,
                "authorization service tentatively rejected our request, but will retry before failing"
            );
            () = tokio::time::sleep(std::time::Duration::from_millis(response.retry_millis)).await;
            continue;
        }
        break response;
    };

    tracing::debug!(
        broker_address,
        broker_token,
        ops_logs_journal,
        ops_stats_journal,
        reactor_address,
        reactor_token,
        shard_id_prefix,
        "resolved task data-plane and authorization"
    );

    let mut md = gazette::Metadata::default();
    md.bearer_token(&reactor_token)?;

    let shard_client = client
        .shard_client
        .with_endpoint_and_metadata(reactor_address, md);

    let mut md = gazette::Metadata::default();
    md.bearer_token(&broker_token)?;

    let journal_client = client
        .journal_client
        .with_endpoint_and_metadata(broker_address, md);

    Ok((
        shard_id_prefix,
        ops_logs_journal,
        ops_stats_journal,
        shard_client,
        journal_client,
    ))
}

#[tracing::instrument(skip(client), err)]
pub async fn fetch_user_collection_authorization(
    client: &Client,
    collection: &str,
    admin: bool,
) -> anyhow::Result<(String, gazette::journal::Client)> {
    let started_unix = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let models::authorizations::UserCollectionAuthorization {
        broker_address,
        broker_token,
        journal_name_prefix,
        retry_millis: _,
    } = loop {
        let response: models::authorizations::UserCollectionAuthorization = client
            .agent_unary(
                "/authorize/user/collection",
                &models::authorizations::UserCollectionAuthorizationRequest {
                    started_unix,
                    collection: models::Collection::new(collection),
                    capability: if admin {
                        models::Capability::Admin
                    } else {
                        models::Capability::Read
                    },
                },
            )
            .await?;

        if response.retry_millis != 0 {
            tracing::warn!(
                secs = response.retry_millis as f64 / 1000.0,
                "authorization service tentatively rejected our request, but will retry before failing"
            );
            () = tokio::time::sleep(std::time::Duration::from_millis(response.retry_millis)).await;
            continue;
        }
        break response;
    };

    tracing::debug!(
        broker_address,
        broker_token,
        journal_name_prefix,
        "resolved collection data-plane and authorization"
    );

    let mut md = gazette::Metadata::default();
    md.bearer_token(&broker_token)?;

    let journal_client = client
        .journal_client
        .with_endpoint_and_metadata(broker_address, md);

    Ok((journal_name_prefix, journal_client))
}

#[tracing::instrument(skip(client), err)]
pub async fn fetch_user_prefix_authorization(
    client: &Client,
    mut request: models::authorizations::UserPrefixAuthorizationRequest,
) -> anyhow::Result<models::authorizations::UserPrefixAuthorization> {
    if request.started_unix == 0 {
        request.started_unix = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    loop {
        let response: models::authorizations::UserPrefixAuthorization = client
            .agent_unary("/authorize/user/prefix", &request)
            .await?;

        if response.retry_millis != 0 {
            tracing::warn!(
                secs = response.retry_millis as f64 / 1000.0,
                "authorization service tentatively rejected our request, but we'll retry before failing"
            );
            () = tokio::time::sleep(std::time::Duration::from_millis(response.retry_millis)).await;
            continue;
        }
        return Ok(response);
    }
}

pub async fn refresh_authorizations(
    client: &Client,
    access_token: Option<String>,
    refresh_token: Option<RefreshToken>,
) -> anyhow::Result<(String, RefreshToken)> {
    // Clear expired or soon-to-expire access token
    let access_token = if let Some(token) = &access_token {
        let claims: ControlClaims = parse_jwt_claims(token.as_str())?;

        // Refresh access tokens with plenty of time to spare if we have a
        // refresh token. If not, allow refreshing right until the token expires
        match (claims.time_remaining().whole_seconds(), &refresh_token) {
            (exp_seconds, Some(_)) if exp_seconds < 60 => None,
            (exp_seconds, None) if exp_seconds <= 0 => None,
            _ => Some(token.to_owned()),
        }
    } else {
        None
    };

    match (access_token, refresh_token) {
        (Some(access), Some(refresh)) => {
            // Authorization is current: nothing to do.
            Ok((access, refresh))
        }
        (Some(access), None) => {
            // We have an access token but no refresh token. Create one.
            let refresh_token = api_exec::<RefreshToken>(
                client.clone().with_user_access_token(Some(access.to_owned())).rpc(
                    "create_refresh_token",
                    serde_json::json!({"multi_use": true, "valid_for": "90d", "detail": "Created by flowctl"})
                        .to_string(),
                ),
            )
            .await?;

            tracing::info!("created new refresh token");
            Ok((access, refresh_token))
        }
        (None, Some(RefreshToken { id, secret })) => {
            // We have a refresh token but no access token. Generate one.

            #[derive(serde::Deserialize)]
            struct Response {
                access_token: String,
                refresh_token: Option<RefreshToken>, // Set iff the token was single-use.
            }
            // We either never had an access token, or we had one and it expired,
            // in which case the client may have an invalid access token configured.
            // The `generate_access_token` RPC only needs the provided refresh token
            // for authentication, so we should use an unauthenticated client to make
            // the request.
            let Response {
                access_token,
                refresh_token: next_refresh_token,
            } = api_exec::<Response>(client.clone().with_user_access_token(None).rpc(
                "generate_access_token",
                serde_json::json!({"refresh_token_id": id, "secret": secret}).to_string(),
            ))
            .await
            .context("failed to obtain access token")?;

            tracing::info!("generated a new access token");
            Ok((
                access_token,
                next_refresh_token.unwrap_or(RefreshToken { id, secret }),
            ))
        }
        _ => anyhow::bail!("Client not authenticated"),
    }
}

pub fn client_claims(client: &Client) -> anyhow::Result<ControlClaims> {
    parse_jwt_claims(
        client
            .user_access_token
            .as_ref()
            .ok_or(anyhow::anyhow!("Client is not authenticated"))?
            .as_str(),
    )
}
