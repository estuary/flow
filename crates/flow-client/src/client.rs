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
    // PostgREST URL.
    pg_url: url::Url,
    // PostgREST access token.
    pg_token: String,
    // User's access token, if authenticated.
    user_access_token: Option<String>,
    // User's refresh token, if authenticated.
    user_refresh_token: Option<RefreshToken>,
    // Base shard client which is cloned to build token-specific clients.
    shard_client: gazette::shard::Client,
    // Base journal client which is cloned to build token-specific clients.
    journal_client: gazette::journal::Client,
}

impl Client {
    /// Build a new Client from the Config.
    pub fn new(
        agent_endpoint: Url,
        pg_token: String,
        pg_url: Url,
        access_token: Option<String>,
        refresh_token: Option<RefreshToken>,
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
            pg_token,
            pg_url,
            journal_client,
            shard_client,
            user_access_token: access_token,
            user_refresh_token: refresh_token,
        }
    }

    pub async fn refresh(&mut self) -> anyhow::Result<()> {
        // Clear expired or soon-to-expire access token
        if let Some(_) = &self.user_access_token {
            let claims = self.claims()?;

            let now = time::OffsetDateTime::now_utc();
            let exp = time::OffsetDateTime::from_unix_timestamp(claims.exp as i64).unwrap();

            // Refresh access tokens with plenty of time to spare if we have a
            // refresh token. If not, allow refreshing right until the token expires
            match ((now - exp).whole_seconds(), &self.user_refresh_token) {
                (exp_seconds, Some(_)) if exp_seconds < 60 => self.user_access_token = None,
                (exp_seconds, None) if exp_seconds <= 0 => self.user_access_token = None,
                _ => {}
            }
        }

        if self.user_access_token.is_some() && self.user_refresh_token.is_some() {
            // Authorization is current: nothing to do.
            Ok(())
        } else if self.user_access_token.is_some() {
            // We have an access token but no refresh token. Create one.
            let refresh_token = api_exec::<RefreshToken>(
                self.rpc(
                    "create_refresh_token",
                    serde_json::json!({"multi_use": true, "valid_for": "90d", "detail": "Created by flowctl"})
                        .to_string(),
                ),
            )
            .await?;

            self.user_refresh_token = Some(refresh_token);

            tracing::info!("created new refresh token");
            Ok(())
        } else if let Some(RefreshToken { id, secret }) = &self.user_refresh_token {
            // We have a refresh token but no access token. Generate one.

            #[derive(serde::Deserialize)]
            struct Response {
                access_token: String,
                refresh_token: Option<RefreshToken>, // Set iff the token was single-use.
            }
            let Response {
                access_token,
                refresh_token: next_refresh_token,
            } = api_exec::<Response>(self.rpc(
                "generate_access_token",
                serde_json::json!({"refresh_token_id": id, "secret": secret}).to_string(),
            ))
            .await
            .context("failed to obtain access token")?;

            if next_refresh_token.is_some() {
                self.user_refresh_token = next_refresh_token;
            }

            self.user_access_token = Some(access_token);

            tracing::info!("generated a new access token");
            Ok(())
        } else {
            anyhow::bail!("Client not authenticated");
        }
    }

    pub fn pg_client(&self) -> postgrest::Postgrest {
        let pg_client = postgrest::Postgrest::new(self.pg_url.as_str())
            .insert_header("apikey", self.pg_token.as_str());

        if let Some(token) = &self.user_access_token {
            return pg_client.insert_header("Authorization", &format!("Bearer {token}"));
        }

        pg_client
    }

    pub fn claims(&self) -> anyhow::Result<ControlClaims> {
        parse_jwt_claims(
            self.user_access_token
                .as_ref()
                .ok_or(anyhow::anyhow!("Client is not authenticated"))?
                .as_str(),
        )
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
            anyhow::bail!("{status}: {body}");
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct RefreshToken {
    pub id: models::Id,
    pub secret: String,
}

#[tracing::instrument(skip(client), err)]
pub async fn fetch_task_authorization(
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
pub async fn fetch_collection_authorization(
    client: &Client,
    collection: &str,
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
