/// Client encapsulates sub-clients for various control-plane
/// and data-plane  services that `flowctl` interacts with.
#[derive(Clone)]
pub struct Client {
    // URL of the control-plane agent API.
    agent_endpoint: url::Url,
    // HTTP client to use for REST requests.
    http_client: reqwest::Client,
    // PostgREST client.
    pg_client: postgrest::Postgrest,
    // User's access token, if authenticated.
    user_access_token: Option<String>,
    // Base shard client which is cloned to build token-specific clients.
    shard_client: gazette::shard::Client,
    // Base journal client which is cloned to build token-specific clients.
    journal_client: gazette::journal::Client,
}

impl Client {
    /// Build a new Client from the Config.
    pub fn new(config: &crate::config::Config) -> Self {
        let user_access_token = config.user_access_token.clone();

        let mut pg_client = postgrest::Postgrest::new(config.get_pg_url().as_str())
            .insert_header("apikey", config.get_pg_public_token());

        if let Some(token) = user_access_token.as_ref() {
            pg_client = pg_client.insert_header("Authorization", &format!("Bearer {token}"));
        }

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
            agent_endpoint: config.get_agent_url().clone(),
            http_client: reqwest::Client::new(),
            journal_client,
            pg_client,
            shard_client,
            user_access_token,
        }
    }

    pub fn from(&self, table: &str) -> postgrest::Builder {
        self.pg_client.from(table)
    }

    pub fn rpc(&self, function: &str, params: String) -> postgrest::Builder {
        self.pg_client.rpc(function, params)
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
