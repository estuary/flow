/// Client composes a base API URL and an HTTP client.
#[derive(Clone)]
pub struct Client {
    /// Base URL of the REST API.
    pub base_url: url::Url,
    /// HTTP client to use for REST requests.
    pub http_client: reqwest::Client,
}

impl Client {
    /// Create a new REST client with the given base URL and user agent.
    pub fn new(base_url: &url::Url, user_agent: &str) -> Self {
        Self {
            base_url: base_url.clone(),
            http_client: new_http_client(user_agent.to_string()),
        }
    }
}

impl Client {
    pub fn post<Request>(
        &self,
        path: &'static str,
        request: &Request,
        access_token: Option<&str>,
    ) -> reqwest::RequestBuilder
    where
        Request: serde::Serialize,
    {
        let mut builder = self
            .http_client
            .post(
                self.base_url
                    .join(path)
                    .expect("path must be valid to join"),
            )
            .json(request);

        if let Some(token) = access_token {
            builder = builder.bearer_auth(token);
        }
        builder
    }
}

/// Error from a GraphQL request.
#[derive(Debug, thiserror::Error)]
pub enum GraphqlError {
    #[error("GraphQL request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("GraphQL HTTP {status}: {body}")]
    Http { status: reqwest::StatusCode, body: String },
    #[error("GraphQL errors: {0:?}")]
    Api(Vec<serde_json::Value>),
    #[error("GraphQL response missing data field")]
    MissingData,
}

impl Client {
    /// Execute a GraphQL query/mutation and extract a single field from the response.
    ///
    /// `field` must match the top-level field name in the response `data` object.
    pub async fn graphql<T: serde::de::DeserializeOwned>(
        &self,
        query: &str,
        variables: Option<serde_json::Value>,
        access_token: Option<&str>,
    ) -> Result<T, GraphqlError> {
        let body = match variables {
            Some(vars) => serde_json::json!({ "query": query, "variables": vars }),
            None => serde_json::json!({ "query": query }),
        };

        let response = self
            .post("/api/graphql", &body, access_token)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(GraphqlError::Http { status, body });
        }

        #[derive(serde::Deserialize)]
        struct GqlResponse<D> {
            data: Option<D>,
            errors: Option<Vec<serde_json::Value>>,
        }

        let gql: GqlResponse<T> = response.json().await?;

        if let Some(errors) = gql.errors {
            return Err(GraphqlError::Api(errors));
        }

        gql.data.ok_or(GraphqlError::MissingData)
    }
}

pub fn new_http_client(user_agent: String) -> reqwest::Client {
    reqwest::ClientBuilder::new()
        .user_agent(user_agent)
        .build()
        .expect("failed to build http client")
}
