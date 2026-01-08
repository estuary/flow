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

pub fn new_http_client(user_agent: String) -> reqwest::Client {
    reqwest::ClientBuilder::new()
        .user_agent(user_agent)
        .build()
        .expect("failed to build http client")
}
