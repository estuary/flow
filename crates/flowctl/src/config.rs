use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Config {
    /// ID of the current draft, or None if no draft is configured.
    pub draft: Option<String>,
    // Current access token, or None if no token is set.
    pub api: Option<API>,
}

impl Config {
    pub fn client(&self) -> anyhow::Result<postgrest::Postgrest> {
        match &self.api {
            Some(api) => {
                let client = postgrest::Postgrest::new(api.endpoint.as_str());
                let client = client.insert_header("apikey", &api.public_token);
                let client =
                    client.insert_header("Authorization", format!("Bearer {}", &api.access_token));
                Ok(client)
            }
            None => {
                anyhow::bail!("You must run `auth login` first")
            }
        }
    }

    pub fn cur_draft(&self) -> anyhow::Result<String> {
        match &self.draft {
            Some(draft) => Ok(draft.clone()),
            None => {
                anyhow::bail!("You must create or select a draft");
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct API {
    // URL endpoint of the Flow control-plane Rest API.
    pub endpoint: url::Url,
    // Public (shared) anonymous token of the control-plane API.
    pub public_token: String,
    // Secret access token of the control-plane API.
    pub access_token: String,
}

impl API {
    pub fn managed(access_token: String) -> Self {
        Self {
            endpoint: url::Url::parse("https://eyrcnmuzzyriypdajwdk.supabase.co/rest/v1").unwrap(),
            public_token: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5cmNubXV6enlyaXlwZGFqd2RrIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NDg3NTA1NzksImV4cCI6MTk2NDMyNjU3OX0.y1OyXD3-DYMz10eGxzo1eeamVMMUwIIeOoMryTRAoco".to_string(),
            access_token,
        }
    }
    pub fn development(access_token: Option<String>) -> Self {
        Self {
            endpoint: url::Url::parse("http://localhost:5431/rest/v1").unwrap(),
            public_token: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24ifQ.625_WdcF3KHqz5amU0x2X5WWHP-OEs_4qj0ssLNHzTs".to_string(),
            // Access token for user "bob" in the development database, good for ten years.
            access_token: access_token.unwrap_or(
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhdXRoZW50aWNhdGVkIiwiZXhwIjoyMjgwMDY3NTAwLCJzdWIiOiIyMjIyMjIyMi0yMjIyLTIyMjItMjIyMi0yMjIyMjIyMjIyMjIiLCJlbWFpbCI6ImJvYkBleGFtcGxlLmNvbSIsInJvbGUiOiJhdXRoZW50aWNhdGVkIn0.7BJJJI17d24Hb7ZImlGYDRBCMDHkqU1ppVTTfqD5l8I".to_string(),
            )
        }
    }
}
