use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Config {
    /// ID of the current draft, or None if no draft is configured.
    pub draft: Option<String>,
    // Current access token, or None if no token is set.
    pub api: Option<API>,
}

impl Config {
    pub async fn client(&mut self) -> anyhow::Result<postgrest::Postgrest> {
        match self.api.as_mut() {
            Some(api) => {
                if let Some(expires_at) = api.expires_at {
                    // 10 minutes before expiry attempt a refresh
                    if expires_at < (chrono::Utc::now() - chrono::Duration::minutes(10)).timestamp() {
                        tracing::debug!("refreshing token");
                        api.refresh().await?;
                    }
                }
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
    // Secret refresh token of the control-plane API.
    pub refresh_token: Option<String>,
    // Expiry of access_token
    pub expires_at: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct APISessionResponse {
    // Secret access token of the control-plane API.
    pub access_token: String,
    // Secret refresh token of the control-plane API.
    pub refresh_token: String,
    // Seconds to expiry of access_token
    pub expires_in: i64,
}

impl API {
    pub fn managed(service_account: crate::auth::ServiceAccount) -> Self {
        Self {
            endpoint: url::Url::parse("https://eyrcnmuzzyriypdajwdk.supabase.co/rest/v1").unwrap(),
            public_token: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5cmNubXV6enlyaXlwZGFqd2RrIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NDg3NTA1NzksImV4cCI6MTk2NDMyNjU3OX0.y1OyXD3-DYMz10eGxzo1eeamVMMUwIIeOoMryTRAoco".to_string(),
            access_token: service_account.access_token,
            refresh_token: Some(service_account.refresh_token),
            expires_at: Some(service_account.expires_at),
        }
    }
    pub fn development(access_token: Option<String>) -> Self {
        Self {
            endpoint: url::Url::parse("http://localhost:5431/rest/v1").unwrap(),
            public_token: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24ifQ.625_WdcF3KHqz5amU0x2X5WWHP-OEs_4qj0ssLNHzTs".to_string(),
            // Access token for user "bob" in the development database, good for ten years.
            access_token: access_token.unwrap_or(
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhdXRoZW50aWNhdGVkIiwiZXhwIjoyMjgwMDY3NTAwLCJzdWIiOiIyMjIyMjIyMi0yMjIyLTIyMjItMjIyMi0yMjIyMjIyMjIyMjIiLCJlbWFpbCI6ImJvYkBleGFtcGxlLmNvbSIsInJvbGUiOiJhdXRoZW50aWNhdGVkIn0.7BJJJI17d24Hb7ZImlGYDRBCMDHkqU1ppVTTfqD5l8I".to_string(),
            ),
            refresh_token: None,
            expires_at: None,
        }
    }

    // Attempt to refresh the token. This function is no-op if there is no refresh_token
    pub async fn refresh(&mut self) -> anyhow::Result<()> {
        let mut refresh_url = self.endpoint.clone();
        refresh_url.set_path("/auth/v1/token");
        refresh_url.set_query(Some("grant_type=refresh_token"));

        if let Some(refresh_token) = &self.refresh_token {
            let client = reqwest::Client::new();

            let body = client.post(refresh_url)
                .json(&HashMap::from([
                    ("refresh_token", refresh_token)
                ]))
                .header("apikey", &self.public_token)
                .send()
                .await?
                .text()
                .await?;

             match serde_json::from_str::<APISessionResponse>(&body) {
                Ok(sess) => {
                    self.access_token = sess.access_token;
                    self.refresh_token = Some(sess.refresh_token);
                    self.expires_at = Some(chrono::Utc::now().timestamp() + sess.expires_in);
                    Ok(())
                }
                Err(e) => {
                    tracing::error!("could not refresh token: {}, response {}", e, body);
                    Err(e)?
                }
             }

        } else {
            Err(anyhow::anyhow!("flowctl has not been configured with a refreshable token"))
        }
    }
}
