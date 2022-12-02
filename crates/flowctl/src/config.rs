use anyhow::Context;
use serde::{Deserialize, Serialize};

lazy_static::lazy_static! {
    static ref DEFAULT_DASHBOARD_URL: url::Url = url::Url::parse("https://dashboard.estuary.dev/").unwrap();
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Config {
    /// URL of the Flow UI, which will be used as a base when flowctl generates links to it.
    pub dashboard_url: Option<url::Url>,
    /// ID of the current draft, or None if no draft is configured.
    pub draft: Option<String>,
    // Current access token, or None if no token is set.
    pub api: Option<API>,
}

impl Config {
    pub fn cur_draft(&self) -> anyhow::Result<String> {
        match &self.draft {
            Some(draft) => Ok(draft.clone()),
            None => {
                anyhow::bail!("You must create or select a draft");
            }
        }
    }

    pub fn set_access_token(&mut self, access_token: String) {
        // Don't overwrite the other fields of api if they are already present.
        if let Some(api) = self.api.as_mut() {
            api.access_token = access_token;
        } else {
            self.api = Some(API::managed(access_token));
        }
    }

    pub fn get_dashboard_url(&self, path: &str) -> anyhow::Result<url::Url> {
        let base = self
            .dashboard_url
            .as_ref()
            .unwrap_or(&*DEFAULT_DASHBOARD_URL);
        let url = base.join(path).context(
            "failed to join path to configured dashboard_url, the dashboard_url is likely invalid",
        )?;
        Ok(url)
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
    fn managed(access_token: String) -> Self {
        Self {
            endpoint: url::Url::parse("https://eyrcnmuzzyriypdajwdk.supabase.co/rest/v1").unwrap(),
            public_token: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5cmNubXV6enlyaXlwZGFqd2RrIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NDg3NTA1NzksImV4cCI6MTk2NDMyNjU3OX0.y1OyXD3-DYMz10eGxzo1eeamVMMUwIIeOoMryTRAoco".to_string(),
            access_token,
        }
    }
}
