use anyhow::Context;

pub mod client;
pub use client::{
    fetch_task_authorization, fetch_user_collection_authorization, fetch_user_task_authorization,
    Client,
};

pub mod pagination;

// api_exec runs a PostgREST request, debug-logs its request, and turns non-success status into an anyhow::Error.
pub async fn api_exec<T>(b: postgrest::Builder) -> anyhow::Result<T>
where
    for<'de> T: serde::Deserialize<'de>,
{
    let req = b.build();
    tracing::debug!(?req, "built request to execute");

    let resp = req.send().await?;
    let status = resp.status();

    if status.is_success() {
        let body: models::RawValue = resp.json().await?;
        tracing::trace!(body = ?::ops::DebugJson(&body), status = %status, "got successful response");
        let t: T = serde_json::from_str(body.get()).context("deserializing response body")?;
        Ok(t)
    } else {
        let body = resp.text().await?;
        anyhow::bail!("{status}: {body}");
    }
}

/// Execute a [`postgrest::Builder`] request returning multiple rows. Unlike [`api_exec`]
/// which is limited to however many rows Postgrest is configured to return in a single response,
/// this will issue as many paginated requests as necessary to fetch every row.
pub async fn api_exec_paginated<T>(b: postgrest::Builder) -> anyhow::Result<Vec<T>>
where
    T: serde::de::DeserializeOwned + Send + Sync + 'static,
{
    use futures::TryStreamExt;

    let pages = pagination::into_items(b).try_collect().await?;

    Ok(pages)
}

pub fn parse_jwt_claims<T: serde::de::DeserializeOwned>(token: &str) -> anyhow::Result<T> {
    let claims = token
        .split('.')
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("malformed token"))?;
    let claims = base64::decode_config(claims, base64::URL_SAFE_NO_PAD)?;
    anyhow::Result::Ok(serde_json::from_slice(&claims)?)
}

lazy_static::lazy_static! {
    pub static ref DEFAULT_AGENT_URL:  url::Url = url::Url::parse("https://agent-api-1084703453822.us-central1.run.app").unwrap();
    pub static ref DEFAULT_DASHBOARD_URL: url::Url = url::Url::parse("https://dashboard.estuary.dev/").unwrap();
    pub static ref DEFAULT_PG_URL: url::Url = url::Url::parse("https://eyrcnmuzzyriypdajwdk.supabase.co/rest/v1").unwrap();

    // Used only when profile is "local".
    pub static ref LOCAL_AGENT_URL: url::Url = url::Url::parse("http://localhost:8675/").unwrap();
    pub static ref LOCAL_DASHBOARD_URL: url::Url = url::Url::parse("http://localhost:3000/").unwrap();
    pub static ref LOCAL_PG_URL: url::Url = url::Url::parse("http://localhost:5431/rest/v1").unwrap();
}

pub const DEFAULT_PG_PUBLIC_TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5cmNubXV6enlyaXlwZGFqd2RrIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NDg3NTA1NzksImV4cCI6MTk2NDMyNjU3OX0.y1OyXD3-DYMz10eGxzo1eeamVMMUwIIeOoMryTRAoco";
pub const LOCAL_PG_PUBLIC_TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0";
pub const LOCAL_DATA_PLANE_HMAC: &str = "c3VwZXJzZWNyZXQ=";
pub const LOCAL_DATA_PLANE_FQDN: &str = "local-cluster.dp.estuary-data.com";
pub const DEFAULT_DATA_PLANE_FQDN: &str = "gcp-us-central1-c1.dp.estuary-data.com";
