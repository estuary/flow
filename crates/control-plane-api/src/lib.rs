pub mod connector_tags;
pub mod controllers;
pub mod data_plane;
pub mod directives;
pub mod discovers;
pub mod draft;
pub mod evolutions;
pub mod jobs;
pub mod live_specs;
pub mod logs;
pub mod proxy_connectors;
pub mod publications;
pub mod server;

// Re-export from the old agent-sql crate
mod text_json;
pub use text_json::TextJson;

pub use models::{Capability, CatalogType, Id};
pub use tables::RoleGrant;

// Re-export the router builder function for the agent to use
pub use server::build_router;

// Re-export the GraphQL schema SDL function for flow-client build script
pub use server::public::graphql::schema_sdl as graphql_schema_sdl;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use sqlx::types::Uuid;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "flow_type")]
#[sqlx(rename_all = "snake_case")]
pub enum FlowType {
    Capture,
    Collection,
    Materialization,
    Test,
    SourceCapture,
}

impl From<CatalogType> for FlowType {
    fn from(c: CatalogType) -> Self {
        match c {
            CatalogType::Capture => FlowType::Capture,
            CatalogType::Collection => FlowType::Collection,
            CatalogType::Materialization => FlowType::Materialization,
            CatalogType::Test => FlowType::Test,
        }
    }
}

/// Returns the user ID for the given email address, or an error if the email address is not found.
pub async fn get_user_id_for_email(email: &str, db: &sqlx::PgPool) -> sqlx::Result<Uuid> {
    sqlx::query_scalar!(
        r#"
        SELECT id
        FROM auth.users
        WHERE email = $1
        "#,
        email
    )
    .fetch_one(db)
    .await
}

/// Wraps a `chrono::Duration` to allow it to be used as a Postgres `interval`
/// type. This is necessary because `chrono::Duration` does not implement
/// `Decode`. Note that converting a `chrono::Duration` to an `interval` may
/// fail if the duration cannot be faithfully represented as an interval. This
/// would be the case if it uses nanosecond precision, for example. Thus if we
/// ever need to support inserting an `Interval`, we should add explicit
/// conversion functions from `chrono::Duration`.
pub struct Interval(chrono::Duration);

impl sqlx::Type<sqlx::postgres::Postgres> for Interval {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <chrono::Duration as sqlx::Type<sqlx::postgres::Postgres>>::type_info()
    }
    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <chrono::Duration as sqlx::Type<sqlx::postgres::Postgres>>::compatible(ty)
    }
}

impl<'q> sqlx::Encode<'q, sqlx::postgres::Postgres> for Interval {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.0.encode_by_ref(buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::postgres::Postgres> for Interval {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let pg_int = <sqlx::postgres::types::PgInterval as sqlx::Decode<
            'r,
            sqlx::postgres::Postgres,
        >>::decode(value)?;

        let d = chrono::Duration::microseconds(pg_int.microseconds);
        Ok(Interval(d))
    }
}

impl std::ops::Deref for Interval {
    type Target = chrono::Duration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Into<chrono::Duration> for Interval {
    fn into(self) -> chrono::Duration {
        self.0
    }
}

// timeout is a convenience for tokio::time::timeout which merges
// its error with the Future's nested anyhow::Result Output.
pub async fn timeout<Ok, Fut, C, WC>(
    dur: std::time::Duration,
    fut: Fut,
    with_context: WC,
) -> anyhow::Result<Ok>
where
    C: std::fmt::Display + Send + Sync + 'static,
    Fut: std::future::Future<Output = anyhow::Result<Ok>>,
    WC: FnOnce() -> C,
{
    use anyhow::Context;

    match tokio::time::timeout(dur, fut).await {
        Ok(result) => result,
        Err(err) => Err(anyhow::anyhow!(err)).with_context(with_context),
    }
}

pub async fn decrypt_hmac_keys(dp: &mut tables::DataPlane) -> anyhow::Result<()> {
    let sops = locate_bin::locate("sops").context("failed to locate sops")?;

    if !dp.hmac_keys.is_empty() {
        return Ok(());
    }

    #[derive(serde::Deserialize)]
    struct HMACKeys {
        hmac_keys: Vec<String>,
    }

    // Note that input_output() pre-allocates an output buffer as large as its input buffer,
    // and our decrypted result will never be larger than its input.
    let async_process::Output {
        stderr,
        stdout,
        status,
    } = async_process::input_output(
        async_process::Command::new(sops).args([
            "--decrypt",
            "--input-type",
            "json",
            "--output-type",
            "json",
            "/dev/stdin",
        ]),
        dp.encrypted_hmac_keys.get().as_bytes(),
    )
    .await
    .context("failed to run sops")?;

    let stdout = zeroize::Zeroizing::from(stdout);

    if !status.success() {
        anyhow::bail!(
            "decrypting hmac sops document failed: {}",
            String::from_utf8_lossy(&stderr),
        );
    }

    dp.hmac_keys = serde_json::from_slice::<HMACKeys>(&stdout)
        .context("parsing decrypted sops document")?
        .hmac_keys;

    Ok(())
}
