use std::time::Duration;

use axum::extract::Extension;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;
use tokio::time::Instant;

use crate::context::AppContext;

#[serde_as]
#[derive(Debug, Serialize)]
pub struct HealthCheck {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    current_time: DateTime<Utc>,
    #[serde_as(as = "Option<serde_with::DurationSecondsWithFrac<String>>")]
    db_ping_seconds: Option<Duration>,
}

pub async fn show(Extension(ctx): Extension<AppContext>) -> Json<HealthCheck> {
    Json(HealthCheck {
        current_time: Utc::now(),
        db_ping_seconds: ping(ctx.db()).await,
    })
}

async fn ping(db: &PgPool) -> Option<Duration> {
    let start = Instant::now();

    let res = sqlx::query("SELECT 1").execute(db).await;

    match res {
        Ok(_) => Some(Instant::now() - start),
        Err(_) => None,
    }
}
