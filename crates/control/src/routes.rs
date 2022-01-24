use std::time::Duration;

use axum::extract::Extension;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::time::Instant;

#[serde_as]
#[derive(Debug, Serialize)]
pub struct HealthCheck {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    current_time: DateTime<Utc>,
    #[serde_as(as = "Option<serde_with::DurationSecondsWithFrac<String>>")]
    db_ping_seconds: Option<Duration>,
}

pub async fn health_check(Extension(db): Extension<PgPool>) -> Json<HealthCheck> {
    Json(HealthCheck {
        current_time: Utc::now(),
        db_ping_seconds: ping(&db).await,
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorType {
    Source,
    Materialization,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Connector {
    description: String,
    image: String,
    name: String,
    owner: String,
    r#type: ConnectorType,
    tags: Vec<String>,
}

pub async fn list_connectors() -> Json<Vec<Connector>> {
    let connectors = vec![Connector {
        description: "A flood of greetings.".to_owned(),
        image: "ghcr.io/estuary/source-hello-world".to_owned(),
        name: "source-hello-world".to_owned(),
        owner: "Estuary".to_owned(),
        r#type: ConnectorType::Source,
        tags: vec!["dev".to_owned()],
    }];

    Json(connectors)
}
