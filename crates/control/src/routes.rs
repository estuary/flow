use axum::Json;
use chrono::Utc;
use serde::{Deserialize, Serialize};

pub async fn health_check() -> String {
    format!("{}", Utc::now())
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
