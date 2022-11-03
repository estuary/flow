mod connector_tags;
mod directives;
mod discovers;
mod jobs;
pub mod logs;
mod publications;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::postgres::PgListener;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, Mutex};

pub use agent_sql::{CatalogType, Id};
pub use connector_tags::TagHandler;
pub use directives::DirectiveHandler;
pub use discovers::DiscoverHandler;
pub use publications::PublishHandler;

#[derive(Debug)]
pub enum HandlerStatus {
    Active,
    Idle,
}

#[derive(Deserialize, Debug)]
pub struct AgentNotification {
    timestamp: DateTime<Utc>,
    table: String,
}

/// Handler is the principal trait implemented by the various task-specific
/// event handlers that the agent runs.
#[async_trait::async_trait]
pub trait Handler {
    async fn handle(&mut self, pg_pool: &sqlx::PgPool) -> anyhow::Result<HandlerStatus>;

    fn table_name(&self) -> &'static str;

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

const AGENT_NOTIFICATION_CHANNEL: &str = "agent_notifications";

// serve one or more Handlers until signaled by a ready |exit| future.
#[tracing::instrument(ret, skip_all)]
pub async fn serve<E>(
    handlers: Vec<Box<dyn Handler>>,
    pg_pool: sqlx::PgPool,
    exit: E,
) -> anyhow::Result<()>
where
    E: std::future::Future<Output = ()> + Send,
{
    let handlers_by_table = handlers
        .into_iter()
        .map(|h| (h.table_name(), Arc::new(Mutex::new(h))))
        .collect::<HashMap<_, _>>();

    let mut listener = PgListener::connect_with(&pg_pool).await?;

    listener.listen(AGENT_NOTIFICATION_CHANNEL).await?;

    let (task_tx, mut task_rx) = mpsc::channel::<String>(1000);

    // Each task gets run at least once to check if there is any pending work
    for (handler_table, _) in handlers_by_table.iter() {
        task_tx.send(handler_table.to_string()).await?;
    }

    tokio::pin!(exit);

    let listen_to_queue = async {
        loop {
            let item = listener.recv().await?;
            let notification: AgentNotification = serde_json::from_str(item.payload())?;
            let table: &str = &notification.table;
            match handlers_by_table.get(table) {
                Some(_) => {
                    tracing::debug!(table = table, "Message received to invoke handler");
                    task_tx.send(table.to_string()).await?
                }
                None => tracing::warn!(table = table, "Message received to handle unknown table"),
            }
        }
    };

    let handle_from_queue = async {
        while let Some(chan) = task_rx.recv().await {
            let mut handler = handlers_by_table
                .get(chan.as_str())
                .expect(format!("Unexpected task channel {}", chan).as_str())
                .lock()
                .await;

            let handle_result = handler.handle(&pg_pool).await;

            match handle_result {
                Ok(status) => {
                    tracing::info!(handler = %handler.name(), table = %handler.table_name(), status = ?status, "invoked handler");
                    match status {
                        HandlerStatus::Active => {
                            // Re-schedule another run to handle this MoreWork
                            task_tx.send(handler.table_name().to_string()).await?;
                        }
                        _ => {}
                    }
                }
                Err(err) => {
                    // Do we actually just want to crash here?
                    tracing::error!(handler = %handler.name(), table = %handler.table_name(), "Error invoking handler: {}", err.to_string());
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    };

    tokio::select! {
        listen_res = listen_to_queue => {
            return listen_res;
        }
        handle_res = handle_from_queue => {
            return handle_res;
        }
        _ = exit => {
            return Ok(())
        }
    }
}

// upsert_draft_specs updates the given draft with specifications of the catalog.
async fn upsert_draft_specs(
    draft_id: Id,
    models::Catalog {
        collections,
        captures,
        materializations,
        tests,
        ..
    }: models::Catalog,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), sqlx::Error> {
    for (collection, spec) in collections {
        agent_sql::upsert_draft_spec(
            draft_id,
            collection.as_str(),
            spec,
            CatalogType::Collection,
            txn,
        )
        .await?;
    }
    for (capture, spec) in captures {
        agent_sql::upsert_draft_spec(draft_id, capture.as_str(), spec, CatalogType::Capture, txn)
            .await?;
    }
    for (materialization, spec) in materializations {
        agent_sql::upsert_draft_spec(
            draft_id,
            materialization.as_str(),
            spec,
            CatalogType::Materialization,
            txn,
        )
        .await?;
    }
    for (test, steps) in tests {
        agent_sql::upsert_draft_spec(draft_id, test.as_str(), steps, CatalogType::Test, txn)
            .await?;
    }

    agent_sql::touch_draft(draft_id, txn).await?;
    Ok(())
}
