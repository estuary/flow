mod connector_tags;
mod directives;
mod discovers;
mod jobs;
pub mod logs;
mod publications;

use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgListener;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{
        mpsc::{self, UnboundedSender},
        Mutex,
    },
    time::error::Elapsed,
};

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

#[derive(Deserialize, Serialize, Debug)]
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

#[derive(Debug)]
struct HandlerInvocation {
    table_name: String,
    is_poll: bool,
}

async fn listen_for_tasks(
    task_tx: UnboundedSender<HandlerInvocation>,
    table_names: Vec<String>,
    pg_pool: &sqlx::PgPool,
) -> anyhow::Result<()> {
    let listener = Arc::new(Mutex::new(PgListener::connect_with(&pg_pool).await?));

    listener
        .lock()
        .await
        .listen(AGENT_NOTIFICATION_CHANNEL)
        .await?;

    // Sqlx does not give us the option to set TCP keepalive on its connections,
    // and this specifc scenario of keeping a long-running connection open to listen
    // for notifications is especially prone to deadlocking
    // due to mismatched connection timeouts on the client vs server.
    //
    // In order to ensure we actually find out about socket disconnects
    // we do this hack that ensures that some traffic goes over the
    // socket at least every 30s.
    //
    // In the case that the remote side thinks the connection is closed,
    // we'll get a TCP reset, which will cause us to close the connection.
    // This will then bubble through as a None return to PgListener:try_recv(),
    // which we'll then attempt to reconnect, and trigger the task handlers
    // to look and see if there's any work waiting for them.
    let cloned_listener = listener.clone();
    let _handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            tracing::debug!("Poking listener to keep connection alive/figure out if it timed out");
            cloned_listener
                .lock()
                .await
                .listen("hacky_keepalive")
                .await
                .unwrap();

            cloned_listener
                .lock()
                .await
                .unlisten("hacky_keepalive")
                .await
                .unwrap();
        }
    });

    loop {
        // We need to release the lock on listener every once in a while so that the
        // keepalive loop above can do its thing, otherwise we'll hold the lock open
        // waiting on try_recv, which may never complete because the connection is
        // unknowningly disconnected, which is the whole point of the keepalive loop.
        match tokio::time::timeout(Duration::from_secs(30), async {
            // try_recv returns None when the channel disconnects,
            // which we want to have explicit handling for
            listener
                .lock()
                .await
                .try_recv()
                .await
                .map_err(|e| anyhow::Error::from(e))
        })
        .await
        {
            Ok(maybe_item) => {
                if let Some(item) = maybe_item? {
                    let notification: AgentNotification = serde_json::from_str(item.payload())
                        .context("deserializing agent task notification")?;

                    tracing::debug!(
                        table = &notification.table,
                        "Message received to invoke handler"
                    );
                    task_tx
                        .send(HandlerInvocation {
                            table_name: notification.table,
                            is_poll: false,
                        })
                        .map_err(|e| anyhow::Error::from(e))?
                } else {
                    tracing::warn!("LISTEN/NOTIFY stream from postgres lost, waking all handlers and attempting to reconnect");
                    table_names.iter().for_each(|table| {
                        task_tx
                            .send(HandlerInvocation {
                                table_name: table.clone(),
                                is_poll: false,
                            })
                            .unwrap()
                    });
                }
            }
            // Timeout reached
            Err(_) => {
                continue;
            }
        }
    }
}

async fn handle_task(
    handler: &mut Box<dyn Handler>,
    handlers_active: Arc<HashMap<&str, Arc<AtomicBool>>>,
    is_poll: bool,
    task_tx: UnboundedSender<HandlerInvocation>,
    pg_pool: &sqlx::PgPool,
) -> anyhow::Result<()> {
    // --- Remove me when we're confident that listen/notify won't miss anything ---
    let active = handlers_active.get(&handler.table_name() as &str).unwrap();

    active.store(true, Ordering::SeqCst);
    // -----------------------------------------------------------------------------

    let handle_result = handler.handle(&pg_pool).await;

    match handle_result {
        Ok(status) => {
            tracing::info!(handler = %handler.name(), table = %handler.table_name(), status = ?status, "invoked handler");
            match status {
                // Active indicates that there may be more work to perform,
                // so we should schedule another run of this handler
                HandlerStatus::Active => {
                    if is_poll {
                        tracing::warn!(
                            handler = %handler.name(),
                            table = %handler.table_name(),
                            "Polled handler actually had work to perform. This means LISTEN/NOTIFY missed something!"
                        );
                    }
                    task_tx.send(HandlerInvocation {
                        table_name: handler.table_name().to_string(),
                        is_poll,
                    })?;
                }
                // Idle indicates that the handler checked and didn't find any work to do,
                // so let's wait until we get a message from the database before we wake again
                HandlerStatus::Idle => {
                    active.store(false, Ordering::SeqCst);
                }
            }
        }
        Err(err) => {
            tracing::error!(handler = %handler.name(), table = %handler.table_name(), "Error invoking handler: {err}");
            active.store(false, Ordering::SeqCst);
            return Err(err);
        }
    };
    Ok(())
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

    // --- Remove me when we're confident that listen/notify won't miss anything ---
    // We need to keep track of which handlers are active so that
    // the polling logic doesn't schedule a poll-tainted handle invocation
    // while "legit" handle invocations are happening. This would look like
    // scary logging about missed messages while in reality that's not the case.
    let handlers_active = Arc::new(
        handlers_by_table
            .iter()
            .map(|(table_name, _)| (*table_name, Arc::new(AtomicBool::new(false))))
            .collect::<HashMap<_, _>>(),
    );
    // -----------------------------------------------------------------------------

    // We use a channel here for two reasons:
    // 1. Because handlers run one task at a time, and can also indicate that they have more work to perform or not,
    //    we want to balance the time spent processing each type of handler so that no one handler can monopolize resources.
    // 2. It makes it easy to preemptively schedule at least one run of each handler on boot up to allow for handling requests
    //    that came in while we weren't running
    // NOTE: it is critical that we use an unbounded channel here, otherwise we would open ourselves up to a deadlock scenario
    let (task_tx, mut task_rx) = mpsc::unbounded_channel::<HandlerInvocation>();

    // Each handler gets run at least once to check if there is any pending work
    let handler_table_names: Vec<String> = handlers_by_table
        .iter()
        .map(|(handler_table, _)| handler_table.to_string())
        .collect();
    handler_table_names.iter().for_each(|table| {
        task_tx
            .send(HandlerInvocation {
                table_name: table.clone(),
                is_poll: false,
            })
            .unwrap()
    });

    let listen_to_datbase_notifications =
        listen_for_tasks(task_tx.clone(), handler_table_names.clone(), &pg_pool);

    // --- Remove me when we're confident that listen/notify won't miss anything ---
    let task_tx_cloned = task_tx.clone();
    let handlers_active_cloned = handlers_active.clone();
    let temporary_polling = async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;

            for table_name in &handler_table_names {
                let active = handlers_active_cloned
                    .get(&table_name as &str)
                    .unwrap()
                    .load(Ordering::SeqCst);

                if active {
                    tracing::debug!(
                        table_name = table_name,
                        "Not polling handler as it's currently active"
                    );
                } else {
                    tracing::debug!(
                        table_name = table_name,
                        "Polling handler as it's currently idle"
                    );
                    task_tx_cloned
                        .send(HandlerInvocation {
                            table_name: table_name.clone(),
                            is_poll: true,
                        })
                        .unwrap();
                }
            }
        }
    };
    // -----------------------------------------------------------------------------

    tokio::pin!(exit);
    tokio::pin!(temporary_polling);
    tokio::pin!(listen_to_datbase_notifications);

    loop {
        // We use tokio::select! here to enforce the desired error handling behavior of
        // only exiting after we have processed whatever task we were working on
        let HandlerInvocation {
            table_name: handler_table_name,
            is_poll,
        } = tokio::select! {
            _ = &mut exit => {
                tracing::debug!("caught signal; exiting...");
                return Ok(()) // All done.
            }
            _ = &mut temporary_polling => {
                unreachable!("Polling exited unexpectedly");
            }
            listener_res = &mut listen_to_datbase_notifications => {
                match listener_res {
                    // It should be impossible to get here since `listen_to_datbase_notifications` loop has no `return`s or `break`s
                    Ok(_) => unreachable!("Unexpected notification listener exit"),
                    // If we get an error from inside `listen_to_datbase_notifications`,
                    // something went wrong when actually listening to the postgres channel
                    Err(e) => return Err(e.into()),
                }
            }
            maybe_handler_table = task_rx.recv() => {
                if let Some(handler_table) = maybe_handler_table {
                    handler_table
                } else {
                    // If `task_rx.recv()` returns None, the channel has been closed
                    // This shouldn't happen (since task_tx/task_rx live until the end of `serve`),
                    // so if it does then something probably went wrong and we should exit
                    unreachable!("Agent task channel unexpectedly closed")
                }
            }
        };

        let mut handler = match handlers_by_table.get(&handler_table_name as &str) {
            Some(handler) => handler.lock().await,
            None => {
                tracing::warn!(
                    table = &handler_table_name,
                    "Message received to handle unknown table"
                );
                continue;
            }
        };

        handle_task(
            &mut handler,
            handlers_active.clone(),
            is_poll,
            task_tx.clone(),
            &pg_pool,
        )
        .await?;
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

#[cfg(test)]
mod test {

    use anyhow::Context;
    use futures::{FutureExt, TryFutureExt};
    use serial_test::serial;
    use sqlx::{postgres::PgListener, PgPool};

    use crate::{serve, AgentNotification, Handler, HandlerStatus, AGENT_NOTIFICATION_CHANNEL};

    const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

    // Delete in reverse order to avoid integrity-check issues
    const HAPPY_PATH_CLEANUP: &str = r#"
      with p7 as (
        delete from user_grants where user_id = '43a18a3e-5a59-11ed-9b6a-0242ac120002'
      ),
      p6 as (
        delete from role_grants where subject_role = 'usageB/'
      ),
      p5 as (
        delete from publications where id = '1111100000000000'
      ),
      p4 as (
        delete from draft_specs where id = '1111000000000000'
      ),
      p3 as (
          delete from live_specs where id = '1000000000000000'
      ),
      p2 as (
        delete from drafts where id = '1110000000000000'
      ),
      p1 as (
        delete from auth.users where id = '43a18a3e-5a59-11ed-9b6a-0242ac120002'
      )
      select 1;
    "#;

    #[derive(Debug)]
    struct MockHandler {
        notifier: tokio::sync::mpsc::UnboundedSender<()>,
        table_name: &'static str,
    }

    impl MockHandler {
        fn new(table_name: &'static str, notifier: tokio::sync::mpsc::UnboundedSender<()>) -> Self {
            MockHandler {
                notifier,
                table_name,
            }
        }
    }

    #[async_trait::async_trait]
    impl Handler for MockHandler {
        async fn handle(&mut self, _: &sqlx::PgPool) -> anyhow::Result<HandlerStatus> {
            self.notifier.send(()).unwrap();
            Ok(HandlerStatus::Idle)
        }

        fn table_name(&self) -> &'static str {
            &self.table_name
        }
    }

    // We indicate that test_handlers_react_quickly and test_pg_notifications are to be run serially
    // because in the middle of their execution, they commit a transaction that modifies the database
    // even though they clean up those changes before exiting. The problem arrises when another test
    // tries to make conflicting changes (even inside a transaction). Therefore, we also have to mark
    // all other tests as #[parallel], so that they are not run at the same time as a #[serial] test
    #[tokio::test]
    #[serial]
    async fn test_handlers_react_quickly() {
        let pg_pool = PgPool::connect(&FIXED_DATABASE_URL).await.unwrap();

        let (handler_notify_tx, mut handler_notify_rx) =
            tokio::sync::mpsc::unbounded_channel::<()>();
        // Must allow `exit_tx` to exist here,
        // otherwise it'll get instantly dropped and kill the server prematurely
        #[allow(unused_variables)]
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<()>();

        let server = serve(
            vec![Box::new(MockHandler::new(
                "publications",
                handler_notify_tx,
            ))],
            pg_pool.clone(),
            exit_rx.map(|_| ()),
        );

        tokio::pin!(server);

        tokio::select! {
            res = &mut server => {
                Err(anyhow::anyhow!("Handler unexpectedly exited: {:#?}", res))
            }
            res = async move {
                // Do this 10 times in a row to make sure that our handler gets called consistently quickly
                for _ in 0..10{
                    let mut txn = pg_pool.begin().await.unwrap();
                    // Sets up the database to have a valid publication task and associated draft/specs
                    sqlx::query(include_str!("publications/test_resources/happy_path.sql"))
                        .execute(&mut txn)
                        .await
                        .unwrap();

                    // We have to commit the transaction for the NOTIFY to get sent
                    txn.commit().await.unwrap();

                    // Make sure that our mock publication handler was called
                    handler_notify_rx.recv()
                        .await
                        .context("receiving from mock task notification channel")
                        .unwrap();

                    // We have to clean up because we commit the transaction above
                    // We can't use `txn` since `.commit()` consumes itself, so we have to
                    // acquire another connection for a sec to do this cleanup
                    let mut conn = pg_pool.acquire().await.unwrap();
                    sqlx::query(HAPPY_PATH_CLEANUP).execute(&mut conn).await.unwrap();
                }

                Ok(())
            } => {
                res
            }
        }
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_pg_notifications() {
        let pg_pool = PgPool::connect(&FIXED_DATABASE_URL).await.unwrap();
        let mut txn = pg_pool.begin().await.unwrap();

        let mut listener = PgListener::connect_with(&pg_pool).await.unwrap();

        listener.listen(AGENT_NOTIFICATION_CHANNEL).await.unwrap();

        // This sets up the database to have a valid publication
        // which should trigger a NOTIFY on the AGENT_NOTIFICATION_CHANNEL
        sqlx::query(include_str!("publications/test_resources/happy_path.sql"))
            .execute(&mut txn)
            .await
            .unwrap();

        // We have to commit the transaction for the NOTIFY to get sent
        txn.commit().await.unwrap();

        let notification: AgentNotification = listener
            .recv()
            .map_ok(|item| serde_json::from_str(item.payload()).unwrap())
            .await
            .unwrap();

        // We can't use `txn` since `.commit()` consumes itself, so we have to
        // acquire another connection for a sec to do this cleanup
        let mut conn = pg_pool.acquire().await.unwrap();
        sqlx::query(HAPPY_PATH_CLEANUP)
            .execute(&mut conn)
            .await
            .unwrap();

        insta::assert_json_snapshot!(
            notification,
            {".timestamp" => "[timestamp]"},
            @r#"
            {
              "timestamp": "[timestamp]",
              "table": "publications"
            }"#
        );
    }
}
