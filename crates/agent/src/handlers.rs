use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgListener;
use std::{collections::HashMap, time::Duration};
use tokio::sync::mpsc::{self, UnboundedSender};

#[derive(Debug, PartialEq)]
pub enum HandleResult {
    HadJob,
    NoJobs,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct AgentNotification {
    timestamp: DateTime<Utc>,
    table: String,
}

/// Handler is the principal trait implemented by the various task-specific
/// event handlers that the agent runs. They need to be `Send` because we
/// spawn the handler invocations on a multithreaded runtime.
#[async_trait::async_trait]
pub trait Handler: Send {
    /// Attempt to handle the next avaialable job, returning a result indicating whether an eligible
    /// job was found. The `allow_background` parameter indicates whether background jobs should be
    /// considered eligible. If `true`, then interactive jobs should still be considered eligible.
    /// If `false`, then any background jobs must be considered ineligible.
    async fn handle(
        &mut self,
        pg_pool: &sqlx::PgPool,
        allow_background: bool,
    ) -> anyhow::Result<HandleResult>;

    fn table_name(&self) -> &'static str;

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

async fn listen_for_tasks(
    task_tx: UnboundedSender<String>,
    table_names: Vec<String>,
    pg_pool: sqlx::PgPool,
) -> anyhow::Result<()> {
    tracing::debug!(?table_names, "listening for notifications on tables");
    let mut listener = PgListener::connect_with(&pg_pool).await?;

    listener.listen(AGENT_NOTIFICATION_CHANNEL).await?;

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
    let mut should_poke_connection = false;
    loop {
        if should_poke_connection {
            tracing::debug!("Poking listener to keep connection alive/figure out if it timed out");
            should_poke_connection = false;

            listener.listen("hacky_keepalive").await?;
            listener.unlisten("hacky_keepalive").await?;
        }

        let recv_timeout = tokio::time::sleep(Duration::from_secs(30));

        let maybe_notification = tokio::select! {
           _ = recv_timeout => {
               should_poke_connection = true;
               continue;
           },
           notify = listener.try_recv() => notify
        }
        .context("listening for notifications from database")?;

        // try_recv returns None when the channel disconnects,
        // which we want to have explicit handling for
        if let Some(notification) = maybe_notification {
            let notification: AgentNotification = serde_json::from_str(notification.payload())
                .context("deserializing agent task notification")?;

            tracing::debug!(
                table = &notification.table,
                "Message received to invoke handler"
            );
            task_tx.send(notification.table)?
        } else {
            tracing::warn!("LISTEN/NOTIFY stream from postgres lost, waking all handlers and attempting to reconnect");
            table_names
                .iter()
                .for_each(|table| task_tx.send(table.clone()).unwrap());
        }
    }
}

const AGENT_NOTIFICATION_CHANNEL: &str = "agent_notifications";

#[derive(Debug, PartialEq, Copy, Clone)]
enum Status {
    PollInteractive,
    PollBackground,
    Idle,
}

struct WrappedHandler {
    status: Status,
    handler: Box<dyn Handler>,
}

impl WrappedHandler {
    async fn handle_next_job(&mut self, pg_pool: &sqlx::PgPool) -> anyhow::Result<()> {
        let allow_background = self.status != Status::PollInteractive;
        match self.handler.handle(pg_pool, allow_background).await {
            Ok(HandleResult::HadJob) => Ok(()),
            Ok(HandleResult::NoJobs) if self.status == Status::PollInteractive => {
                tracing::debug!(handler = %self.handler.name(), "handler completed all interactive jobs");
                self.status = Status::PollBackground;
                Ok(())
            }
            Ok(HandleResult::NoJobs) => {
                tracing::debug!(handler = %self.handler.name(), "handler completed all background jobs");
                self.status = Status::Idle;
                Ok(())
            }
            Err(err) => {
                tracing::error!(handler = %self.handler.name(), error = ?err, "Error invoking handler");
                Err(err)
            }
        }
    }
}

#[tracing::instrument(ret, skip_all)]
pub async fn serve<E>(
    handlers: Vec<Box<dyn Handler>>,
    pg_pool: sqlx::PgPool,
    exit: E,
) -> anyhow::Result<()>
where
    E: std::future::Future<Output = ()> + Send,
{
    use futures::FutureExt;

    // We use a channel here because we're spawning another task to listen for notifications.
    // We could probably use a bounded channel, but it doesn't seem important that we do so,
    // and I'm uncertain as to how a bounded channel might affect the reliability of the listener.
    let (task_tx, mut task_rx) = mpsc::unbounded_channel::<String>();

    let handler_table_names = handlers
        .iter()
        .map(|h| h.table_name().to_string())
        .collect::<Vec<String>>();

    let mut handlers_by_table = handlers
        .into_iter()
        .map(|h| {
            (
                h.table_name().to_string(),
                WrappedHandler {
                    // We'll start by assuming every handler might have interactive jobs to handle
                    status: Status::PollInteractive,
                    handler: h,
                },
            )
        })
        .collect::<HashMap<_, _>>();

    let mut listen_to_datbase_notifications = tokio::spawn(listen_for_tasks(
        task_tx.clone(),
        handler_table_names.clone(),
        pg_pool.clone(),
    ));

    tokio::pin!(exit);

    loop {
        // Check our exit conditions
        if (&mut exit).now_or_never().is_some() {
            tracing::info!("caught signal; exiting...");
            return Ok(()); // All done.
        }
        if let Some(listener_res) = (&mut listen_to_datbase_notifications).now_or_never() {
            match listener_res {
                // It should be impossible to get here since `listen_to_datbase_notifications` loop has no `return`s or `break`s
                Ok(_) => unreachable!("Unexpected notification listener exit"),
                // If we get an error from inside `listen_to_datbase_notifications`,
                // something went wrong when actually listening to the postgres channel
                Err(e) => return Err(e.into()),
            }
        }

        // Receive all the notifications that are avaialable right now, without blocking
        while let Ok(table_name) = task_rx.try_recv() {
            let Some(handler) = handlers_by_table.get_mut(&table_name) else {
                tracing::warn!(%table_name, "got notification for unknown job table");
                continue;
            };
            tracing::debug!(%table_name, handler = %handler.handler.name(), "got notification for handler table");
            handler.status = Status::PollInteractive;
        }

        // Invoke each of the handler types that _might_ have interactive jobs to perform
        for handler in handlers_by_table
            .values_mut()
            .filter(|h| h.status == Status::PollInteractive)
        {
            handler.handle_next_job(&pg_pool).await?;
        }

        // We only process background jobs if there are no interactive jobs of any type
        if handlers_by_table
            .values()
            .any(|h| h.status == Status::PollInteractive)
        {
            continue;
        }

        for handler in handlers_by_table
            .values_mut()
            .filter(|h| h.status == Status::PollBackground)
        {
            handler.handle_next_job(&pg_pool).await?;
        }

        if handlers_by_table.values().all(|h| h.status == Status::Idle) {
            tracing::debug!("all handlers idle, awaiting notification");
            tokio::select! {
                _ = &mut exit => {
                    tracing::info!("caught signal; exiting...");
                    return Ok(()); // All done.
                }
                recvd = task_rx.recv() => {
                    let Some(table_name) = recvd else {
                        panic!("notification channel closed unexpectedly");
                    };
                    let Some(handler) = handlers_by_table.get_mut(&table_name) else {
                        tracing::warn!(%table_name, "got notification for unknown job table");
                        continue;
                    };
                    tracing::debug!(%table_name, "got notification for handler table");
                    handler.status = Status::PollInteractive;
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    tracing::debug!("polling all handlers, just in case we missed a notification");
                    for handler in handlers_by_table.values_mut() {
                        // No need to go through both interactive and background, since this is "extra"
                        // and handlers should still dequeue interactive jobs even when `allow_background = true`.
                        handler.status = Status::PollBackground;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {

    use super::{serve, AgentNotification, HandleResult, Handler, AGENT_NOTIFICATION_CHANNEL};
    use anyhow::Context;
    use futures::{FutureExt, TryFutureExt};
    use serial_test::serial;
    use sqlx::{postgres::PgListener, PgPool};

    const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

    const SETUP: &str = r#"
        with p1 as (
          insert into auth.users (id) values
          ('43a18a3e-5a59-11ed-9b6a-0242ac120002') on conflict do nothing
        ),
        p2 as (
          insert into drafts (id, user_id) values
          ('1110000000000000', '43a18a3e-5a59-11ed-9b6a-0242ac120002')
        ),
        p3 as (
          insert into publications (id, job_status, user_id, draft_id) values
          ('1111100000000000', '{"type": "queued"}'::json, '43a18a3e-5a59-11ed-9b6a-0242ac120002', '1110000000000000')
        ),
        p4 as (
          insert into role_grants (subject_role, object_role, capability) values
          ('handlerTest/', 'handlerTest/', 'admin')
          on conflict do nothing
        ),
        p5 as (
          insert into user_grants (user_id, object_role, capability) values
          ('43a18a3e-5a59-11ed-9b6a-0242ac120002', 'handlerTest/', 'admin')
          on conflict do nothing
        )
        select 1;
    "#;

    // Delete in reverse order to avoid integrity-check issues
    const CLEANUP: &str = r#"
      with p5 as (
        delete from user_grants where user_id = '43a18a3e-5a59-11ed-9b6a-0242ac120002'
      ),
      p4 as (
        delete from role_grants where subject_role = 'handlerTest/'
      ),
      p3 as (
        delete from publications where id = '1111100000000000'
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
        async fn handle(
            &mut self,
            _: &sqlx::PgPool,
            _allow_background: bool,
        ) -> anyhow::Result<HandleResult> {
            self.notifier.send(()).unwrap();
            Ok(HandleResult::NoJobs)
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
                    // Sets up the database to have a valid publication task
                    sqlx::query(SETUP)
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
                    sqlx::query(CLEANUP).execute(&mut conn).await.unwrap();
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
        sqlx::query(SETUP).execute(&mut txn).await.unwrap();

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
        sqlx::query(CLEANUP).execute(&mut conn).await.unwrap();

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
