use anyhow::Context;
use std::collections::VecDeque;

pub struct Controller {
    pub logs_tx: super::logs::Tx,
    pub repo: super::repo::Repo,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Message {
    data_plane_id: models::Id,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum State {
    Init,
}

impl Default for State {
    fn default() -> Self {
        Self::Init
    }
}

#[derive(Debug)]
pub struct Outcome {
    inner: automations::Action,
}

impl automations::Outcome for Outcome {
    async fn apply<'s>(
        self,
        _txn: &'s mut sqlx::PgConnection,
    ) -> anyhow::Result<automations::Action> {
        Ok(self.inner)
    }
}

impl automations::Executor for Controller {
    const TASK_TYPE: automations::TaskType = automations::TaskType(1);

    type Receive = Message;
    type State = State;
    type Outcome = Outcome;

    #[tracing::instrument(
        ret,
        err(Debug, level = tracing::Level::ERROR),
        skip_all,
        fields(?task_id, ?parent_id, ?state, ?inbox),
    )]
    async fn poll<'s>(
        &'s self,
        pool: &'s sqlx::PgPool,
        task_id: models::Id,
        parent_id: Option<models::Id>,
        state: &'s mut Self::State,
        inbox: &'s mut VecDeque<(models::Id, Option<Message>)>,
    ) -> anyhow::Result<Self::Outcome> {
        if let State::Init = state {
            let Some((_from_id, Some(Message { data_plane_id }))) = inbox.pop_front() else {
                return Ok(Outcome {
                    inner: automations::Action::Suspend,
                });
            };
            // Collapse multiple request to converge the same data-plane.
            while matches!(inbox.front(),
                Some((_from_id, Some(Message { data_plane_id: other_id }))) if *other_id == data_plane_id)
            {
                inbox.pop_front();
            }

            let row = sqlx::query!(
                r#"
                select
                    data_plane_name,
                    data_plane_fqdn,
                    ops_logs_name,
                    ops_stats_name,
                    broker_address,
                    reactor_address,
                    config,
                    hmac_keys,
                    logs_token
                from data_planes
                where id = $1
                "#,
                data_plane_id as models::Id,
            )
            .fetch_one(pool)
            .await
            .context("failed to fetch data-plane row")?;

            tracing::info!(row.data_plane_name, ?row.logs_token, "FETCHED");

            let checkout = self
                .repo
                .checkout(&self.logs_tx, row.logs_token, "origin/main")
                .await?;

            self.repo.return_checkout(checkout);
        };

        anyhow::bail!("failed!")
    }
}
