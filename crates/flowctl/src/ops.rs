use crate::collection::read::ReadBounds;

#[derive(clap::Args, Debug)]
pub struct Logs {
    #[clap(flatten)]
    pub task: TaskSelector,

    #[clap(flatten)]
    pub bounds: ReadBounds,
}

/// Selects a Flow task.
#[derive(clap::Args, Debug, Default, Clone)]
pub struct TaskSelector {
    /// The name of the task
    #[clap(long)]
    pub task: String,
}

impl Logs {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        read_task_ops_journal(
            &ctx.client,
            &self.task.task,
            OpsCollection::Logs,
            &self.bounds,
        )
        .await
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum OpsCollection {
    Logs,
    Stats,
}

pub async fn read_task_ops_journal(
    client: &crate::Client,
    task_name: &str,
    collection: OpsCollection,
    bounds: &ReadBounds,
) -> anyhow::Result<()> {
    let models::authorizations::UserTaskAuthorization {
        broker_address,
        broker_token,
        ops_logs_journal,
        ops_stats_journal,
        ..
    } = flow_client::fetch_user_task_authorization(
        &ctx.client,
        models::authorizations::UserTaskAuthorizationRequest {
            task: selector.task.clone(),
            capability: models::Capability::Read,
            started_unix: 0,
        },
    )
    .await?;

    let journal_client = gazette::journal::Client::new(
        broker_address,
        gazette::Metadata::new().with_bearer_token(&broker_token)?,
        ctx.router.clone(),
    );

    let journal_name = match collection {
        OpsCollection::Logs => ops_logs_journal,
        OpsCollection::Stats => ops_stats_journal,
    };

    crate::collection::read::read_collection_journal(journal_client, &journal_name, bounds).await
}
