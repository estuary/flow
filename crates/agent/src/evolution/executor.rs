use super::{evolve, Evolution, EvolutionOutput, EvolveRequest, EvolvedCollection};
use agent_sql::evolutions::{fetch_evolution, Row};
use anyhow::Context;
use itertools::Itertools;
use models::Id;
use serde::{Deserialize, Serialize};

pub struct EvolutionExecutor;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum JobStatus {
    EvolutionFailed {
        error: String,
    },
    Success {
        evolved_collections: Vec<EvolvedCollection>,
        publication_id: Option<Id>,
    },
    Queued,
}

pub struct EvolutionOutcome {
    id: Id,
    time_queued: chrono::Duration,
    result: anyhow::Result<EvolutionOutput>,
    draft_id: Id,
}

impl automations::Outcome for EvolutionOutcome {
    async fn apply<'s>(
        self,
        txn: &'s mut sqlx::PgConnection,
    ) -> anyhow::Result<automations::Action> {
        let EvolutionOutcome {
            id,
            time_queued,
            result,
            draft_id,
        } = self;
        let (job_status, draft_errors) = match result {
            Ok(outcome) if outcome.is_success() => {
                agent_sql::drafts::delete_errors(draft_id, txn).await?;
                crate::draft::upsert_draft_catalog(draft_id, &outcome.draft, txn).await?;
                // If we've re-created a collection, delete the old collection from the draft.
                // Note that this does _not_ delete the collection from the live catalog.
                let delete_specs = outcome
                    .actions
                    .iter()
                    .filter(|a| a.old_name != a.new_name)
                    .map(|a| a.old_name.as_str())
                    .collect::<Vec<_>>();
                if !delete_specs.is_empty() {
                    agent_sql::drafts::delete_specs(draft_id, &delete_specs, txn).await?;
                }
                let status = JobStatus::Success {
                    evolved_collections: outcome.actions,
                    publication_id: None,
                };
                (status, Vec::new())
            }
            Ok(outcome) => {
                let errors = outcome
                    .draft
                    .errors
                    .iter()
                    .map(tables::Error::to_draft_error)
                    .collect::<Vec<_>>();
                let error = outcome.draft.errors.iter().map(|e| &e.error).join(", ");
                (JobStatus::EvolutionFailed { error }, errors)
            }
            Err(err) => {
                let status = JobStatus::EvolutionFailed {
                    error: format!("{err:#}"),
                };
                let draft_err = models::draft_error::Error {
                    catalog_name: String::new(),
                    scope: None,
                    detail: format!("{err:#}"),
                };
                (status, vec![draft_err])
            }
        };
        crate::draft::insert_errors(draft_id, draft_errors, txn).await?;
        let status = serde_json::to_value(job_status)?;
        tracing::info!(%id, %time_queued, %status, "evolution finished");
        agent_sql::evolutions::resolve(id, &status, txn).await?;
        Ok(automations::Action::Done)
    }
}

impl automations::Executor for EvolutionExecutor {
    const TASK_TYPE: automations::TaskType = automations::task_types::EVOLUTIONS;

    type Receive = serde_json::Value;

    type State = ();

    type Outcome = EvolutionOutcome;

    async fn poll<'s>(
        &'s self,
        pool: &'s sqlx::PgPool,
        task_id: models::Id,
        _parent_id: Option<models::Id>,
        _state: &'s mut Self::State,
        inbox: &'s mut std::collections::VecDeque<(models::Id, Option<Self::Receive>)>,
    ) -> anyhow::Result<Self::Outcome> {
        tracing::debug!(?inbox, %task_id, "running evolution task");
        let row = fetch_evolution(task_id, pool).await?;

        let time_queued = chrono::Utc::now().signed_duration_since(row.updated_at);
        let output = do_evolution(&row, pool).await;

        inbox.clear();
        Ok(EvolutionOutcome {
            id: task_id,
            time_queued,
            result: output,
            draft_id: row.draft_id,
        })
    }
}

#[tracing::instrument(skip_all, fields(id = %row.id, ))]
async fn do_evolution(row: &Row, db: &sqlx::PgPool) -> anyhow::Result<EvolutionOutput> {
    let Row {
        draft_id,
        user_id,
        collections,
        ..
    } = row;
    let collections_requests: Vec<EvolveRequest> =
        serde_json::from_str(collections.get()).context("invalid 'collections' input")?;

    if collections_requests.is_empty() {
        anyhow::bail!("evolution collections parameter is empty");
    }

    let draft = crate::draft::load_draft(*draft_id, db).await?;

    let evolution = Evolution {
        draft,
        requests: collections_requests,
        user_id: *user_id,
        require_user_can_admin: true,
    };
    evolve(evolution, db).await
}
