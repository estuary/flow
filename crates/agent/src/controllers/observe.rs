use anyhow::Context;
use chrono::{DateTime, Utc};
use serde_json::value::RawValue;
use std::collections::{BTreeMap, BTreeSet};

use super::{ControlJob, ControllerState, ControllerUpdate, PublicationResult, ALL_CONTROLLERS};

#[tracing::instrument(level = "debug", skip_all)]
pub async fn observe_publication(
    txn: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    publication: &PublicationResult,
) -> anyhow::Result<()> {
    // get the current controller states for anything that's being updated by the current publication.
    // This will include the names of specs that are being deleted by the publication.
    let catalog_names = participating_catalog_names(publication)
        .into_iter()
        .collect::<Vec<_>>();
    let state_rows = agent_sql::controllers::fetch_controller_states(txn, &catalog_names)
        .await
        .context("fetching controller states")?;
    let n_states = state_rows.len();

    // Group states by controller type
    let mut states_by_type: BTreeMap<String, BTreeMap<String, ControllerState<Box<RawValue>>>> =
        BTreeMap::new();
    for state in state_rows {
        let agent_sql::controllers::ControllerJob {
            catalog_name,
            controller,
            next_run,
            updated_at,
            status,
            failures,
            error: _,
            active,
            logs_token: _,
            background: _,
        } = state;

        let type_states = states_by_type
            .entry(controller)
            .or_insert_with(BTreeMap::new);
        type_states.insert(
            catalog_name,
            ControllerState {
                active,
                next_run,
                updated_at,
                status: status.0,
                failures,
            },
        );
    }

    // Collect all the updates to be applied for each controller. These are broken into a separate vec for each
    // column, because sqlx doesn't seem to have a better way to do this.
    let mut catalog_names: Vec<String> = Vec::with_capacity(n_states);
    let mut controllers: Vec<String> = Vec::with_capacity(n_states);
    let mut next_runs: Vec<Option<DateTime<Utc>>> = Vec::with_capacity(n_states);
    let mut statuses: Vec<Option<agent_sql::TextJson<Box<RawValue>>>> =
        Vec::with_capacity(n_states);
    let mut active: Vec<bool> = Vec::with_capacity(n_states);
    for controller in ALL_CONTROLLERS {
        let controller_name = controller.controller_name();
        let states = states_by_type.remove(&controller_name).unwrap_or_default();
        let controller_updates =
            invoke_controller_observe(&controller_name, states, publication, *controller)
                .with_context(|| format!("invoking controller observe for {controller_name}"))?;
        for (catalog_name, update) in controller_updates {
            catalog_names.push(catalog_name);
            controllers.push(controller_name.clone());
            next_runs.push(update.next_run.map(|n| n.compute_time()));
            statuses.push(update.status.map(|s| agent_sql::TextJson(s)));
            active.push(update.active);
        }
    }
    agent_sql::controllers::upsert_many(
        txn,
        &catalog_names,
        &controllers,
        &next_runs,
        &statuses,
        &active,
    )
    .await
    .context("persisting controller updates")?;

    tracing::debug!("finished observing publication");

    Ok(())
}

#[tracing::instrument(level = "debug", err, skip_all, fields(controller_name = controller_name, n_input_states = states.len(), n_updates))]
fn invoke_controller_observe(
    controller_name: &str,
    states: BTreeMap<String, ControllerState<Box<RawValue>>>,
    publication: &PublicationResult,
    controller: &dyn ControlJob<Status = Box<RawValue>>,
) -> anyhow::Result<BTreeMap<String, ControllerUpdate<Box<RawValue>>>> {
    let updates = controller.observe_publication(states, publication)?;
    tracing::Span::current().record("n_updates", &updates.len());
    Ok(updates)
}

fn participating_catalog_names(publication: &PublicationResult) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    names.extend(
        publication
            .draft
            .captures
            .iter()
            .map(|r| r.catalog_name.to_string()),
    );
    names.extend(
        publication
            .draft
            .collections
            .iter()
            .map(|r| r.catalog_name.to_string()),
    );
    names.extend(
        publication
            .draft
            .materializations
            .iter()
            .map(|r| r.catalog_name.to_string()),
    );
    names.extend(
        publication
            .draft
            .tests
            .iter()
            .map(|r| r.catalog_name.to_string()),
    );

    // Don't include expanded live specs for observing failed publications.
    // This is just an optimization, because we don't have any controllers that care about
    // failed publications that they themselves didn't create.
    if publication.publication_status.is_success() {
        names.extend(
            publication
                .live
                .captures
                .iter()
                .map(|r| r.catalog_name.to_string()),
        );
        names.extend(
            publication
                .live
                .collections
                .iter()
                .map(|r| r.catalog_name.to_string()),
        );
        names.extend(
            publication
                .live
                .materializations
                .iter()
                .map(|r| r.catalog_name.to_string()),
        );
        names.extend(
            publication
                .live
                .tests
                .iter()
                .map(|r| r.catalog_name.to_string()),
        );
    }
    names
}
