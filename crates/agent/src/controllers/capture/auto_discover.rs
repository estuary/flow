use anyhow::Context;
use chrono::{DateTime, Utc};
use models::status::{
    capture::{AutoDiscoverFailure, AutoDiscoverOutcome, AutoDiscoverStatus, DiscoverChange},
    publications::PublicationStatus,
};

use crate::{
    controllers::{
        publication_status::{self, PendingPublication},
        ControllerState, NextRun,
    },
    controlplane::ConnectorSpec,
    discovers::DiscoverOutput,
    evolution,
    publications::PublicationResult,
    ControlPlane,
};

async fn try_connector_spec<C: ControlPlane>(
    model: &models::CaptureDef,
    control_plane: &mut C,
) -> anyhow::Result<ConnectorSpec> {
    let models::CaptureEndpoint::Connector(cfg) = &model.endpoint else {
        anyhow::bail!("only connector endpoints are supported for auto-discovery");
    };
    let spec = control_plane
        .get_connector_spec(cfg.image.clone())
        .await
        .context("failed to fetch connector spec")?;
    if spec.resource_path_pointers.is_empty() {
        anyhow::bail!("connector has no resource path pointers");
    }
    Ok(spec)
}

/// Performs an auto-discover if one is due, and returns a boolean
/// indicating whether a publication was performed. If this returns true,
/// then the controller should immediately return and schedule a subsequent
/// run.
pub async fn update<C: ControlPlane>(
    status: &mut AutoDiscoverStatus,
    state: &ControllerState,
    model: &models::CaptureDef,
    control_plane: &mut C,
    pub_status: &mut PublicationStatus,
) -> anyhow::Result<bool> {
    update_next_run(status, state, model, control_plane).await?;
    if status
        .next_at
        .map(|due| control_plane.current_time() <= due)
        .unwrap_or(true)
    {
        return Ok(false);
    }

    tracing::debug!("starting auto-discover");
    // We'll return the original discover error if it fails
    let result = try_auto_discover(state, model, control_plane, pub_status).await;

    // We'll return whether we've actually published anything. If all we did
    // was run a discover that found no changes, then we may proceed with
    // other controller actions.
    let has_changes = match result {
        Ok(outcome) => {
            let has_changes = outcome.is_successful() && outcome.has_changes();
            let result = outcome.get_result();
            record_outcome(status, outcome);
            result?; // return an error if the auto-discover failed

            // Auto-discover was successful, so determine the time of the next attempt
            update_next_run(status, state, model, control_plane).await?;
            has_changes
        }
        Err(error) => {
            tracing::debug!(?error, "auto-discover failed with error");
            let outcome = AutoDiscoverOutcome::error(
                control_plane.current_time(),
                &state.catalog_name,
                &error,
            );
            record_outcome(status, outcome);
            return Err(error);
        }
    };
    Ok(has_changes)
}

async fn update_next_run<C: ControlPlane>(
    status: &mut AutoDiscoverStatus,
    state: &ControllerState,
    model: &models::CaptureDef,
    control_plane: &mut C,
) -> anyhow::Result<()> {
    if model.shards.disable {
        status.next_at = None;
        return Ok(());
    }

    if status.next_at.is_none()
        || status.next_at.is_some_and(|n| {
            status
                .last_success
                .as_ref()
                .map(|ls| ls.ts > n)
                .unwrap_or(false)
        })
    {
        // `next_at` is `None` or else we've successfully completed a
        // discover since, so determine the next auto-discover time.
        // If there's no `connector_tags` row for this capture connector
        // then we cannot discover, so this is an error.
        let connector_spec = try_connector_spec(model, control_plane)
            .await
            .context("fetching connector spec")?;

        let auto_discover_interval = status
            .interval
            .and_then(|i| chrono::Duration::from_std(i).ok())
            .unwrap_or(connector_spec.auto_discover_interval)
            .abs();

        let prev = status
            .last_success
            .as_ref()
            .map(|s| s.ts)
            .unwrap_or(state.created_at);

        let next = prev + auto_discover_interval;
        tracing::debug!(%next, %auto_discover_interval, "determined new next_at time");
        status.next_at = Some(next);
    }
    Ok(())
}

pub fn next_run(status: &AutoDiscoverStatus) -> Option<NextRun> {
    status
        .next_at
        .map(|n| NextRun::after(n).with_jitter_percent(0))
}

async fn publication_finished<C: ControlPlane>(
    mut pub_result: PublicationResult,
    history: &mut PublicationStatus,
    state: &ControllerState,
    control_plane: &mut C,
    model: &models::CaptureDef,
    pending_outcome: &mut AutoDiscoverOutcome,
) -> anyhow::Result<()> {
    pending_outcome.publish_result = Some(pub_result.status.clone());

    // Did the publication result in incompatible collections, which we should evolve?
    let evolve_incompatible = model
        .auto_discover
        .as_ref()
        .unwrap()
        .evolve_incompatible_collections;

    if let Some(incompatible_collections) = pub_result
        .status
        .incompatible_collections()
        .filter(|_| evolve_incompatible)
    {
        let evolve_requests = crate::evolutions_requests(incompatible_collections);
        // This is because we never try to publish materializations, so we
        // should never see incompatibilities that don't require re-creating
        // the collection.
        assert!(
            evolve_requests.iter().all(|r| r.new_name.is_some()),
            "expected all evolutions to re-create collections"
        );
        assert!(!evolve_requests.is_empty());
        let mut draft = std::mem::take(&mut pub_result.draft);
        draft.errors.clear();
        let evolution_result = control_plane
            .evolve_collections(draft, evolve_requests)
            .await
            .context("evolving collections")?;
        if !evolution_result.is_success() {
            tracing::warn!("evolution failed");
            pending_outcome.errors.extend(
                evolution_result
                    .draft
                    .errors
                    .iter()
                    .map(tables::Error::to_draft_error),
            );
        } else {
            let evolution::EvolutionOutput { draft, actions } = evolution_result;
            tracing::info!(
                collection_count = actions.len(),
                "successfully re-created collections"
            );
            let new_detail = format!(
                "{}, and re-creating {} collections",
                pub_result.detail.as_deref().unwrap_or("no detail"),
                actions.len()
            );
            pending_outcome.re_created_collections = actions;
            let new_result = control_plane
                .publish(
                    Some(new_detail),
                    state.logs_token,
                    draft,
                    state.data_plane_name.clone(),
                )
                .await
                .context("publishing evolved collections")?;
            publication_status::record_result(history, publication_status::pub_info(&new_result));
            pending_outcome.publish_result = Some(new_result.status.clone());
        }
    }

    return Ok(());
}

pub fn new_outcome(
    ts: DateTime<Utc>,
    output: DiscoverOutput,
) -> (AutoDiscoverOutcome, tables::DraftCatalog) {
    let DiscoverOutput {
        capture_name: _,
        draft,
        added,
        modified,
        removed,
    } = output;

    let errors = draft
        .errors
        .iter()
        .map(tables::Error::to_draft_error)
        .collect();

    let outcome = AutoDiscoverOutcome {
        ts,
        added: added
            .into_iter()
            .map(|(rp, change)| DiscoverChange::new(rp, change))
            .collect(),
        modified: modified
            .into_iter()
            .map(|(rp, change)| DiscoverChange::new(rp, change))
            .collect(),
        removed: removed
            .into_iter()
            .map(|(rp, change)| DiscoverChange::new(rp, change))
            .collect(),
        errors,
        re_created_collections: Default::default(),
        publish_result: None,
    };
    (outcome, draft)
}

async fn try_auto_discover<C: ControlPlane>(
    state: &ControllerState,
    model: &models::CaptureDef,
    control_plane: &mut C,
    pub_status: &mut PublicationStatus,
) -> anyhow::Result<AutoDiscoverOutcome> {
    let update_only = !model.auto_discover.as_ref().unwrap().add_new_bindings;
    let capture_name = models::Capture::new(&state.catalog_name);

    let mut draft = tables::DraftCatalog::default();
    draft.captures.insert(tables::DraftCapture {
        capture: capture_name.clone(),
        scope: tables::synthetic_scope(models::CatalogType::Capture, &capture_name),
        expect_pub_id: Some(state.last_pub_id),
        model: Some(model.clone()),
        // start with a touch. The discover merge will set this to false if it actually updates the capture
        is_touch: true,
    });

    let mut output = control_plane
        .discover(
            models::Capture::new(&state.catalog_name),
            draft,
            update_only,
            state.logs_token,
            state.data_plane_id,
        )
        .await
        .context("failed to discover")?;

    // Return early if there was a discover error.
    if !output.is_success() {
        let (outcome, _) = new_outcome(control_plane.current_time(), output);
        return Ok(outcome);
    }

    // The discover was successful, but has anything actually changed?
    // First prune the discovered draft to remove any unchanged specs.
    let unchanged_count = output.prune_unchanged_specs();
    let is_unchanged = output.is_unchanged();
    tracing::info!(
        %is_unchanged,
        %unchanged_count,
        added=output.added.len(),
        removed=output.removed.len(),
        modified=output.modified.len(),
        "auto-discover succeeded"
    );
    if is_unchanged {
        let (outcome, _) = new_outcome(control_plane.current_time(), output);
        return Ok(outcome);
    }

    // There are changes to publish
    let (mut outcome, draft) = new_outcome(control_plane.current_time(), output);

    assert!(
        draft.spec_count() > 0,
        "draft should have at least one spec since has_changes() returned true"
    );

    let mut pending = PendingPublication::new();
    let publish_detail = format!(
        "auto-discover changes ({} added, {} modified, {} removed)",
        outcome.added.len(),
        outcome.modified.len(),
        outcome.removed.len(),
    );
    pending.details.push(publish_detail);
    // Add the draft back into the pending publication, so it will be published.
    pending.draft = draft;
    let initial_pub_result = pending
        .finish(state, pub_status, control_plane)
        .await
        .context("executing publication")?;

    publication_finished(
        initial_pub_result,
        pub_status,
        state,
        control_plane,
        model,
        &mut outcome,
    )
    .await?;

    Ok(outcome)
}

fn record_outcome(status: &mut AutoDiscoverStatus, outcome: AutoDiscoverOutcome) {
    if outcome.is_successful() {
        tracing::info!(?outcome, "auto-discover completed successfully");
        status.failure = None;
        status.last_success = Some(outcome);
        return;
    }

    tracing::info!(?outcome, "auto-discover failed");
    if let Some(failure) = status.failure.as_mut() {
        failure.count += 1;
        failure.last_outcome = outcome;
    } else {
        status.failure = Some(AutoDiscoverFailure {
            count: 1,
            first_ts: outcome.ts,
            last_outcome: outcome,
        });
    };
}
