use std::collections::BTreeSet;

use anyhow::Context;
use chrono::{DateTime, Utc};
use control_plane_api::publications::PublicationResult;
use models::{AnySpec, ModelDef, status::publications::PublicationStatus};

use crate::{
    ControlPlane,
    controllers::{
        ControllerErrorExt, ControllerState, backoff_publication_failure,
        publication_status::{self, PendingPublication},
    },
};

/// Information about the dependencies of a live spec.
pub struct Dependencies {
    /// Dependencies that have not been deleted (but might have been updated).
    /// The `last_pub_id` of each spec can be used to determine whether the dependent needs to
    /// be published.
    pub live: tables::LiveCatalog,
    /// Dependencies that have been deleted. If this is non-empty, then the dependent needs to be
    /// published.
    pub deleted: BTreeSet<String>,
    pub hash: Option<String>,
}

impl Dependencies {
    /// Fetches all of the live spec dependencies of the current spec, and
    /// computes a new dependency hash. The hash can be compared against the
    /// `state.live_dependency_hash` in order to determine whether a publication
    /// is necessary.
    pub async fn resolve<C: ControlPlane>(
        state: &ControllerState,
        control_plane: &C,
    ) -> anyhow::Result<Dependencies> {
        let Some(model) = state.live_spec.as_ref() else {
            // The spec is being deleted, and thus has no dependencies
            return Ok(Dependencies {
                live: Default::default(),
                deleted: Default::default(),
                hash: None,
            });
        };
        let all_deps = model.all_dependencies();
        let live = control_plane
            .get_live_specs(all_deps.clone())
            .await
            .context("fetching live_specs dependencies")?;
        let mut deleted = all_deps;
        for name in live.all_spec_names() {
            deleted.remove(name);
        }

        let dep_hasher = tables::Dependencies::from_live(&live);
        let hash = match model {
            AnySpec::Capture(c) => dep_hasher.compute_hash(c),
            AnySpec::Collection(c) => dep_hasher.compute_hash(c),
            AnySpec::Materialization(m) => dep_hasher.compute_hash(m),
            AnySpec::Test(t) => dep_hasher.compute_hash(t),
        };

        if hash != state.live_dependency_hash {
            tracing::info!(?state.live_dependency_hash, new_hash = ?hash, deleted_count = %deleted.len(), "spec dependencies have changed");
        }

        Ok(Dependencies {
            live,
            deleted,
            hash,
        })
    }

    /// Publishes the spec if the new dependency hash differs from the live
    /// dependency hash. If any dependencies are found to have been deleted,
    /// calls `handle_deleted` to return a new model and publication detail
    /// message for the publication. Otherwise if the hash is different, a touch
    /// publication will be done.
    ///
    /// Returns a boolean indicating whether any type of publication was
    /// performed. If true, then the controller should return immediately and
    /// schedule a subsequent run.
    pub async fn update<C, DF, M>(
        &mut self,
        state: &ControllerState,
        control_plane: &C,
        pub_status: &mut PublicationStatus,
        handle_deleted: DF,
    ) -> anyhow::Result<Option<PublicationResult>>
    where
        C: ControlPlane,
        DF: FnOnce(&BTreeSet<String>) -> anyhow::Result<(String, M)>,
        M: Into<models::AnySpec>,
    {
        let min_backoff = control_plane.controller_publication_cooldown();
        let mut pending = self
            .start_update(
                state,
                control_plane.current_time(),
                control_plane.controller_publication_cooldown(),
                handle_deleted,
            )
            .await?;
        if pending.has_pending() {
            // Ensure that we're allowed to publish before proceeding
            publication_status::check_can_publish(pub_status, control_plane)?;

            let pub_result = pending
                .finish(state, pub_status, control_plane)
                .await
                .context("failed to execute publish")?
                .error_for_status();
            // The most recent failure in the history is guaranteed to be the
            // right one if this attempt has failed, so an empty prefix works.
            let failures = super::last_pub_failed(pub_status, "")
                .map(|(_, count)| count)
                .unwrap_or(1);
            let success_result =
                pub_result.with_retry(backoff_publication_failure(failures, min_backoff))?;
            Ok(Some(success_result))
        } else {
            Ok(None)
        }
    }

    /// Starts the update process and returns a `PendingPublication`. This is
    /// basically the same as `update` except that it allows the controller to
    /// finish the publication itself if it needs to.
    async fn start_update<DF, M>(
        &mut self,
        state: &ControllerState,
        start_time: DateTime<Utc>,
        min_publication_backoff: chrono::Duration,
        handle_deleted: DF,
    ) -> anyhow::Result<PendingPublication>
    where
        DF: FnOnce(&BTreeSet<String>) -> anyhow::Result<(String, M)>,
        M: Into<models::AnySpec>,
    {
        let mut pending_pub = PendingPublication::new();
        if self.hash == state.live_dependency_hash {
            return Ok(pending_pub);
        }

        let detail = if self.deleted.is_empty() {
            // This is the common case
            let new_hash = self.hash.as_deref().unwrap_or("None");
            let old_hash = state.live_dependency_hash.as_deref().unwrap_or("None");
            format!(
                "in response to change in dependencies, prev hash: {old_hash}, new hash: {new_hash}"
            )
        } else {
            "in response to deletion one or more depencencies".to_string()
        };

        // Do we need to backoff a previous failed attempt? First question is
        // whether the last attempt failed. Note that our use of the full detail
        // when matching the prefix is intentional, so that the backoff gets
        // reset whenever the dependency hashes change.
        if let Some((last, fail_count)) = state
            .current_status
            .publication_status()
            .and_then(|s| super::last_pub_failed(s, &detail))
        {
            let backoff = backoff_publication_failure(fail_count, min_publication_backoff);
            // 0 the jitter when computing here so that we don't randomly use a greater jitter
            // than was determined when the backoff error was first returned. This isn't critical,
            // but avoids potentially "extra" controller runs.
            let next_attempt = last + backoff.with_jitter_percent(0).compute_duration();
            if next_attempt > start_time {
                return super::backoff_err(backoff, "dependency update publication", fail_count);
            }
        }

        if self.deleted.is_empty() {
            pending_pub.start_touch(state, detail);
        } else {
            let (detail, updated_model) = handle_deleted(&self.deleted)
                .context("updating model in response to deleted dependencies")?;
            let updated_model: models::AnySpec = updated_model.into();
            pending_pub = PendingPublication::update_model(
                &state.catalog_name,
                state.last_pub_id,
                updated_model,
                "in response to deletion one or more depencencies",
            );
            pending_pub.details.push(detail);
            tracing::debug!(deleted_collections = ?self.deleted, "disabling bindings for collections that have been deleted");
        }
        Ok(pending_pub)
    }
}
