use super::{
    backoff_data_plane_activate,
    publication_status::{ActivationStatus, PendingPublication, PublicationInfo},
    ControlPlane, ControllerErrorExt, ControllerState, NextRun,
};
use crate::{
    controllers::publication_status::PublicationStatus,
    controlplane::ConnectorSpec,
    discovers::{Changed, ResourcePath},
    evolution, publications,
};
use crate::{discovers::DiscoverOutput, publications::PublicationResult};
use anyhow::Context;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Status of a capture controller
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
pub struct CaptureStatus {
    // TODO: auto discovers are not yet implemented as controllers, but they should be soon.
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // #[schemars(schema_with = "super::datetime_schema")]
    // pub next_auto_discover: Option<DateTime<Utc>>,
    #[serde(default)]
    pub publications: PublicationStatus,
    #[serde(default)]
    pub activation: ActivationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_discover: Option<AutoDiscoverStatus>,
}

impl CaptureStatus {
    pub async fn update<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        control_plane: &mut C,
        model: &models::CaptureDef,
    ) -> anyhow::Result<Option<NextRun>> {
        let mut pending_pub = PendingPublication::new();
        let dependencies = self
            .publications
            .resolve_dependencies(state, control_plane)
            .await?;
        if dependencies.hash != state.live_dependency_hash {
            if dependencies.deleted.is_empty() {
                pending_pub.start_touch(state, dependencies.hash.as_deref());
            } else {
                let draft = pending_pub.start_spec_update(
                    state,
                    format!("in response to publication of one or more depencencies"),
                );
                tracing::debug!(deleted_collections = ?dependencies.deleted, "disabling bindings for collections that have been deleted");
                let draft_capture = draft
                    .captures
                    .get_mut_by_key(&models::Capture::new(&state.catalog_name))
                    .expect("draft must contain capture");
                let mut disabled_count = 0;
                for binding in draft_capture.model.as_mut().unwrap().bindings.iter_mut() {
                    if dependencies.deleted.contains(binding.target.as_str()) && !binding.disable {
                        disabled_count += 1;
                        binding.disable = true;
                    }
                }
                let detail = format!(
                    "disabled {disabled_count} binding(s) in response to deleted collections: [{}]",
                    dependencies.deleted.iter().format(", ")
                );
                pending_pub.update_pending_draft(detail);
            }
        }

        if model.auto_discover.is_some() {
            let ad_status = self
                .auto_discover
                .get_or_insert_with(AutoDiscoverStatus::default);
            ad_status
                .update(state, control_plane, model, &mut pending_pub)
                .await
                .context("updating auto-discover")?;
        } else {
            // Clear auto-discover status to avoid confusion, but only if
            // auto-discover is disabled. We leave the auto-discover status if
            // shards are disabled, since it's still useful for debugging.
            self.auto_discover = None;
        };

        if pending_pub.has_pending() {
            let mut pub_result = pending_pub
                .finish(state, &mut self.publications, control_plane)
                .await
                .context("failed to execute publish")?;

            let Self {
                publications,
                auto_discover,
                ..
            } = self;
            if let Some(auto_discovers) = auto_discover.as_mut() {
                pub_result = auto_discovers
                    .publication_finished(pub_result, publications, state, control_plane, model)
                    .await?;
            }

            pub_result
                .error_for_status()
                .with_maybe_retry(backoff_publication_failure(state.failures))?;
        } else {
            // Not much point in activating if we just published, since we're going to be
            // immediately invoked again.
            self.activation
                .update(state, control_plane)
                .await
                .with_retry(backoff_data_plane_activate(state.failures))?;
            self.publications
                .notify_dependents(state, control_plane)
                .await
                .context("failed to notify dependents")?;
        }

        // Finally, determine the time at which we should next attampt an auto-discover, if appropriate
        if let Some(auto_discover) = self.auto_discover.as_mut() {
            let maybe_next_run = auto_discover
                .update_next_run(state, model, control_plane)
                .await?;
            Ok(maybe_next_run)
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, JsonSchema, Clone)]
pub struct DiscoverChange {
    /// Identifies the resource in the source system that this change pertains to.
    pub resource_path: ResourcePath,
    /// The target collection of the capture binding that was changed.
    pub target: models::Collection,
    /// Whether the capture binding is disabled.
    pub disable: bool,
}

impl DiscoverChange {
    pub fn new(resource_path: ResourcePath, Changed { target, disable }: Changed) -> Self {
        Self {
            resource_path,
            target,
            disable,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, JsonSchema, Clone)]
pub struct AutoDiscoverOutcome {
    /// Time at which the disocver was attempted
    #[schemars(schema_with = "super::datetime_schema")]
    pub ts: DateTime<Utc>,
    /// Bindings that were added to the capture.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added: Vec<DiscoverChange>,
    /// Bindings that were modified, either to change the schema or the collection key.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub modified: Vec<DiscoverChange>,
    /// Bindings that were removed because they no longer appear in the source system.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed: Vec<DiscoverChange>,
    /// Errors that occurred during the discovery or evolution process.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<crate::draft::Error>,
    /// Collections that were re-created due to the collection key having changed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub re_created_collections: Vec<crate::evolution::EvolvedCollection>,
    /// The result of publishing the discovered changes, if a publication was attempted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_result: Option<publications::JobStatus>,
}

impl AutoDiscoverOutcome {
    fn has_changes(&self) -> bool {
        self.errors.is_empty()
            && (!self.added.is_empty() || !self.modified.is_empty() || !self.removed.is_empty())
    }

    fn spec_error(
        ts: DateTime<Utc>,
        capture_name: &str,
        error: &anyhow::Error,
    ) -> AutoDiscoverOutcome {
        let errors = vec![crate::draft::Error {
            catalog_name: capture_name.to_string(),
            detail: error.to_string(),
            scope: None,
        }];
        AutoDiscoverOutcome {
            ts,
            errors,
            added: Vec::new(),
            modified: Vec::new(),
            removed: Vec::new(),
            re_created_collections: Vec::new(),
            publish_result: None,
        }
    }

    fn from_output(ts: DateTime<Utc>, output: DiscoverOutput) -> (Self, tables::DraftCatalog) {
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
            .map(crate::draft::Error::from_tables_error)
            .collect();

        let outcome = Self {
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
}

#[derive(Debug, Serialize, Deserialize, PartialEq, JsonSchema, Clone)]
pub struct AutoDiscoverFailure {
    /// The number of consecutive failures that have been observed.
    pub count: u32,
    /// The timestamp of the first failure in the current sequence.
    #[schemars(schema_with = "super::datetime_schema")]
    pub first_ts: DateTime<Utc>,
    /// The discover outcome corresponding to the most recent failure. This will
    /// be updated with the results of each retry until an auto-discover
    /// succeeds.
    pub last_outcome: AutoDiscoverOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
pub struct AutoDiscoverStatus {
    /// The interval at which auto-discovery is run. This is normally unset, which uses
    /// the default interval.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "interval_schema")]
    pub interval: Option<std::time::Duration>,

    /// Time at which the next auto-discover should be run.
    #[serde(default)]
    #[schemars(schema_with = "super::datetime_schema")]
    pub next_at: Option<DateTime<Utc>>,
    /// The outcome of the a recent discover, which is about to be published.
    /// This will typically only be observed if the publication failed for some
    /// reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_publish: Option<AutoDiscoverOutcome>,
    /// The outcome of the last _successful_ auto-discover. If `failure` is set,
    /// then that will typically be more recent than `last_success`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success: Option<AutoDiscoverOutcome>,
    /// If auto-discovery has failed, this will include information about that failure.
    /// This field is cleared as soon as a successful auto-discover is run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<AutoDiscoverFailure>,
}

fn interval_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "type": ["string", "null"],
        "pattern": "^\\d+(s|m|h)$"
    }))
    .unwrap()
}

impl AutoDiscoverStatus {
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

    async fn update<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        control_plane: &mut C,
        model: &models::CaptureDef,
        pending: &mut PendingPublication,
    ) -> anyhow::Result<()> {
        if model.shards.disable {
            self.next_at = None;
            return Ok(());
        }
        if !self
            .next_at
            .is_some_and(|due| control_plane.current_time() >= due)
        {
            // Not due yet.
            return Ok(());
        }

        // Time to discover. Start by clearing out any pending publish, since we'll use the outcome
        // of the discover to determine that.
        self.pending_publish = None;

        // We'll return the original discover error if it fails
        let result = self
            .try_auto_discover(state, control_plane, model, pending)
            .await;
        let (outcome, error) = match result {
            Ok(o) => (o, None),
            Err(err) => {
                let outcome = AutoDiscoverOutcome::spec_error(
                    control_plane.current_time(),
                    &state.catalog_name,
                    &err,
                );
                (outcome, Some(err))
            }
        };
        if !outcome.errors.is_empty() {
            let err = error.unwrap_or_else(|| {
                anyhow::anyhow!("discover failed: {}", &outcome.errors[0].detail)
            });
            tracing::info!(error = ?err, "auto-discover failed");

            if let Some(failure) = self.failure.as_mut() {
                failure.count += 1;
                failure.last_outcome = outcome;
            } else {
                self.failure = Some(AutoDiscoverFailure {
                    count: 1,
                    first_ts: control_plane.current_time(),
                    last_outcome: outcome,
                });
            };

            Err(err)
        } else if outcome.has_changes() {
            assert!(
                pending.has_pending(),
                "pending draft must contain specs to publish"
            );
            self.pending_publish = Some(outcome);
            Ok(())
        } else {
            self.failure = None;
            self.last_success = Some(outcome);
            Ok(())
        }
    }

    async fn update_next_run<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        model: &models::CaptureDef,
        control_plane: &mut C,
    ) -> anyhow::Result<Option<NextRun>> {
        if model.shards.disable {
            self.next_at = None;
            return Ok(None);
        }

        // If the current `next_at` is still in the future, then don't overwrite it.
        let next = if let Some(n) = self.next_at.filter(|n| *n > control_plane.current_time()) {
            n
        } else {
            // next_at is none or past, so determine the next auto-discover time
            let next = self
                .determine_next_discover_time(state.created_at, model, control_plane)
                .await?;
            self.next_at = Some(next);
            next
        };
        Ok(Some(NextRun::after(next).with_jitter_percent(0)))
    }

    async fn publication_finished<C: ControlPlane>(
        &mut self,
        mut pub_result: PublicationResult,
        history: &mut PublicationStatus,
        state: &ControllerState,
        control_plane: &mut C,
        model: &models::CaptureDef,
    ) -> anyhow::Result<PublicationResult> {
        let Some(pending_outcome) = self.pending_publish.as_mut() else {
            // Nothing to do if we didn't attempt to publish. This just means that the publication
            // was due to dependency updates, not auto-discover.
            return Ok(pub_result);
        };
        pending_outcome.publish_result = Some(pub_result.status.clone());

        // Did the publication result in incompatible collections, which we should evolve?
        let evolve_incompatible = model
            .auto_discover
            .as_ref()
            .unwrap()
            .evolve_incompatible_collections;

        let evolution_failed = if let Some(incompatible_collections) = pub_result
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
                        .map(crate::draft::Error::from_tables_error),
                );
                true // evolution failed
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
                history.record_result(PublicationInfo::observed(&new_result));
                pending_outcome.publish_result = Some(new_result.status.clone());
                pub_result = new_result;
                false // evolution succeeded
            }
        } else {
            false // no evolution needed
        };

        let pending_outcome = self.pending_publish.take().unwrap();
        if !evolution_failed
            && (pub_result.status.is_success() || pub_result.status.is_empty_draft())
        {
            self.failure = None;
            self.last_success = Some(pending_outcome);
        } else {
            if let Some(fail) = self.failure.as_mut() {
                fail.count += 1;
                fail.last_outcome = pending_outcome;
            } else {
                self.failure = Some(AutoDiscoverFailure {
                    count: 1,
                    first_ts: pending_outcome.ts,
                    last_outcome: pending_outcome,
                });
            }
        }

        return Ok(pub_result);
    }

    async fn try_auto_discover<C: ControlPlane>(
        &self,
        state: &ControllerState,
        control_plane: &mut C,
        model: &models::CaptureDef,
        pending: &mut PendingPublication,
    ) -> anyhow::Result<AutoDiscoverOutcome> {
        let update_only = !model.auto_discover.as_ref().unwrap().add_new_bindings;
        let capture_name = models::Capture::new(&state.catalog_name);

        // Take the draft out of the pending publication, and put it back in at
        // the end if everything is successful. This ensures that we don't
        // clobber other changes that may have already been made to the draft.
        let mut draft = std::mem::take(&mut pending.draft);
        if !draft.captures.get_by_key(&capture_name).is_some() {
            draft.captures.insert(tables::DraftCapture {
                capture: capture_name.clone(),
                scope: tables::synthetic_scope(models::CatalogType::Capture, &capture_name),
                expect_pub_id: Some(state.last_pub_id),
                model: Some(model.clone()),
                // start with a touch. The discover merge will set this to false if it actually updates the capture
                is_touch: true,
            });
        }

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
            let (outcome, _) =
                AutoDiscoverOutcome::from_output(control_plane.current_time(), output);
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
            let (outcome, _) =
                AutoDiscoverOutcome::from_output(control_plane.current_time(), output);
            return Ok(outcome);
        }

        // We must publish the discovered changes
        let (outcome, draft) =
            AutoDiscoverOutcome::from_output(control_plane.current_time(), output);

        assert!(
            draft.spec_count() > 0,
            "draft should have at least one spec since is_unchanged() returned false"
        );

        let publish_detail = format!(
            "auto-discover changes ({} added, {} modified, {} removed)",
            outcome.added.len(),
            outcome.modified.len(),
            outcome.removed.len(),
        );
        pending.details.push(publish_detail);
        // Add the draft back into the pending publication, so it will be published.
        pending.draft = draft;
        Ok(outcome)
    }

    async fn determine_next_discover_time<C: ControlPlane>(
        &self,
        spec_created_at: DateTime<Utc>,
        model: &models::CaptureDef,
        control_plane: &mut C,
    ) -> anyhow::Result<DateTime<Utc>> {
        // If there's no `connector_tags` row for this capture connector then we
        // cannot discover, so this is an error.
        let connector_spec = Self::try_connector_spec(model, control_plane)
            .await
            .context("fetching connector spec")?;

        let auto_discover_interval = self
            .interval
            .and_then(|i| chrono::Duration::from_std(i).ok())
            .unwrap_or(connector_spec.auto_discover_interval)
            .abs();

        let prev = self
            .last_success
            .as_ref()
            .map(|s| s.ts)
            .into_iter()
            .chain(self.failure.as_ref().map(|f| f.last_outcome.ts))
            .chain(Some(spec_created_at))
            .max()
            .unwrap();

        let failures = self.failure.as_ref().map(|f| f.count).unwrap_or(0);

        let delay = match failures {
            0..3 => auto_discover_interval / 8,
            3..8 => auto_discover_interval / 2,
            8.. => auto_discover_interval,
        };
        let backoff = delay * failures as i32;

        let next = prev + auto_discover_interval + backoff;
        tracing::debug!(%prev, %failures, %backoff, %auto_discover_interval, "determined next auto-discover time");
        Ok(next)
    }
}

fn backoff_publication_failure(prev_failures: i32) -> Option<NextRun> {
    if prev_failures < 3 {
        Some(NextRun::after_minutes(prev_failures.max(1) as u32))
    } else if prev_failures < 10 {
        Some(NextRun::after_minutes(prev_failures as u32 * 60))
    } else {
        None
    }
}
