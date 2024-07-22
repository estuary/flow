use crate::{
    controllers::ControllerErrorExt,
    controlplane::ControlPlane,
    publications::{self, PublicationResult},
};
use anyhow::Context;
use chrono::{DateTime, TimeDelta, Utc};
use models::{AnySpec, Id, ModelDef};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, VecDeque},
    time::Duration,
};

use super::{backoff_data_plane_activate, ControllerState, NextRun};

/// Information about the dependencies of a live spec.
pub struct Dependencies {
    /// Dependencies that have not been deleted (but might have been updated).
    /// The `last_pub_id` of each spec can be used to determine whether the dependent needs to
    /// be published.
    pub live: tables::LiveCatalog,
    /// Dependencies that have been deleted. If this is non-empty, then the dependent needs to be
    /// published.
    pub deleted: BTreeSet<String>,

    /// If a publication is required and permitted, then this will be Some non-zero publication id.
    /// If `next_pub_id` is `None`, then either no publication is necessary (`next_run` will be
    /// `None`), or the publication must be deferred in order to respect the
    /// `min_publication_interval` (`next_run` will be set to the time at which the publication
    /// may proceed).
    pub next_pub_id: Option<Id>,
    /// If a publication is required, but should be deferred, then `next_run` will be set to the
    /// time at which we should next publish. There's a limit on how frequently a given spec should
    /// publish in response to changes in dependencies. This is important when, for example, a
    /// task depends on many collections that use schema inference, which themselves get published
    /// frequently. If a materialization re-published itself in response to every collection
    /// publication, the publications could be so frequent as to make UI-initiated publications
    /// very likely to fail due to `expect_pub_id` mismatches.
    pub next_run: Option<NextRun>,
}

fn default_min_pub_interval() -> Duration {
    Duration::from_secs(60 * 30)
}

fn duration_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "type": ["string", "null"],
        "pattern": "^\\d+(s|m|h)$"
    }))
    .unwrap()
}

/// Status of the activation of the task in the data-plane
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct ActivationStatus {
    /// The publication id that was last activated in the data plane.
    /// If this is less than the `last_pub_id` of the controlled spec,
    /// then an activation is still pending.
    #[serde(default = "Id::zero", skip_serializing_if = "Id::is_zero")]
    pub last_activated: Id,
}

impl Default for ActivationStatus {
    fn default() -> Self {
        Self {
            last_activated: Id::zero(),
        }
    }
}

impl ActivationStatus {
    pub async fn update<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        control_plane: &mut C,
    ) -> anyhow::Result<()> {
        if state.last_pub_id > self.last_activated {
            let name = state.catalog_name.clone();
            let built_spec = state.built_spec.as_ref().expect("built_spec must be Some");
            control_plane
                .data_plane_activate(name, built_spec)
                .await
                .with_retry(backoff_data_plane_activate(state.failures))
                .context("failed to activate")?;
            tracing::debug!(last_activated = %self.last_activated, "activated");
            self.last_activated = state.last_pub_id;
        }
        Ok(())
    }
}

/// Summary of a publication that was attempted by a controller.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, JsonSchema)]
pub struct PublicationInfo {
    pub id: Id,
    /// Time at which the publication was initiated
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "super::datetime_schema")]
    pub created: Option<DateTime<Utc>>,
    /// Time at which the publication was completed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "super::datetime_schema")]
    pub completed: Option<DateTime<Utc>>,
    /// A brief description of the reason for the publication
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// The final result of the publication
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<publications::JobStatus>,
    /// Errors will be non-empty for publications that were not successful
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<crate::draft::Error>,
}

impl PublicationInfo {
    pub fn is_success(&self) -> bool {
        // TODO: should EmptyDraft be considered successful?
        self.result.as_ref().is_some_and(|s| s.is_success())
    }

    pub fn observed(publication: &PublicationResult) -> Self {
        PublicationInfo {
            id: publication.pub_id,
            created: Some(publication.started_at),
            completed: Some(publication.completed_at),
            result: Some(publication.status.clone()),
            detail: publication.detail.clone(),
            errors: publication.draft_errors(),
        }
    }
}

/// Represents a draft that is pending publication
#[derive(Debug)]
pub struct PendingPublication {
    /// The publication id, or 0 if none has yet been determined
    pub id: Id,
    /// The draft to be published
    pub draft: tables::DraftCatalog,
    /// Reasons for updating the draft, which will be joined together to become
    /// the `detail` of the publication.
    pub details: Vec<String>,
}

impl PartialEq for PendingPublication {
    fn eq(&self, _: &Self) -> bool {
        // Pending publications are never equal, because we ought to never be comparing statuses
        // while a publication is still pending.
        false
    }
}

impl PendingPublication {
    pub fn new() -> Self {
        PendingPublication {
            id: Id::zero(),
            draft: tables::DraftCatalog::default(),
            details: Vec::new(),
        }
    }

    pub fn has_pending(&self) -> bool {
        self.draft.spec_count() > 0
    }

    pub fn start_spec_update(
        &mut self,
        pub_id: models::Id,
        state: &ControllerState,
        detail: impl Into<String>,
    ) -> &mut tables::DraftCatalog {
        self.id = pub_id;
        let model = state
            .live_spec
            .as_ref()
            .expect("cannot start spec update after live spec has been deleted");
        self.draft = draft_publication(state, model);

        self.update_pending_draft(detail)
    }

    pub fn update_pending_draft(&mut self, detail: impl Into<String>) -> &mut tables::DraftCatalog {
        self.details.push(detail.into());
        &mut self.draft
    }

    pub async fn finish<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        status: &mut PublicationStatus,
        control_plane: &mut C,
    ) -> anyhow::Result<PublicationResult> {
        // If no publication id has been assigned, do so now
        if self.id.is_zero() {
            self.id = control_plane.next_pub_id();
            status.target_pub_id = self.id;
        }
        let PendingPublication { id, draft, details } =
            std::mem::replace(self, PendingPublication::new());

        let detail = details.join(", ");
        let result = control_plane
            .publish(id, Some(detail), state.logs_token, draft)
            .await;
        match result.as_ref() {
            Ok(r) => {
                status.record_result(PublicationInfo::observed(r));
                if r.status.is_success() {
                    control_plane
                        .notify_dependents(state.catalog_name.clone())
                        .await
                        .context("notifying dependents after successful publication")?;
                    status.max_observed_pub_id = id;
                }
            }
            Err(err) => {
                let info = PublicationInfo {
                    id,
                    completed: Some(control_plane.current_time()),
                    detail: Some(details.join(", ")),
                    errors: vec![crate::draft::Error {
                        detail: format!("{err:#}"),
                        ..Default::default()
                    }],
                    created: None,
                    result: None,
                };
                status.record_result(info);
            }
        }
        result
    }
}

/// Information on the publications performed by the controller.
/// This does not include any information on user-initiated publications.
#[derive(Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct PublicationStatus {
    /// Time of the last controller-initiated publication
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "super::datetime_schema")]
    pub last_publication_time: Option<DateTime<Utc>>,
    /// The minimim time to wait between controller-initiated publications.
    #[serde(default = "default_min_pub_interval", with = "humantime_serde")]
    #[schemars(schema_with = "duration_schema")]
    pub min_publication_interval: Duration,
    /// The largest `last_pub_id` among all of this spec's dependencies.
    /// For example, for materializations the dependencies are all of
    /// the collections of all enabled binding `source`s, as well as the
    /// `sourceCapture`. If any of these are published, it will increase
    /// the `target_pub_id` and the materialization will be published at
    /// `target_pub_id` in turn.
    #[serde(default = "Id::zero", skip_serializing_if = "Id::is_zero")]
    pub target_pub_id: Id,
    /// The publication id at which the controller has last notified dependent
    /// specs. A publication of the controlled spec will cause the controller to
    /// notify the controllers of all dependent specs. When it does so, it sets
    /// `max_observed_pub_id` to the current `last_pub_id`, so that it can avoid
    /// notifying dependent controllers unnecessarily.
    #[serde(default = "Id::zero", skip_serializing_if = "Id::is_zero")]
    pub max_observed_pub_id: Id,
    /// A limited history of publications performed by this controller
    pub history: VecDeque<PublicationInfo>,
}

impl Clone for PublicationStatus {
    fn clone(&self) -> Self {
        PublicationStatus {
            last_publication_time: self.last_publication_time.clone(),
            min_publication_interval: self.min_publication_interval.clone(),
            target_pub_id: self.target_pub_id,
            max_observed_pub_id: self.max_observed_pub_id,
            history: self.history.clone(),
        }
    }
}

impl Default for PublicationStatus {
    fn default() -> Self {
        PublicationStatus {
            last_publication_time: None,
            min_publication_interval: default_min_pub_interval(),
            target_pub_id: Id::zero(),
            max_observed_pub_id: Id::zero(),
            history: VecDeque::new(),
        }
    }
}

impl PublicationStatus {
    const MAX_HISTORY: usize = 5;

    pub async fn resolve_dependencies<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        control_plane: &mut C,
    ) -> anyhow::Result<Dependencies> {
        let Some(model) = state.live_spec.as_ref() else {
            return Ok(Dependencies {
                live: Default::default(),
                deleted: Default::default(),
                next_pub_id: None,
                next_run: None,
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

        let mut next_pub_id = if deleted.is_empty() {
            live.last_pub_ids()
                .max()
                .filter(|id| id > &state.last_pub_id)
        } else {
            // If any dependencies have been deleted, then we'll need to publish
            // at a new pub_id, since we cannot know the `last_pub_id` of
            // deleted specs.
            Some(control_plane.next_pub_id())
        };

        let next_run = if next_pub_id.is_some() {
            self.can_publish_at(control_plane.current_time())
        } else {
            None
        };
        if next_run.is_some() {
            next_pub_id = None;
            tracing::info!("deferring publication in response to dependency publish");
        };
        if let Some(next) = next_pub_id {
            self.target_pub_id = next;
        }
        Ok(Dependencies {
            live,
            deleted,
            next_pub_id,
            next_run,
        })
    }

    fn can_publish_at(&self, time: DateTime<Utc>) -> Option<NextRun> {
        let Some(last_pub) = self.last_publication_time else {
            return None;
        };
        let minimum = TimeDelta::from_std(self.min_publication_interval).unwrap_or_else(|_| {
                tracing::warn!(interval = ?self.min_publication_interval, "min_publication_interval out of range, falling back to TimeDelta::max_value");
                TimeDelta::max_value()
            });
        let elapsed = time.signed_duration_since(last_pub);

        if elapsed >= minimum {
            None
        } else {
            let diff = minimum - elapsed;
            let after_seconds = diff.num_seconds().abs().min(u32::max_value() as i64) as u32;
            tracing::info!(?diff, ?after_seconds, pub_interval = ?minimum, ?elapsed, ?last_pub, ?time, "delaying publication because last_pub was too recent");
            Some(NextRun {
                after_seconds,
                jitter_percent: 0, // don't add jitter if we're delaying here
            })
        }
    }

    pub async fn notify_dependents<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        control_plane: &mut C,
    ) -> anyhow::Result<()> {
        if state.last_pub_id > self.max_observed_pub_id {
            control_plane
                .notify_dependents(state.catalog_name.clone())
                .await?;
            self.max_observed_pub_id = state.last_pub_id;
        }
        Ok(())
    }

    pub fn record_result(&mut self, publication: PublicationInfo) {
        tracing::info!(pub_id = ?publication.id, status = ?publication.result, "controller finished publication");
        if publication.is_success() {
            self.last_publication_time = publication.completed;
        }
        self.history.push_front(publication);
        while self.history.len() > PublicationStatus::MAX_HISTORY {
            self.history.pop_back();
        }
    }
}

fn draft_publication(state: &ControllerState, live_spec: &AnySpec) -> tables::DraftCatalog {
    let mut draft = tables::DraftCatalog::default();
    draft.add_any_spec(
        &state.catalog_name,
        live_spec.clone(),
        Some(state.last_pub_id),
    );
    draft
}
