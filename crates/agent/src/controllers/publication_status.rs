use crate::{
    controllers::ControllerErrorExt,
    controlplane::ControlPlane,
    publications::{self, PublicationResult},
};
use anyhow::Context;
use chrono::{DateTime, Utc};
use models::{AnySpec, Id, ModelDef};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, VecDeque};

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
}

impl Dependencies {
    pub fn is_publication_required(&self, state: &ControllerState) -> bool {
        self.max_last_pub_id()
            .is_some_and(|id| id > state.last_pub_id)
            || !self.deleted.is_empty()
    }

    pub async fn resolve<C: ControlPlane>(
        live_spec: &Option<AnySpec>,
        control_plane: &mut C,
    ) -> anyhow::Result<Dependencies> {
        let Some(model) = live_spec.as_ref() else {
            return Ok(Dependencies {
                live: Default::default(),
                deleted: Default::default(),
            });
        };
        let all_deps = model.all_dependencies();
        let live = control_plane
            .get_live_specs(all_deps.clone())
            .await
            .with_retry(NextRun::after_minutes(60))?;
        let mut deleted = all_deps;
        for name in live.all_spec_names() {
            deleted.remove(name);
        }
        Ok(Dependencies { live, deleted })
    }

    /// Returns an id for the next publication in response to a publication of one or more
    /// dependencies. Generally, this is just the largest `last_pub_id` from among the
    /// dependencies, but there's a notable edge case regarding deletions. We cannot know
    /// the `last_pub_id` of any deleted dependencies. Even if we tried to query the soft-deleted
    /// `live_specs` row, we'd be racing against that spec's controller, which will be trying
    /// to hard-delete it. So, if any dependencies have been deleted, we generate a new publication
    /// id. Note that this won't pose a problem for cyclic dependencies because you can only delete
    /// something once.
    pub fn next_pub_id<C: ControlPlane>(&self, control_plane: &mut C) -> models::Id {
        self.max_last_pub_id()
            .filter(|_| self.deleted.is_empty())
            .unwrap_or_else(|| control_plane.next_pub_id())
    }

    fn max_last_pub_id(&self) -> Option<models::Id> {
        self.live.last_pub_ids().max()
    }
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

#[derive(Debug)]
pub struct PendingPublication {
    pub id: Id,
    pub draft: tables::DraftCatalog,
    pub details: Vec<String>,
}

impl PartialEq for PendingPublication {
    fn eq(&self, _: &Self) -> bool {
        // Pending publications are never equal, because we ought to never be comparing statuses
        // while a publication is still pending.
        false
    }
}

impl PendingPublication {}

/// Information on the publications performed by the controller.
/// This does not include any information on user-initiated publications.
#[derive(Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct PublicationStatus {
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
    // TODO(phil): move `PendingPublication` out of this struct
    #[serde(default, skip)]
    pub pending: Option<PendingPublication>,
}

impl Clone for PublicationStatus {
    fn clone(&self) -> Self {
        PublicationStatus {
            target_pub_id: self.target_pub_id,
            max_observed_pub_id: self.max_observed_pub_id,
            history: self.history.clone(),
            pending: None,
        }
    }
}

impl Default for PublicationStatus {
    fn default() -> Self {
        PublicationStatus {
            target_pub_id: Id::zero(),
            max_observed_pub_id: Id::zero(),
            history: VecDeque::new(),
            pending: None,
        }
    }
}

impl PublicationStatus {
    const MAX_HISTORY: usize = 5;

    pub fn update_pending_draft<'a, 'c, C: ControlPlane>(
        &'a mut self,
        add_detail: String,
        cp: &'c mut C,
    ) -> &mut PendingPublication {
        if self.pending.is_none() {
            let id = cp.next_pub_id();
            tracing::debug!(publication_id = ?id, "creating new publication");
            self.pending = Some(PendingPublication {
                id,
                draft: tables::DraftCatalog::default(),
                details: Vec::new(),
            });
        }
        let pending = self.pending.as_mut().unwrap();
        pending.details.push(add_detail);
        pending
    }

    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
    }

    pub async fn start_spec_update<'a>(
        &'a mut self,
        pub_id: models::Id,
        state: &ControllerState,
        detail: String,
    ) -> &'a mut tables::DraftCatalog {
        self.target_pub_id = self.target_pub_id.max(pub_id);
        let model = state
            .live_spec
            .as_ref()
            .expect("cannot state spec update after live spec has been deleted");
        let draft = draft_publication(state, model);
        self.pending = Some(PendingPublication {
            id: pub_id,
            draft,
            details: vec![detail],
        });

        &mut self.pending.as_mut().unwrap().draft
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
        self.history.push_front(publication);
        while self.history.len() > PublicationStatus::MAX_HISTORY {
            self.history.pop_back();
        }
    }

    #[tracing::instrument(skip_all, err)]
    pub async fn finish_pending_publication<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        cp: &mut C,
    ) -> anyhow::Result<PublicationResult> {
        let pending = self
            .pending
            .take()
            .ok_or_else(|| anyhow::anyhow!("no pending publication to finish"))?;
        let detail = pending.details.join(", ");
        let result = cp
            .publish(pending.id, Some(detail), state.logs_token, pending.draft)
            .await?;

        self.record_result(PublicationInfo::observed(&result));
        if result.status.is_success() {
            self.max_observed_pub_id = result.pub_id;
            cp.notify_dependents(state.catalog_name.clone()).await?;
        }

        Ok(result)
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
