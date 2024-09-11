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

use super::{backoff_data_plane_activate, ControllerState};

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

/// Status of the activation of the task in the data-plane
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct ActivationStatus {
    /// The build id that was last activated in the data plane.
    /// If this is less than the `last_build_id` of the controlled spec,
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
        if state.last_build_id > self.last_activated {
            let name = state.catalog_name.clone();
            let built_spec = state.built_spec.as_ref().expect("built_spec must be Some");
            control_plane
                .data_plane_activate(name, built_spec, state.data_plane_id)
                .await
                .with_retry(backoff_data_plane_activate(state.failures))
                .context("failed to activate")?;
            tracing::debug!(last_activated = %state.last_build_id, "activated");
            self.last_activated = state.last_build_id;
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
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_touch: bool,
    #[serde(default = "default_count", skip_serializing_if = "is_one")]
    #[schemars(schema_with = "count_schema")]
    pub count: u32,
}

/// Used for publication info serde
fn is_false(b: &bool) -> bool {
    !*b
}

/// Used for publication info serde
fn default_count() -> u32 {
    1
}

/// Used for publication info serde
fn is_one(i: &u32) -> bool {
    *i == 1
}

fn count_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "type": "integer",
        "minimum": 1,
    }))
    .unwrap()
}

impl PublicationInfo {
    pub fn is_success(&self) -> bool {
        // TODO: should EmptyDraft be considered successful?
        self.result.as_ref().is_some_and(|s| s.is_success())
    }

    pub fn observed(publication: &PublicationResult) -> Self {
        let is_touch = publication.draft.tests.iter().all(|r| r.is_touch)
            && publication.draft.collections.iter().all(|r| r.is_touch)
            && publication.draft.captures.iter().all(|r| r.is_touch)
            && publication
                .draft
                .materializations
                .iter()
                .all(|r| r.is_touch);
        PublicationInfo {
            id: publication.pub_id,
            created: Some(publication.started_at),
            completed: Some(publication.completed_at),
            result: Some(publication.status.clone()),
            detail: publication.detail.clone(),
            errors: publication.draft_errors(),
            count: 1,
            is_touch,
        }
    }

    fn try_reduce(&mut self, other: PublicationInfo) -> Option<PublicationInfo> {
        if !self.is_touch || !other.is_touch || self.result != other.result {
            return Some(other);
        }
        self.count += other.count;
        self.completed = other.completed;
        self.errors = other.errors;
        self.detail = other.detail;
        None
    }
}

/// Represents a draft that is pending publication
#[derive(Debug)]
pub struct PendingPublication {
    pub is_touch: bool,
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
            is_touch: false,
            draft: tables::DraftCatalog::default(),
            details: Vec::new(),
        }
    }

    pub fn has_pending(&self) -> bool {
        self.draft.spec_count() > 0
    }

    pub fn start_touch(&mut self, state: &ControllerState, new_dependency_hash: Option<&str>) {
        tracing::info!("starting touch");
        let new_hash = new_dependency_hash.unwrap_or("None");
        let old_hash = state.live_dependency_hash.as_deref().unwrap_or("None");
        self.details.push(format!("in response to change in dependencies, prev hash: {old_hash}, new hash: {new_hash}"));

        let model = state
            .live_spec
            .as_ref()
            .expect("cannot start touch after live spec has been deleted");
        self.draft = tables::DraftCatalog::default();
        let catalog_type = state.live_spec.as_ref().unwrap().catalog_type();
        let scope = tables::synthetic_scope(catalog_type, &state.catalog_name);
        self.draft
            .add_spec(
                catalog_type,
                &state.catalog_name,
                scope,
                Some(state.last_pub_id),
                Some(&model.to_raw_value()),
                true,
            )
            .unwrap();
    }

    pub fn start_spec_update(
        &mut self,
        state: &ControllerState,
        detail: impl Into<String>,
    ) -> &mut tables::DraftCatalog {
        tracing::info!("starting spec update");
        let model = state
            .live_spec
            .as_ref()
            .expect("cannot start spec update after live spec has been deleted");
        self.draft = tables::DraftCatalog::default();
        let scope = tables::synthetic_scope(model.catalog_type(), &state.catalog_name);
        self.draft
            .add_spec(
                model.catalog_type(),
                &state.catalog_name,
                scope,
                Some(state.last_pub_id),
                Some(&model.to_raw_value()),
                false,
            )
            .unwrap();

        self.update_pending_draft(detail)
    }

    pub fn update_pending_draft(&mut self, detail: impl Into<String>) -> &mut tables::DraftCatalog {
        self.is_touch = false;
        self.details.push(detail.into());
        &mut self.draft
    }

    pub async fn finish<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        status: &mut PublicationStatus,
        control_plane: &mut C,
    ) -> anyhow::Result<PublicationResult> {
        let pub_id = if self.is_touch {
            debug_assert!(
                self.draft.captures.iter().all(|c| c.is_touch),
                "all drafted specs must have is_touch: true for touch pub"
            );
            debug_assert!(
                self.draft.collections.iter().all(|c| c.is_touch),
                "all drafted specs must have is_touch: true for touch pub"
            );
            debug_assert!(
                self.draft.materializations.iter().all(|c| c.is_touch),
                "all drafted specs must have is_touch: true for touch pub"
            );
            debug_assert!(
                self.draft.tests.iter().all(|c| c.is_touch),
                "all drafted specs must have is_touch: true for touch pub"
            );

            state.last_pub_id
        } else {
            control_plane.next_pub_id()
        };
        let PendingPublication {
            is_touch,
            draft,
            details,
        } = std::mem::replace(self, PendingPublication::new());

        let detail = details.join(", ");
        let result = control_plane
            .publish(pub_id, Some(detail), state.logs_token, draft)
            .await;
        match result.as_ref() {
            Ok(r) => {
                status.record_result(PublicationInfo::observed(r));
                if r.status.is_success() {
                    control_plane
                        .notify_dependents(state.catalog_name.clone())
                        .await
                        .context("notifying dependents after successful publication")?;
                    status.max_observed_pub_id = pub_id;
                }
            }
            Err(err) => {
                let info = PublicationInfo {
                    id: pub_id,
                    completed: Some(control_plane.current_time()),
                    detail: Some(details.join(", ")),
                    errors: vec![crate::draft::Error {
                        detail: format!("{err:#}"),
                        ..Default::default()
                    }],
                    created: None,
                    result: None,
                    count: 1,
                    is_touch,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_hash: Option<String>,
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
            max_observed_pub_id: self.max_observed_pub_id,
            history: self.history.clone(),
            dependency_hash: self.dependency_hash.clone(),
        }
    }
}

impl Default for PublicationStatus {
    fn default() -> Self {
        PublicationStatus {
            dependency_hash: None,
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

    // TODO: fold touch publication into history
    pub fn record_result(&mut self, publication: PublicationInfo) {
        tracing::info!(pub_id = ?publication.id, status = ?publication.result, "controller finished publication");
        let maybe_new_entry = if let Some(last_entry) = self.history.front_mut() {
            last_entry.try_reduce(publication)
        } else {
            Some(publication)
        };
        if let Some(new_entry) = maybe_new_entry {
            self.history.push_front(new_entry);
            while self.history.len() > PublicationStatus::MAX_HISTORY {
                self.history.pop_back();
            }
        }
    }
}
