use crate::{
    controllers::ControllerErrorExt,
    controlplane::ControlPlane,
    publications::{self, PublicationResult},
};
use anyhow::Context;
use chrono::{DateTime, Utc};
use models::Id;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

use super::{backoff_data_plane_activate, ControllerState};

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
    /// Activates the spec in the data plane if necessary.
    pub async fn update<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        control_plane: &mut C,
    ) -> anyhow::Result<()> {
        if state.last_build_id > self.last_activated {
            let name = state.catalog_name.clone();
            let built_spec = state.built_spec.as_ref().expect("built_spec must be Some");

            crate::timeout(
                std::time::Duration::from_secs(60),
                control_plane.data_plane_activate(name, built_spec, state.data_plane_id),
                || "Timeout while activating into data-plane",
            )
            .await
            .with_retry(backoff_data_plane_activate(state.failures))
            .context("failed to activate into data-plane")?;

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

fn is_touch_pub(draft: &tables::DraftCatalog) -> bool {
    draft.tests.iter().all(|r| r.is_touch)
        && draft.collections.iter().all(|r| r.is_touch)
        && draft.captures.iter().all(|r| r.is_touch)
        && draft.materializations.iter().all(|r| r.is_touch)
}

impl PublicationInfo {
    pub fn is_success(&self) -> bool {
        self.result.as_ref().is_some_and(|s| s.is_success())
    }

    pub fn observed(publication: &PublicationResult) -> Self {
        let is_touch = is_touch_pub(&publication.draft);
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

    /// Tries to reduce `other` into `self` if the two should be combined in the
    /// history. If `other` cannot be reduced into `self`, then it is returned
    /// unmodified.
    ///
    /// Combining events in the history is a way to cram more information into a
    /// smaller summary, and it helps avoid having repeated publications (like
    /// touch publications, which can number in the hundreds) quickly push out
    /// relevant prior events. But it's important that we _only_ combine
    /// publication entries in the specific cases where we know it won't cause
    /// confusion. Two publications should be combined in the history only if
    /// their final `job_status`es are identical (e.g. both `{"type":
    /// "buildFailed"}`). And then only in one of these cases:
    /// - they are both touch publications
    /// - If they are both _unsuccessful_ non-touch publications (i.e. we never
    ///   combine successful publications that have modified the spec)
    fn try_reduce(&mut self, other: PublicationInfo) -> Option<PublicationInfo> {
        if (self.is_touch != other.is_touch)
            || (self.result != other.result)
            || (!self.is_touch && self.is_success())
        {
            return Some(other);
        }
        self.id = other.id;
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
            draft: tables::DraftCatalog::default(),
            details: Vec::new(),
        }
    }

    pub fn of(details: Vec<String>, draft: tables::DraftCatalog) -> PendingPublication {
        PendingPublication { details, draft }
    }

    pub fn has_pending(&self) -> bool {
        self.draft.spec_count() > 0
    }

    pub fn update_model<M: Into<models::AnySpec>>(
        name: &str,
        last_pub_id: Id,
        model: M,
        detail: impl Into<String>,
    ) -> PendingPublication {
        let mut pending = PendingPublication::new();
        pending.details.push(detail.into());
        let model: models::AnySpec = model.into();
        let scope = tables::synthetic_scope(model.catalog_type(), name);
        pending
            .draft
            .add_any_spec(name, scope, Some(last_pub_id), model, false);
        pending
    }

    pub fn start_touch(&mut self, state: &ControllerState, detail: impl Into<String>) {
        let detail = detail.into();
        tracing::debug!(%detail, "starting touch");
        self.details.push(detail);
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
                true, // is_touch
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
        self.details.push(detail.into());
        &mut self.draft
    }

    pub async fn finish<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        status: &mut PublicationStatus,
        control_plane: &mut C,
    ) -> anyhow::Result<PublicationResult> {
        let is_touch = is_touch_pub(&self.draft);
        let PendingPublication { draft, details } =
            std::mem::replace(self, PendingPublication::new());

        let detail = details.join(", ");
        let result = control_plane
            .publish(
                Some(detail),
                state.logs_token,
                draft,
                state.data_plane_name.clone(),
            )
            .await;
        match result.as_ref() {
            Ok(r) => {
                status.record_result(PublicationInfo::observed(r));
                if r.status.is_success() {
                    control_plane
                        .notify_dependents(state.catalog_name.clone())
                        .await
                        .context("notifying dependents after successful publication")?;
                    status.max_observed_pub_id = r.pub_id;
                }
            }
            Err(err) => {
                let info = PublicationInfo {
                    id: models::Id::zero(),
                    completed: Some(control_plane.current_time()),
                    detail: Some(details.join(", ")),
                    errors: vec![crate::draft::Error {
                        detail: format!("publish error: {err:#}"),
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

    pub async fn update_notify_dependents<C: ControlPlane>(
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
        for err in publication.errors.iter() {
            tracing::debug!(?err, "publication error");
        }
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

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_publication_history_folding() {
        let touch_success = PublicationInfo {
            id: models::Id::new([1, 1, 1, 1, 1, 1, 1, 1]),
            created: Some("2024-11-11T11:11:11Z".parse().unwrap()),
            completed: Some("2024-11-22T17:01:01Z".parse().unwrap()),
            detail: Some("touch success".to_string()),
            result: Some(publications::JobStatus::Success),
            errors: Vec::new(),
            is_touch: true,
            count: 1,
        };

        // Sucessful touch publications should be combined
        let other_touch_success = PublicationInfo {
            id: models::Id::new([2, 1, 1, 1, 1, 1, 1, 1]),
            completed: Some("2024-11-22T22:22:22Z".parse().unwrap()),
            created: Some("2024-11-11T11:00:00Z".parse().unwrap()),
            detail: Some("other touch success".to_string()),
            ..touch_success.clone()
        };
        let mut reduced = touch_success.clone();
        assert!(reduced.try_reduce(other_touch_success).is_none());
        assert_eq!(
            reduced,
            PublicationInfo {
                id: models::Id::new([2, 1, 1, 1, 1, 1, 1, 1]),
                completed: Some("2024-11-22T22:22:22Z".parse().unwrap()),
                created: Some("2024-11-11T11:11:11Z".parse().unwrap()),
                detail: Some("other touch success".to_string()),
                result: Some(publications::JobStatus::Success),
                errors: Vec::new(),
                is_touch: true,
                count: 2,
            }
        );

        let reg_success = PublicationInfo {
            id: models::Id::new([3, 1, 1, 1, 1, 1, 1, 1]),
            completed: Some("2024-11-23T23:33:33Z".parse().unwrap()),
            created: Some("2024-11-11T11:11:11Z".parse().unwrap()),
            detail: Some("non-touch success".to_string()),
            result: Some(publications::JobStatus::Success),
            errors: Vec::new(),
            is_touch: false,
            count: 1,
        };
        // Touch success and regular success should not be combined
        let mut touch_subject = touch_success.clone();
        assert!(touch_subject.try_reduce(reg_success.clone()).is_some());

        // Successful non-touch publications should never be combined because we
        // want to preserve the history of modifications to the model.
        let mut reg_subject = reg_success.clone();
        assert!(reg_subject
            .try_reduce(PublicationInfo {
                id: models::Id::new([4, 1, 1, 1, 1, 1, 1, 1]),
                ..reg_success.clone()
            })
            .is_some(),);

        let reg_fail = PublicationInfo {
            id: models::Id::new([5, 1, 1, 1, 1, 1, 1, 1]),
            completed: Some("2024-12-01T01:55:55Z".parse().unwrap()),
            created: Some("2024-11-11T11:11:11Z".parse().unwrap()),
            detail: Some("reg failure".to_string()),
            result: Some(publications::JobStatus::BuildFailed {
                incompatible_collections: Vec::new(),
                evolution_id: None,
            }),
            errors: vec![crate::draft::Error {
                catalog_name: "acmeCo/fail-thing".to_string(),
                scope: None,
                detail: "schmeetail".to_string(),
            }],
            is_touch: false,
            count: 1,
        };

        // A publication with the same unsuccessful status should be combined,
        // and the detail and error should be that of the most recent
        // publication.
        let same_reg_fail = PublicationInfo {
            id: models::Id::new([5, 1, 1, 1, 1, 1, 1, 1]),
            completed: Some("2024-12-01T01:55:55Z".parse().unwrap()),
            detail: Some("same but different reg failure".to_string()),
            errors: vec![crate::draft::Error {
                catalog_name: "acmeCo/fail-thing".to_string(),
                scope: None,
                detail: "a different error".to_string(),
            }],
            ..reg_fail.clone()
        };
        let mut reduced = reg_fail.clone();
        assert!(reduced.try_reduce(same_reg_fail.clone()).is_none());
        assert_eq!(
            reduced,
            PublicationInfo {
                id: models::Id::new([5, 1, 1, 1, 1, 1, 1, 1]),
                completed: Some("2024-12-01T01:55:55Z".parse().unwrap()),
                created: Some("2024-11-11T11:11:11Z".parse().unwrap()),
                detail: Some("same but different reg failure".to_string()),
                errors: vec![crate::draft::Error {
                    catalog_name: "acmeCo/fail-thing".to_string(),
                    scope: None,
                    detail: "a different error".to_string(),
                }],
                result: Some(publications::JobStatus::BuildFailed {
                    incompatible_collections: Vec::new(),
                    evolution_id: None
                }),
                is_touch: false,
                count: 2,
            }
        );

        // A publication with a different status should not be combined
        let diff_reg_fail = PublicationInfo {
            result: Some(publications::JobStatus::BuildFailed {
                incompatible_collections: vec![publications::IncompatibleCollection {
                    collection: "acmeCo/anvils".to_string(),
                    requires_recreation: Vec::new(),
                    affected_materializations: Vec::new(),
                }],
                evolution_id: None,
            }),
            ..same_reg_fail.clone()
        };
        let mut reg_subject = reg_fail.clone();
        assert!(reg_subject.try_reduce(diff_reg_fail).is_some());
    }
}
