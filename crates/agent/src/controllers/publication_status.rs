use crate::{controlplane::ControlPlane, publications::PublicationResult};
use anyhow::Context;
use models::{
    draft_error,
    status::publications::{PublicationInfo, PublicationStatus},
    Id,
};

use super::ControllerState;

fn is_touch_pub(draft: &tables::DraftCatalog) -> bool {
    draft.tests.iter().all(|r| r.is_touch)
        && draft.collections.iter().all(|r| r.is_touch)
        && draft.captures.iter().all(|r| r.is_touch)
        && draft.materializations.iter().all(|r| r.is_touch)
}

pub fn pub_info(publication: &PublicationResult) -> PublicationInfo {
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
        control_plane: &C,
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
                record_result(status, pub_info(r));
                if r.status.is_success() {
                    control_plane
                        .notify_dependents(state.live_spec_id)
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
                    errors: vec![draft_error::Error {
                        detail: format!("publish error: {err:#}"),
                        ..Default::default()
                    }],
                    created: None,
                    result: None,
                    count: 1,
                    is_touch,
                };
                record_result(status, info);
            }
        }
        result
    }
}

const MAX_HISTORY: usize = 5;

pub async fn update_notify_dependents<C: ControlPlane>(
    status: &mut PublicationStatus,
    state: &ControllerState,
    control_plane: &C,
) -> anyhow::Result<()> {
    if state.last_pub_id > status.max_observed_pub_id {
        control_plane.notify_dependents(state.live_spec_id).await?;
        status.max_observed_pub_id = state.last_pub_id;
    }
    Ok(())
}

pub fn record_result(status: &mut PublicationStatus, publication: PublicationInfo) {
    tracing::info!(pub_id = ?publication.id, status = ?publication.result, "controller finished publication");
    for err in publication.errors.iter() {
        tracing::debug!(?err, "publication error");
    }
    let maybe_new_entry = if let Some(last_entry) = status.history.front_mut() {
        last_entry.try_reduce(publication)
    } else {
        Some(publication)
    };
    if let Some(new_entry) = maybe_new_entry {
        status.history.push_front(new_entry);
        while status.history.len() > MAX_HISTORY {
            status.history.pop_back();
        }
    }
}
