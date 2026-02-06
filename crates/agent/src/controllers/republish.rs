use control_plane_api::{controllers::Message, publications::PublicationResult};
use models::status::{
    AlertType, Alerts,
    publications::{PublicationStatus, RepublishRequested},
};

use super::{
    ControlPlane, ControllerState, Inbox, alerts,
    publication_status::{self, PendingPublication},
};

pub async fn update_republish<C: ControlPlane>(
    events: &Inbox,
    state: &ControllerState,
    pub_status: &mut PublicationStatus,
    alerts_status: &mut Alerts,
    control_plane: &C,
) -> anyhow::Result<Option<PublicationResult>> {
    if let Some(reason) = was_republish_requested(events) {
        let received_at = control_plane.current_time();
        tracing::info!(%reason, %received_at, "received request to re-publish spec");
        pub_status.pending_republish = Some(RepublishRequested {
            received_at,
            reason,
            last_build_id: state.last_build_id,
        });
    } else {
        // We didn't just receive a Republish request, but we might have previously.
        // If so, check to see if we've since completed a publication
        if let Some(prev_req) = pub_status
            .pending_republish
            .take_if(|p| p.last_build_id < state.last_build_id)
        {
            tracing::info!(
                ?prev_req,
                %state.live_spec_updated_at,
                "re-publication was previously requested, but a publication has been completed since"
            );
            alerts::resolve_alert(alerts_status, AlertType::BackgroundPublicationFailed);
            return Ok(None);
        }
    }

    let Some(RepublishRequested {
        received_at,
        reason,
        last_build_id,
    }) = pub_status.pending_republish.clone()
    else {
        return Ok(None);
    };

    // A publication is required, so let's make sure that now is a good time
    publication_status::check_can_publish(pub_status, control_plane)?;

    let mut pending = PendingPublication::new();
    pending.start_touch(state, format!("re-publication requested: {reason}"));
    let publication = pending
        .finish(state, pub_status, Some(alerts_status), control_plane)
        .await?
        .error_for_status()?;

    tracing::info!(%reason, %received_at, %last_build_id, "requested re-publication was completed successfully");
    pub_status.pending_republish.take();

    Ok(Some(publication))
}

fn was_republish_requested(inbox: &Inbox) -> Option<String> {
    inbox
        .iter()
        .flat_map(|(_, m)| {
            if let Some(Message::Republish { reason }) = m {
                Some(reason.clone())
            } else {
                None
            }
        })
        .next()
}
