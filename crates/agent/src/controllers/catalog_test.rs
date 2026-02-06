use std::collections::BTreeSet;

use crate::controllers::publication_status;

use super::{
    ControlPlane, ControllerState, Inbox, NextRun, dependencies::Dependencies, periodic, republish,
};
use models::status::catalog_test::TestStatus;

pub async fn update<C: ControlPlane>(
    status: &mut TestStatus,
    state: &ControllerState,
    events: &Inbox,
    control_plane: &C,
    _model: &models::TestDef,
) -> anyhow::Result<Option<NextRun>> {
    let mut dependencies = Dependencies::resolve(state, control_plane).await?;
    status.passing = false;
    if dependencies
        .update(
            state,
            control_plane,
            &mut status.publications,
            &mut status.alerts,
            error_on_deleted_dependencies,
        )
        .await?
        .is_some()
    {
        // We've successfully published against the latest versions of the dependencies
        status.passing = true;
        return Ok(Some(NextRun::immediately()));
    }

    let republish_result = republish::update_republish(
        events,
        state,
        &mut status.publications,
        &mut status.alerts,
        control_plane,
    )
    .await?;
    if republish_result.is_some() {
        return Ok(Some(NextRun::immediately()));
    }

    if periodic::update_periodic_publish(
        state,
        &mut status.publications,
        &mut status.alerts,
        control_plane,
    )
    .await?
    .is_some()
    {
        // We've successfully published against the latest versions of the dependencies
        status.passing = true;
        return Ok(Some(NextRun::immediately()));
    }

    // If dependencies are up to date, then the test is passing.
    status.passing = true;

    // Clear any background_publication_failed alert if the test has been successfully published by the user
    publication_status::update_observed_pub_id(
        &mut status.publications,
        &mut status.alerts,
        state,
        control_plane,
    )
    .await?;

    Ok(periodic::next_periodic_publish(state))
}

fn error_on_deleted_dependencies(
    deleted: &BTreeSet<String>,
) -> anyhow::Result<(String, models::TestDef)> {
    // This will be considered a retryable because technically the
    // collection could spring back into existence.
    anyhow::bail!(
        "test failed because {} of the collection(s) it depends on have been deleted",
        deleted.len()
    )
}
