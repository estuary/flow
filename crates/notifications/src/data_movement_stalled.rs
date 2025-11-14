use super::template_names;
use anyhow::Context;

pub fn register_templates<'a>(registry: &mut handlebars::Handlebars<'a>) -> anyhow::Result<()> {
    // Fired state templates
    let (fired_subject, fired_body) =
        template_names(models::status::AlertType::DataMovementStalled, false);
    registry
        .register_template_string(
            &fired_subject,
            r#"Estuary Flow: Alert for {{arguments.spec_type}} {{catalog_name}}"#,
        )
        .context("registering data_movement_stalled-fired-subject template")?;

    registry
        .register_template_string(
            &fired_body,
            r#"<p class="body-text">
    You are receiving this alert because your task, {{arguments.spec_type}} <span class="identifier">{{catalog_name}}</span> hasn't seen new data in {{arguments.evaluation_interval}}. You can locate your task <a href="{{> spec_dashboard_overview_url}}" target="_blank" rel="noopener">here</a> to make changes or update its alerting settings.
</p>"#,
        )
        .context("registering data_movement_stalled-fired-body template")?;

    // Resolved state templates
    let (resolved_subject, resolved_body) =
        template_names(models::status::AlertType::DataMovementStalled, true);
    registry
        .register_template_string(
            &resolved_subject,
            r#"Estuary Flow: Alert resolved for {{arguments.spec_type}} {{catalog_name}}"#,
        )
        .context("registering data_movement_stalled-resolved-subject template")?;

    registry
        .register_template_string(
            &resolved_body,
            r#"<p class="body-text">
    You are receiving this notice because a previous alert for your task, {{arguments.spec_type}} <span class="identifier">{{catalog_name}}</span>, has now resolved. You can locate your task <a href="{{> spec_dashboard_overview_url}}" target="_blank" rel="noopener">here</a> to make changes or update its alerting settings.
</p>"#,
        )
        .context("registering data_movement_stalled-resolved-body template")?;

    Ok(())
}
