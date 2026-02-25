use super::template_names;
use anyhow::Context;

pub fn register_templates<'a>(registry: &mut handlebars::Handlebars<'a>) -> anyhow::Result<()> {
    let (fired_subject, fired_body) =
        template_names(models::status::AlertType::TaskAbandoned, false);
    registry
        .register_template_string(
            &fired_subject,
            r#"Estuary Flow: {{arguments.spec_type}} {{catalog_name}} appears abandoned"#,
        )
        .context("registering task_abandoned-fired-subject template")?;

    registry
        .register_template_string(
            &fired_body,
            r#"<p class="body-text">
    Your Estuary {{arguments.spec_type}} <span class="identifier">{{catalog_name}}</span> has not had healthy shard activity for an extended period while remaining enabled.
</p>
<p class="body-text">
    This may indicate the task is no longer needed. If the task is still required, please check its configuration and logs for issues preventing it from running successfully.
</p>
<ul>
    <li><a href="{{> spec_dashboard_overview_url}}" target="_blank" rel="noopener">View the task status and logs</a></li>
    <li>If you no longer need this task, consider disabling it to free up resources.</li>
    <li>If you need help, reach out to our team via Slack (#support) or reply to this email.</li>
</ul>"#,
        )
        .context("registering task_abandoned-fired-body template")?;

    // Resolved state templates
    let (resolved_subject, resolved_body) =
        template_names(models::status::AlertType::TaskAbandoned, true);
    registry
        .register_template_string(
            &resolved_subject,
            r#"Estuary Flow: {{arguments.spec_type}} {{catalog_name}} is no longer abandoned"#,
        )
        .context("registering task_abandoned-resolved-subject template")?;

    registry
        .register_template_string(
            &resolved_body,
            r#"<p class="body-text">
    Your Estuary {{arguments.spec_type}} <span class="identifier">{{catalog_name}}</span> has resumed healthy operation and is no longer flagged as abandoned.
</p>
<p class="body-text">
    You can <a href="{{> spec_dashboard_overview_url}}" target="_blank" rel="noopener">view your task</a> to confirm everything is working as expected.
</p>"#,
        )
        .context("registering task_abandoned-resolved-body template")?;

    Ok(())
}
