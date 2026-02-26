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
    Your Estuary {{arguments.spec_type}} <span class="identifier">{{catalog_name}}</span> has not started successfully {{#if arguments.last_primary_ts}}since {{arguments.last_primary_ts}}{{else}}since it was created{{/if}}. We have attempted to restart it {{arguments.count}} time(s) without success.
</p>
<p class="body-text">
    If the task is still important to you, please check its configuration and logs for issues preventing it from running successfully. Otherwise, it will be disabled automatically on {{arguments.disable_at}}.
</p>
<ul>
    <li><a href="{{> spec_dashboard_overview_url}}" target="_blank" rel="noopener">View the task status and logs</a></li>
    <li>If you need help, reach out to our team via Slack (#support) or reply to this email.</li>
</ul>"#,
        )
        .context("registering task_abandoned-fired-body template")?;

    // No resolved templates: resolution (via recovery or disable) completes
    // silently without sending an email.

    Ok(())
}
