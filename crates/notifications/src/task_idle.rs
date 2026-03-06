use super::template_names;
use anyhow::Context;

pub fn register_templates<'a>(registry: &mut handlebars::Handlebars<'a>) -> anyhow::Result<()> {
    let (fired_subject, fired_body) =
        template_names(models::status::AlertType::TaskIdle, false);
    registry
        .register_template_string(
            &fired_subject,
            r#"Estuary Flow: {{arguments.spec_type}} {{catalog_name}} has not moved data"#,
        )
        .context("registering task_idle-fired-subject template")?;

    registry
        .register_template_string(
            &fired_body,
            r#"<p class="body-text">
    Your Estuary {{arguments.spec_type}} <span class="identifier">{{catalog_name}}</span> has not moved data in over 30 days. If this task is no longer needed, you can disable or delete it. Otherwise, it will be disabled automatically on {{arguments.disable_at}}.
</p>
<ul>
    <li><a href="{{> spec_dashboard_overview_url}}" target="_blank" rel="noopener">View the task status and logs</a></li>
    <li>If you need help, reach out to our team via Slack (#support) or reply to this email.</li>
</ul>"#,
        )
        .context("registering task_idle-fired-body template")?;

    // No resolved templates: resolution (via recovery or disable) completes
    // silently without sending an email.

    Ok(())
}
