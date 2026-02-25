use super::template_names;
use anyhow::Context;

pub fn register_templates<'a>(registry: &mut handlebars::Handlebars<'a>) -> anyhow::Result<()> {
    // Only fired templates: this is a one-shot notification. The alert resolves
    // immediately after firing, and no resolution email is sent.
    let (fired_subject, fired_body) =
        template_names(models::status::AlertType::TaskAutoDisabledIdle, false);
    registry
        .register_template_string(
            &fired_subject,
            r#"Estuary Flow: {{arguments.spec_type}} {{catalog_name}} has been automatically disabled"#,
        )
        .context("registering task_auto_disabled_idle-fired-subject template")?;

    registry
        .register_template_string(
            &fired_body,
            r#"<p class="body-text">
    Your Estuary {{arguments.spec_type}} <span class="identifier">{{catalog_name}}</span> has been automatically disabled because it has not moved data. If you still need this task, re-enable it and verify that data is flowing. Otherwise, no further action is needed.
</p>
<ul>
    <li><a href="{{> spec_dashboard_overview_url}}" target="_blank" rel="noopener">View the task status and logs</a></li>
    <li>If you need help, reach out to our team via Slack (#support) or reply to this email.</li>
</ul>"#,
        )
        .context("registering task_auto_disabled_idle-fired-body template")?;

    Ok(())
}
