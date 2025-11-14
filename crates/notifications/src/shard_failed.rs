use super::template_names;
use anyhow::Context;

pub fn register_templates<'a>(registry: &mut handlebars::Handlebars<'a>) -> anyhow::Result<()> {
    let (fired_subject, fired_body) = template_names(models::status::AlertType::ShardFailed, false);
    registry
        .register_template_string(
            &fired_subject,
            r#"Estuary Flow: Task failure detected for {{arguments.spec_type}} {{catalog_name}}"#,
        )
        .context("registering shard_failed-fired-subject template")?;

    registry
        .register_template_string(
            &fired_body,
            r#"<p class="body-text">
    Your Estuary {{arguments.spec_type}} <span class="identifier">{{catalog_name}}</span> has a failure that is impacting your data pipeline. To troubleshoot please:
</p>
<ul>
    <li><a href="{{> spec_dashboard_overview_url}}" target="_blank" rel="noopener">Visit the task status and logs</a> for more information about the error</li>
    <li>If you need help please reach out to our team via Slack (#support and #ask-ai) or reply to this email.</li>
</ul>
<p class="body-text">
    We are here to help ensure your data pipelines run smoothly.
</p>"#,
        )
        .context("registering shard_failed-fired-body template")?;

    // Resolved state templates
    let (resolved_subject, resolved_body) =
        template_names(models::status::AlertType::ShardFailed, true);
    registry
        .register_template_string(
            &resolved_subject,
            r#"Estuary Flow: Task failure resolved for {{arguments.spec_type}} {{catalog_name}}"#,
        )
        .context("registering shard_failed-resolved-subject template")?;

    registry
        .register_template_string(
            &resolved_body,
            r#"<p class="body-text">
    Good news! The shard failure for your {{arguments.spec_type}} <span class="identifier">{{catalog_name}}</span> has been resolved.
</p>
<p class="body-text">
    You can <a href="{{> spec_dashboard_overview_url}}" target="_blank" rel="noopener">view your task</a> to confirm everything is working as expected, or update your alerting settings.
</p>
<p class="body-text">
    If you continue to experience issues, please don't hesitate to reach out to our support team.
</p>"#,
        )
        .context("registering shard_failed-resolved-body template")?;

    Ok(())
}
