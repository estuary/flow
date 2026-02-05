use super::template_names;
use anyhow::Context;

pub fn register_templates<'a>(registry: &mut handlebars::Handlebars<'a>) -> anyhow::Result<()> {
    let (fired_subject, fired_body) = template_names(
        models::status::AlertType::BackgroundPublicationFailed,
        false,
    );
    registry
        .register_template_string(
            &fired_subject,
            r#"Estuary Flow: Automated background publication failed for {{arguments.spec_type}} {{catalog_name}}"#,
        )
        .context("registering background_publication_failed-fired-subject template")?;

    registry
        .register_template_string(
            &fired_body,
            r#"<p class="body-text">
    Your Estuary {{arguments.spec_type}} <span class="identifier">{{catalog_name}}</span> has encountered errors during automated background publication. This means Estuary was unable to update your {{arguments.spec_type}}, and your {{arguments.spec_type}} will not function normally until the issue is resolved. To troubleshoot please:
</p>
<ul>
    <li><a href="{{> spec_dashboard_overview_url}}" target="_blank" rel="noopener">Visit the task status and logs</a> for more information about the error</li>
    <li>If you need help please reach out to our team via Slack (#support and #ask-ai) or reply to this email.</li>
</ul>
<p class="body-text">
    We are here to help ensure your data pipelines run smoothly.
</p>"#,
        )
        .context("registering background_publication_failed-fired-body template")?;

    let (resolved_subject, resolved_body) =
        template_names(models::status::AlertType::BackgroundPublicationFailed, true);
    registry
        .register_template_string(
            &resolved_subject,
            r#"Estuary Flow: Automated background publication alert resolved for {{arguments.spec_type}} {{catalog_name}}"#,
        )
        .context("registering background_publication_failed-resolved-subject template")?;

    registry
        .register_template_string(
            &resolved_body,
            r#"<p class="body-text">
    Good news! The background publication failed alert for your {{arguments.spec_type}} <span class="identifier">{{catalog_name}}</span> has been resolved.
</p>
<p class="body-text">
    You can <a href="{{> spec_dashboard_overview_url}}" target="_blank" rel="noopener">view your task</a> to confirm everything is working as expected, or update your alerting settings.
</p>
<p class="body-text">
    We are here to help ensure your data pipelines run smoothly.
</p>"#,
        )
        .context("registering background_publication_failed-resolved-body template")?;

    Ok(())
}
