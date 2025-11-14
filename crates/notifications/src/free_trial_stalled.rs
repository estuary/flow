use super::template_names;
use anyhow::Context;

pub fn register_templates<'a>(registry: &mut handlebars::Handlebars<'a>) -> anyhow::Result<()> {
    let (fired_subject, fired_body) =
        template_names(models::status::AlertType::FreeTrialStalled, false);
    registry
        .register_template_string(
            &fired_subject,
            r#"Free Tier Grace Period for {{arguments.tenant}}: No CC 💳❌"#,
        )
        .context("registering free_trial_stalled-fired-subject template")?;

    registry
        .register_template_string(
            &fired_body,
            r#"<p class="body-text" style="font-size: 20px; color: #512d0b;"><strong>Name:</strong> {{#if recipient.full_name}}{{recipient.full_name}}{{else}}Unknown{{/if}}</p>
<p class="body-text" style="font-size: 20px; color: #512d0b;"><strong>Email:</strong> {{recipient.email}}</p>
<p class="body-text" style="font-size: 20px; color: #512d0b;"><strong>Tenant:</strong> {{arguments.tenant}}</p>
<p class="body-text" style="font-size: 20px; color: #512d0b;"><strong>Trial Start:</strong> {{arguments.trial_start}}, <strong>Trial End:</strong> {{arguments.trial_end}}</p>
<p class="body-text" style="font-size: 20px; color: #512d0b;"><strong>Credit Card</strong>: ❌</p>"#,
        )
        .context("registering free_trial_stalled-fired-body template")?;

    let (resolved_subject, resolved_body) =
        template_names(models::status::AlertType::FreeTrialStalled, true);
    registry
        .register_template_string(
            &resolved_subject,
            r#"Free Tier Grace Period for {{arguments.tenant}}: CC Entered 💳✅"#,
        )
        .context("registering free_trial_stalled-resolved-subject template")?;

    registry
        .register_template_string(
            &resolved_body,
            r#"<p class="body-text" style="font-size: 20px; color: #512d0b;"><strong>Name:</strong> {{#if recipient.full_name}}{{recipient.full_name}}{{else}}Unknown{{/if}}</p>
<p class="body-text" style="font-size: 20px; color: #512d0b;"><strong>Email:</strong> {{recipient.email}}</p>
<p class="body-text" style="font-size: 20px; color: #512d0b;"><strong>Tenant:</strong> {{arguments.tenant}}</p>
<p class="body-text" style="font-size: 20px; color: #512d0b;"><strong>Trial Start:</strong> {{arguments.trial_start}}, <strong>Trial End:</strong> {{arguments.trial_end}}</p>
<p class="body-text" style="font-size: 20px; color: #512d0b;"><strong>Credit Card</strong>: ✅</p>"#,
        )
        .context("registering free_trial_stalled-resolved-body template")?;

    Ok(())
}
