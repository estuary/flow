use super::template_names;
use anyhow::Context;

pub fn register_templates<'a>(registry: &mut handlebars::Handlebars<'a>) -> anyhow::Result<()> {
    let (fired_subject, fired_body) = template_names(models::status::AlertType::FreeTrial, false);
    registry
        .register_template_string(&fired_subject, r#"Estuary Free Trial"#)
        .context("registering free_trial-fired-subject template")?;

    registry
        .register_template_string(
            &fired_body,
            r#"{{#if arguments.has_credit_card}}
<p class="body-text">Your Estuary account <span class="identifier">{{arguments.tenant}}</span> has started its 30-day free trial. This trial will end on <strong>{{arguments.trial_end}}</strong>. Billing will begin accruing then.</p>
{{else}}
<p class="body-text">We hope you're enjoying Estuary Flow. Our free tier includes 10 GB/month and 2 connectors. Your account <span class="identifier">{{arguments.tenant}}</span> has now exceeded that, so it has been transitioned to a 30 day free trial ending on <strong>{{arguments.trial_end}}</strong>.</p>
<p class="body-text">Please add payment information in the next 30 days to continue using the platform. If you have any questions, feel free to reach out to <a href="mailto:support@estuary.dev">support@estuary.dev</a></p>
<a href="{{> dashboard_billing_url}}" class="button">Add payment information</a>
{{/if}}"#,
        )
        .context("registering free_trial-fired-body template")?;

    // Resolved state templates
    let (resolved_subject, resolved_body) =
        template_names(models::status::AlertType::FreeTrial, true);
    registry
        .register_template_string(
            &resolved_subject,
            r#"{{#if arguments.has_credit_card}}Estuary Flow: Paid Tier{{else}}Estuary Paid Tier: Enter Payment Info to Continue Access{{/if}}"#,
        )
        .context("registering free_trial-resolved-subject template")?;

    registry
        .register_template_string(
            &resolved_body,
            r#"{{#if arguments.has_credit_card}}
<p class="body-text">We hope you are enjoying Estuary Flow. Your free trial for account <span class="identifier">{{arguments.tenant}}</span> is officially over.</p>
<p class="body-text">Since you have already added a payment method, no action is required. If you have any questions, feel free to reach out to <a href="mailto:support@estuary.dev">support@estuary.dev</a> anytime!</p>
<a href="https://dashboard.estuary.dev" class="button">🌊 View your data flows</a>
{{else}}
<p class="body-text">We hope you are enjoying Estuary Flow. Your free trial for account <span class="identifier">{{arguments.tenant}}</span> is officially over.</p>
<p class="body-text">Please add payment information immediately to continue using the platform. If you have any questions, feel free to reach out to <a href="mailto:support@estuary.dev">support@estuary.dev</a></p>
<a href="{{> dashboard_billing_url}}" class="button">Add payment information</a>
{{/if}}"#,
        )
        .context("registering free_trial-resolved-body template")?;

    Ok(())
}
