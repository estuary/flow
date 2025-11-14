use super::template_names;
use anyhow::Context;

pub fn register_templates<'a>(registry: &mut handlebars::Handlebars<'a>) -> anyhow::Result<()> {
    // Only fired state templates - this alert doesn't send on resolution
    let (fired_subject, fired_body) =
        template_names(models::status::AlertType::FreeTrialEnding, false);
    registry
        .register_template_string(&fired_subject, r#"Estuary Flow: Paid Tier"#)
        .context("registering free_trial_ending-fired-subject template")?;

    registry
        .register_template_string(
            &fired_body,
            r#"{{#if arguments.has_credit_card}}
<p class="body-text">Just so you know, your free trial for <span class="identifier">{{arguments.tenant}}</span> will be ending on <strong>{{arguments.trial_end}}</strong>. Since you have already added a payment method, no action is required.</p>
<a href="{{> dashboard_billing_url}}" class="button">📈 View your stats</a>
{{else}}
<p class="body-text">Your free trial for <span class="identifier">{{arguments.tenant}}</span> is ending on <strong>{{arguments.trial_end}}</strong>, at which point your account will begin accruing usage. Please enter a payment method in order to continue using the platform after your trial ends. We'd be sad to see you go!</p>
<a href="{{> dashboard_billing_url}}" class="button">Add payment information</a>
{{/if}}"#,
        )
        .context("registering free_trial_ending-fired-body template")?;

    Ok(())
}
