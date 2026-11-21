use super::template_names;
use anyhow::Context;

pub fn register_templates<'a>(registry: &mut handlebars::Handlebars<'a>) -> anyhow::Result<()> {
    registry.register_helper("usd", Box::new(usd));

    // Only fired state templates: the alert resolves without an email.
    let (fired_subject, fired_body) =
        template_names(models::status::AlertType::BillingThresholdExceeded, false);
    registry
        .register_template_string(
            &fired_subject,
            r#"Estuary: Month-to-date charges for {{arguments.tenant}} reached {{usd arguments.threshold_usd_cents}}"#,
        )
        .context("registering billing_threshold_exceeded-fired-subject template")?;

    registry
        .register_template_string(
            &fired_body,
            r#"<p class="body-text">Month-to-date charges for <span class="identifier">{{arguments.tenant}}</span> in {{arguments.month}} are <strong>{{usd arguments.charges_usd_cents}}</strong>, which is at or above its billing alert threshold of <strong>{{usd arguments.threshold_usd_cents}}</strong>.</p>
<p class="body-text">This amount covers charges so far, and the final invoice for {{arguments.month}} may differ.</p>
<a href="{{> dashboard_billing_url}}" class="button">📈 See your bill</a>"#,
        )
        .context("registering billing_threshold_exceeded-fired-body template")?;

    Ok(())
}

// Formats an amount of US cents as dollars, like `$1,234.56`.
handlebars::handlebars_helper!(usd: |cents: u64| {
    use num_format::ToFormattedString;
    format!(
        "${}.{:02}",
        (cents / 100).to_formatted_string(&num_format::Locale::en),
        cents % 100
    )
});
