use super::template_names;
use anyhow::Context;

pub fn register_templates<'a>(registry: &mut handlebars::Handlebars<'a>) -> anyhow::Result<()> {
    // Only resolved state templates - this alert only sends when payment method is added
    let (resolved_subject, resolved_body) =
        template_names(models::status::AlertType::MissingPaymentMethod, true);
    registry
        .register_template_string(
            &resolved_subject,
            r#"Estuary: Thanks for Adding a Payment Method🎉"#,
        )
        .context("registering missing_payment_method-resolved-subject template")?;

    registry
        .register_template_string(
            &resolved_body,
            r#"<p class="body-text">We hope you are enjoying Estuary Flow. We have received your payment method for your account <span class="identifier">{{arguments.tenant}}</span>. {{#if (eq arguments.plan_state "free_trial")}}After your free trial ends on <strong>{{arguments.trial_end}}</strong>, you will automatically be switched the paid tier.{{else}}You are now on the paid tier.{{/if}}</p>
<a href="https://dashboard.estuary.dev/admin/billing" class="button">📈 See your bill</a>

<hr style="border: 0; border-top: 1px dashed lightgrey; margin: 40px 0 10px 0;">
<p style="text-align: center; font-weight: bold; font-size: 22px; margin: 10px 0 20px 0;">Frequently Asked Questions</p>
<hr style="border: 0; border-top: 1px dashed lightgrey; margin: 0 0 20px 0;">

<div style="margin-top: 15px;">
    <p style="font-weight: bold; font-size: 19px; margin-bottom: 5px;">Where is my data stored?</p>
    <div style="border-left: 4px solid lightgrey; padding-left: 12px; margin-bottom: 15px;">
        <p class="body-text" style="margin-bottom: 0;">By default, all collection data is stored in an Estuary-owned cloud storage bucket with a 30 day retention policy. Now that you have a paid account, you can update this to store data in your own cloud storage bucket. We support GCS, S3, and Azure Blob storage.</p>
    </div>
</div>

<div style="margin-top: 15px;">
    <p style="font-weight: bold; font-size: 19px; margin-bottom: 5px;">How can I access Estuary Support?</p>
    <div style="border-left: 4px solid lightgrey; padding-left: 12px; margin-bottom: 15px;">
        <p class="body-text" style="margin-bottom: 0;">Reach out to support@estuary.dev or join our slack.</p>
    </div>
</div>

<div style="margin-top: 15px;">
    <p style="font-weight: bold; font-size: 19px; margin-bottom: 5px;">Is it possible to schedule data flows?</p>
    <div style="border-left: 4px solid lightgrey; padding-left: 12px; margin-bottom: 15px;">
        <p class="body-text" style="margin-bottom: 0;">Estuary moves most data in real-time by default, without the need for scheduling, but you can add "update delays" to data warehouses to enable more downtime on your warehouse for cost savings. This can be enabled under "advanced settings" and default settings are 30 minutes for a warehouse.</p>
    </div>
</div>"#,
        )
        .context("registering missing_payment_method-resolved-body template")?;

    Ok(())
}
