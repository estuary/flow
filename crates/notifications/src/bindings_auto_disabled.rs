use super::template_names;
use anyhow::Context;

pub fn register_templates<'a>(registry: &mut handlebars::Handlebars<'a>) -> anyhow::Result<()> {
    let (fired_subject, fired_body) =
        template_names(models::status::AlertType::BindingsAutoDisabled, false);
    registry
        .register_template_string(
            &fired_subject,
            r#"Estuary: bindings of {{arguments.spec_type}} {{catalog_name}} have been automatically disabled"#,
        )
        .context("registering bindings_auto_disabled-fired-subject template")?;

    registry
        .register_template_string(
            &fired_body,
            r#"<p class="body-text">
    Bindings of your Estuary {{arguments.spec_type}} <span class="identifier">{{catalog_name}}</span> have been automatically disabled. Disabled bindings don't update their destination resources.
</p>
<ul>
{{#each arguments.bindings}}
    <li>
        Collection <span class="identifier">{{this.collection}}</span>, resource <span class="identifier">{{#each this.resource_path}}{{#unless @first}}.{{/unless}}{{this}}{{/each}}</span>:
        <div style="background-color: #f4f4f4; color: #000000; border-left: 3px solid #dadada; margin: 8px 0 16px; padding: 12px;">
            <strong>Disabled because of {{#if (eq this.reason "incompatible_schema_change")}}an incompatible schema change{{else}}{{this.reason}}{{/if}}</strong>
            <pre style="font-family: monospace; font-size: 14px; line-height: 1.5; margin: 8px 0 0; white-space: pre-wrap; overflow-wrap: anywhere; word-break: break-word;">{{this.detail}}</pre>
        </div>
    </li>
{{/each}}
</ul>
<p class="body-text">
    We try our best to handle schema changes for you, but we weren't able to this time. In order to restore a disabled binding while keeping the incompatible change, you must backfill it.
</p>
<p class="body-text">
    Estuary's free trial storage <a href="https://docs.estuary.dev/getting-started/installation/#migrating-existing-data">expires after 20 days</a>, so beware that if you haven't set up your own cloud storage, backfilling just the disabled bindings will only materialize data captured within the past 20 days. You can also perform a <a href="https://docs.estuary.dev/reference/backfilling-data/#dataflow-reset">dataflow reset</a> which will re-capture from the source.
</p>
<p class="body-text">
    For more information about handling schema changes, see our <a href="https://docs.estuary.dev/guides/schema-evolution/">schema evolution guide</a>.
</p>
<p class="body-text">
    If you need help, you can <a href="{{> spec_dashboard_overview_url}}" target="_blank" rel="noopener">view the task status and bindings</a>, reach out to our team via <a href="https://go.estuary.dev/slack">Slack</a> (#support), or reply to this email.
</p>"#,
        )
        .context("registering bindings_auto_disabled-fired-body template")?;

    Ok(())
}
