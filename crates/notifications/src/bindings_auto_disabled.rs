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
    We try our best to handle schema changes automatically. In this case(s), we weren't able to. In order to restore a disabled binding, you must backfill it.
</p>
<p class="body-text">
    Beware that if the collection being materialized is stored in Estuary's trial storage bucket, data older than 20 days is not available. You can either proceed with that limitation and backfill the materialization binding, or you can re-backfill from the source by performing a dataflow reset, which will re-capture all of the data from the source.
</p>
<p class="body-text">
    If you need help, you can <a href="{{> spec_dashboard_overview_url}}" target="_blank" rel="noopener">view the task status and bindings</a>, reach out to our team via <a href="https://go.estuary.dev/slack">Slack</a> (#support), or reply to this email.
</p>"#,
        )
        .context("registering bindings_auto_disabled-fired-body template")?;

    Ok(())
}
