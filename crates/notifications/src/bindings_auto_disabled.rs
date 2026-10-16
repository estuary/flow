use super::template_names;
use anyhow::Context;

pub fn register_templates<'a>(registry: &mut handlebars::Handlebars<'a>) -> anyhow::Result<()> {
    // Only fired templates: the alert resolves when the user re-enables or
    // removes the bindings, which needs no notification.
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
    Estuary automatically disabled bindings of your {{arguments.spec_type}} <span class="identifier">{{catalog_name}}</span>. Disabled bindings don't update their destination resources.
</p>
<ul>
{{#each arguments.bindings}}
    <li>
        Collection <span class="identifier">{{this.collection}}</span>, resource <span class="identifier">{{#each this.resource_path}}{{#unless @first}}.{{/unless}}{{this}}{{/each}}</span>:
        disabled because of {{#if (eq this.reason "incompatible_schema_change")}}an incompatible schema change{{else}}{{this.reason}}{{/if}} ({{this.detail}})
    </li>
{{/each}}
</ul>
<p class="body-text">
    To restore a binding:
</p>
<ul>
    <li>Backfill the capture which writes to its collection, so that the collection holds the complete history of your source. This is needed if the collection is in Estuary trial storage, which retains only recent data.</li>
    <li>Then re-enable the binding and increment its backfill counter, which rebuilds its destination resource from the collection. If you re-enable a binding without a backfill, the same incompatibility can disable it again.</li>
</ul>
<ul>
    <li><a href="{{> spec_dashboard_overview_url}}" target="_blank" rel="noopener">View the task status and bindings</a></li>
    <li>If you need help, reach out to our team via <a href="https://go.estuary.dev/slack">Slack</a> (#support) or reply to this email</li>
</ul>"#,
        )
        .context("registering bindings_auto_disabled-fired-body template")?;

    Ok(())
}
