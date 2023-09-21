use crate::local_specs;
use anyhow::Context;
use proto_flow::{capture, flow};
use std::collections::BTreeMap;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Discover {
    /// Path or URL to a Flow specification file.
    #[clap(long)]
    source: String,
    /// Name of the capture to discover within the Flow specification file.
    /// Capture is required if there are multiple captures in --source specifications.
    #[clap(long)]
    capture: Option<String>,
    /// Should specs be written to one specification file, instead of the canonical layout?
    #[clap(long)]
    flat: bool,
    /// Docker network to run the connector.
    #[clap(long, default_value = "bridge")]
    network: String,
}

pub async fn do_discover(
    _ctx: &mut crate::CliContext,
    Discover {
        source,
        capture,
        flat,
        network,
    }: &Discover,
) -> anyhow::Result<()> {
    let source = build::arg_source_to_url(source, false)?;
    let mut sources = local_specs::surface_errors(local_specs::load(&source).await.into_result())?;

    // Identify the capture to discover.
    let needle = if let Some(needle) = capture {
        needle.as_str()
    } else if sources.captures.len() == 1 {
        sources.captures.first().unwrap().capture.as_str()
    } else if sources.captures.is_empty() {
        anyhow::bail!("sourced specification files do not contain any captures");
    } else {
        anyhow::bail!("sourced specification files contain multiple captures. Use --capture to identify a specific one");
    };

    let capture = match sources
        .captures
        .binary_search_by_key(&needle, |c| c.capture.as_str())
    {
        Ok(index) => &mut sources.captures[index],
        Err(_) => anyhow::bail!("could not find the capture {needle}"),
    };

    // Inline a clone of the capture spec for use with the discover RPC.
    let mut spec_clone = capture.spec.clone();
    sources::inline_capture(&capture.scope, &mut spec_clone, &sources.resources);

    let discover = match &spec_clone.endpoint {
        models::CaptureEndpoint::Connector(config) => capture::request::Discover {
            connector_type: flow::capture_spec::ConnectorType::Image as i32,
            config_json: serde_json::to_string(&config).unwrap(),
        },
        models::CaptureEndpoint::Local(config) => capture::request::Discover {
            connector_type: flow::capture_spec::ConnectorType::Local as i32,
            config_json: serde_json::to_string(config).unwrap(),
        },
    };
    let mut discover = capture::Request {
        discover: Some(discover),
        ..Default::default()
    };

    if let Some(log_level) = capture
        .spec
        .shards
        .log_level
        .as_ref()
        .and_then(|s| ops::LogLevel::from_str_name(s))
    {
        discover.set_internal_log_level(log_level);
    }

    let capture::response::Discovered { bindings } = runtime::Runtime::new(
        true, // All local.
        network.clone(),
        ops::tracing_log_handler,
        None,
        format!("discover/{}", capture.capture),
    )
    .unary_capture(discover, build::CONNECTOR_TIMEOUT)
    .await
    .map_err(crate::status_to_anyhow)?
    .discovered
    .context("connector didn't send expected Discovered response")?;

    // Modify the capture's bindings in-place.
    // TODO(johnny): Refactor and re-use discover deep-merge behavior from the agent.
    capture.spec.bindings.clear();

    let prefix = capture
        .capture
        .rsplit_once("/")
        .map(|(prefix, _)| prefix)
        .unwrap_or("acmeCo");

    // Create a catalog with the discovered bindings
    let mut collections = BTreeMap::new();
    for binding in bindings {
        let collection_name = format!("{prefix}/{}", binding.recommended_name);
        let collection = models::Collection::new(collection_name);

        capture.spec.bindings.push(models::CaptureBinding {
            target: collection.clone(),
            disable: false,
            resource: models::RawValue::from_string(binding.resource_config_json)?,
        });

        collections.insert(
            collection,
            models::CollectionDef {
                schema: Some(models::Schema::new(models::RawValue::from_string(
                    binding.document_schema_json,
                )?)),
                write_schema: None,
                read_schema: None,
                key: models::CompositeKey::new(
                    binding
                        .key
                        .iter()
                        .map(models::JsonPointer::new)
                        .collect::<Vec<_>>(),
                ),
                derive: None,
                projections: Default::default(),
                journals: Default::default(),
            },
        );
    }

    let catalog = models::Catalog {
        collections,
        ..Default::default()
    };

    let count = local_specs::extend_from_catalog(
        &mut sources,
        catalog,
        // We need to overwrite here to allow for bindings to be added to the capture
        local_specs::pick_policy(true, *flat),
    );

    local_specs::indirect_and_write_resources(sources)?;
    println!("Wrote {count} specifications under {source}.");

    Ok(())
}
