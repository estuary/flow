use crate::local_specs;
use anyhow::Context;
use proto_flow::{capture, flow};

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
    /// Emit the raw discover output so it can be used in snapshot tests introspected during development
    /// Rather than updating the filesystem with the discovered specs
    #[clap(long)]
    emit_raw: bool,
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
        emit_raw,
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

    let model = capture.model.as_mut().expect("not a deletion");
    let mut model_clone = model.clone();

    // Inline a clone of the capture model for use with the discover RPC.
    sources::inline_capture(
        &capture.scope,
        &mut model_clone,
        &mut sources.imports,
        &sources.resources,
    );

    let discover = match &model_clone.endpoint {
        models::CaptureEndpoint::Connector(config) => capture::request::Discover {
            connector_type: flow::capture_spec::ConnectorType::Image as i32,
            config_json: serde_json::to_string(&config).unwrap(),
        },
        models::CaptureEndpoint::Local(config) => capture::request::Discover {
            connector_type: flow::capture_spec::ConnectorType::Local as i32,
            config_json: serde_json::to_string(config).unwrap(),
        },
    };
    let discover = capture::Request {
        discover: Some(discover),
        ..Default::default()
    }
    .with_internal(|internal| {
        if let Some(s) = &model_clone.shards.log_level {
            internal.set_log_level(ops::LogLevel::from_str_name(s).unwrap_or_default());
        }
    });

    let capture::response::Discovered { bindings } = runtime::Runtime::new(
        true, // All local.
        network.clone(),
        ops::tracing_log_handler,
        None,
        format!("discover/{}", capture.capture),
    )
    .unary_capture(discover)
    .await?
    .discovered
    .context("connector didn't send expected Discovered response")?;

    if *emit_raw {
        for binding in bindings {
            println!("{}", serde_json::to_string(&binding)?)
        }
        return Ok(());
    }
    // Modify the capture's bindings in-place.
    // TODO(johnny): Refactor and re-use discover deep-merge behavior from the agent.
    model.bindings.clear();

    let prefix = capture
        .capture
        .rsplit_once("/")
        .map(|(prefix, _)| prefix)
        .unwrap_or("acmeCo");

    // Create a catalog with the discovered bindings.
    let mut catalog = tables::DraftCatalog::default();
    let scope = url::Url::parse("flow://control").unwrap();

    for binding in bindings {
        let collection_name = format!("{prefix}/{}", binding.recommended_name);
        let collection = models::Collection::new(collection_name);

        model.bindings.push(models::CaptureBinding {
            target: collection.clone(),
            disable: false,
            resource: models::RawValue::from_string(binding.resource_config_json)?,
            backfill: 0,
        });

        catalog.collections.insert_row(
            collection,
            &scope,
            None,
            Some(models::CollectionDef {
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
                expect_pub_id: None,
                delete: false,
            }),
            false, // !is_touch
        );
    }

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
