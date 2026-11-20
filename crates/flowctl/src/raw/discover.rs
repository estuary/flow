use crate::{local_connector, local_specs};
use anyhow::Context;
use proto_flow::{capture, connector};

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
}

/// Discover a capture of local sources, and merge discovered bindings into
/// its local model as the control plane would (see the `discover` crate).
///
/// Local sources are the merge's draft, and target collections resolve
/// against live specs. The capture and collections of local sources are
/// written back, along with live collections which the discover changed.
pub async fn do_discover(
    ctx: &mut crate::CliContext,
    Discover {
        source,
        capture,
        flat,
        emit_raw,
    }: &Discover,
) -> anyhow::Result<()> {
    let source = build::arg_source_to_url(source, false)?;

    // The merge draft is an inline copy of the local sources.
    let mut draft = local_specs::surface_errors(local_specs::load(&source).await.into_result())?;
    ::sources::inline_draft_catalog(&mut draft);

    // Identify the capture to discover.
    let needle = if let Some(needle) = capture {
        needle.as_str()
    } else if draft.captures.len() == 1 {
        draft.captures.first().unwrap().capture.as_str()
    } else if draft.captures.is_empty() {
        anyhow::bail!("sourced specification files do not contain any captures");
    } else {
        anyhow::bail!(
            "sourced specification files contain multiple captures. Use --capture to identify a specific one"
        );
    };
    let capture_name = match draft.captures.get_by_key(&models::Capture::new(needle)) {
        Some(row) => row.capture.clone(),
        None => anyhow::bail!("could not find the capture {needle}"),
    };
    // Discovered collections are named under the capture's prefix.
    if !capture_name.contains('/') {
        anyhow::bail!(
            "capture {capture_name} must have a catalog prefix, such as acmeCo/{capture_name}"
        );
    }
    let local_collections: std::collections::BTreeSet<models::Collection> = draft
        .collections
        .iter()
        .map(|row| row.collection.clone())
        .collect();

    let capture_model = draft
        .captures
        .get_mut_by_key(&capture_name)
        .unwrap()
        .model
        .as_mut()
        .context("the capture to be discovered is marked for deletion")?;

    let capture::request::Spec {
        connector_type,
        config_json,
    } = validation::capture_spec_request(&capture_model.endpoint, &capture_model.shards);

    let discover = capture::request::Discover {
        name: capture_name.to_string(),
        connector_type,
        config_json,
        created_at: String::new(),
        secrets: assemble::secrets(&capture_model.secrets),
    };

    let router = ctx.local_connector_router();
    let (started, response) = local_connector::unary(
        &*router,
        capture_model.shards.log_level.as_deref(),
        connector::request::Kind::Capture(capture::Request {
            kind: Some(capture::request::Kind::Discover(Box::new(discover))),
            ..Default::default()
        }),
    )
    .await?;

    let Some(capture::Response {
        kind: Some(capture::response::Kind::Discovered(discovered)),
        ..
    }) = proto_grpc::connector::unwrap_capture(response)
    else {
        anyhow::bail!("connector didn't send expected Discovered response");
    };

    if *emit_raw {
        for binding in discover::inline_bindings(discovered)? {
            println!("{}", serde_json::to_string(&binding)?)
        }
        return Ok(());
    }

    let resource_path_pointers = match started.spec {
        Some(connector::response::started::Spec::Capture(spec)) => spec
            .resource_path_pointers
            .iter()
            .map(|ptr| json::Pointer::from_str(ptr))
            .collect(),
        _ => Vec::new(),
    };

    let resolver = local_specs::Resolver {
        rest: ctx.rest.clone(),
        access_token: ctx.access_token(),
    };
    let live = resolve_live(&resolver, vec![capture_name.as_str()]).await?;
    let options = discover::Options::for_capture(
        capture_model,
        live.captures.get_by_key(&capture_name).is_some(),
    );

    let merged = discover::merge_bindings(
        &capture_name,
        capture_model,
        discovered,
        &options,
        &resource_path_pointers,
    )?;

    let live = resolve_live(
        &resolver,
        merged.targets().into_iter().map(|t| t.as_str()).collect(),
    )
    .await?;
    discover::merge_collections(&merged, &mut draft.collections, &live.collections, &options)?;

    // Collections created by the discover are written without an
    // `expectPubId`, as are other local specs.
    for row in draft.collections.iter_mut() {
        if row.expect_pub_id == Some(models::Id::zero())
            && !local_collections.contains(&row.collection)
        {
            row.expect_pub_id = None;
        }
    }

    // Merged specs are written over the local sources as they're laid out
    // on disk, which the inlined draft no longer reflects.
    let mut sources = local_specs::surface_errors(local_specs::load(&source).await.into_result())?;

    let tables::DraftCatalog {
        mut captures,
        collections,
        ..
    } = draft;
    captures.retain(|row| row.capture == capture_name);
    let count = local_specs::extend_from_catalog(
        &mut sources,
        tables::DraftCatalog {
            captures,
            collections,
            ..Default::default()
        },
        // Overwrite, so that merged specs replace their local sources.
        local_specs::pick_policy(true, *flat),
    );

    local_specs::indirect_and_write_resources(sources)?;
    println!("Wrote {count} specifications under {source}.");

    Ok(())
}

async fn resolve_live(
    resolver: &local_specs::Resolver,
    names: Vec<&str>,
) -> anyhow::Result<tables::LiveCatalog> {
    let mut live = resolver.resolve(names).await;
    if let Some(err) = std::mem::take(&mut live.errors).into_iter().next() {
        return Err(err.error.context("resolving live specs"));
    }
    Ok(live)
}
