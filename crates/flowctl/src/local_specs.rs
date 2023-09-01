use anyhow::Context;
use futures::{future::BoxFuture, FutureExt};
use std::collections::BTreeMap;

/// Load and validate sources and derivation connectors (only).
/// Capture and materialization connectors are not validated.
pub(crate) async fn load_and_validate(
    client: crate::controlplane::Client,
    source: &str,
) -> anyhow::Result<(tables::Sources, tables::Validations)> {
    let source = build::arg_source_to_url(source, false)?;
    let sources = surface_errors(load(&source).await.into_result())?;
    let (sources, validations) = validate(client, true, false, true, sources).await;
    Ok((sources, surface_errors(validations.into_result())?))
}

/// Load and validate sources and all connectors.
pub(crate) async fn load_and_validate_full(
    client: crate::controlplane::Client,
    source: &str,
) -> anyhow::Result<(tables::Sources, tables::Validations)> {
    let source = build::arg_source_to_url(source, false)?;
    let sources = surface_errors(load(&source).await.into_result())?;
    let (sources, validations) = validate(client, false, false, false, sources).await;
    Ok((sources, surface_errors(validations.into_result())?))
}

/// Generate connector files by validating sources with derivation connectors.
pub(crate) async fn generate_files(
    client: crate::controlplane::Client,
    sources: tables::Sources,
) -> anyhow::Result<()> {
    let source = &sources.fetches[0].resource.clone();
    let project_root = build::project_root(source);

    let (mut sources, validations) = validate(client, true, false, true, sources).await;

    build::generate_files(&project_root, &validations)?;

    sources.errors = sources
        .errors
        .into_iter()
        .filter_map(|tables::Error { scope, error }| {
            match error.downcast_ref() {
                // Skip load errors about missing resources. That's the point!
                Some(sources::LoadError::Fetch { .. }) => None,
                _ => Some(tables::Error { scope, error }),
            }
        })
        .collect();

    if let Err(errors) = sources
        .into_result()
        .and_then(|_| validations.into_result())
    {
        for tables::Error { scope, error } in errors.iter() {
            tracing::error!(%scope, ?error);
        }
        tracing::error!(
            "I may not have generated all files because the Flow specifications have errors.",
        );
    }

    Ok(())
}

pub(crate) async fn load(source: &url::Url) -> tables::Sources {
    // We never use a file root jail when loading on a user's machine.
    build::load(source, std::path::Path::new("/")).await
}

async fn validate(
    client: crate::controlplane::Client,
    noop_captures: bool,
    noop_derivations: bool,
    noop_materializations: bool,
    sources: tables::Sources,
) -> (tables::Sources, tables::Validations) {
    let source = &sources.fetches[0].resource.clone();
    let project_root = build::project_root(source);

    let (sources, mut validate) = build::validate(
        "local-build",
        "", // Use default connector network.
        &Resolver { client },
        false, // Don't generate ops collections.
        ops::tracing_log_handler,
        noop_captures,
        noop_derivations,
        noop_materializations,
        &project_root,
        sources,
    )
    .await;

    // Local specs are not expected to satisfy all referential integrity checks.
    // Filter out errors which are not really "errors" for the Flow CLI.
    validate.errors = validate
        .errors
        .into_iter()
        .filter(|err| match err.error.downcast_ref() {
            // Ok if *no* storage mappings are defined.
            // If at least one mapping is defined, then we do require that all
            // collections have appropriate mappings.
            Some(validation::Error::NoStorageMappings { .. }) => false,
            // All other validation errors bubble up as top-level errors.
            _ => true,
        })
        .collect::<tables::Errors>();

    (sources, validate)
}

pub(crate) fn surface_errors<T>(result: Result<T, tables::Errors>) -> anyhow::Result<T> {
    match result {
        Err(errors) => {
            for tables::Error { scope, error } in errors.iter() {
                tracing::error!(%scope, ?error);
            }
            Err(anyhow::anyhow!("failed due to encountered errors"))
        }
        Ok(ok) => return Ok(ok),
    }
}

// Indirect specifications so that larger configurations, etc become reference
// resources, then write them out if they're under the project root.
pub(crate) fn indirect_and_write_resources(
    mut sources: tables::Sources,
) -> anyhow::Result<tables::Sources> {
    ::sources::indirect_large_files(&mut sources, 1 << 9);
    write_resources(sources)
}

pub(crate) fn write_resources(mut sources: tables::Sources) -> anyhow::Result<tables::Sources> {
    let source = &sources.fetches[0].resource.clone();
    let project_root = build::project_root(source);
    ::sources::rebuild_catalog_resources(&mut sources);

    build::write_files(
        &project_root,
        sources
            .resources
            .iter()
            .map(
                |tables::Resource {
                     resource, content, ..
                 }| (resource.clone(), content.to_vec()),
            )
            .collect(),
    )?;

    Ok(sources)
}

pub(crate) fn into_catalog(sources: tables::Sources) -> models::Catalog {
    let tables::Sources {
        captures,
        collections,
        fetches: _,
        imports: _,
        materializations,
        resources: _,
        storage_mappings: _,
        tests,
        errors,
    } = sources;

    assert!(errors.is_empty());

    models::Catalog {
        _schema: None,
        import: Vec::new(), // Fully inline and requires no imports.
        captures: captures
            .into_iter()
            .map(|tables::Capture { capture, spec, .. }| (capture, spec))
            .collect(),
        collections: collections
            .into_iter()
            .map(
                |tables::Collection {
                     collection, spec, ..
                 }| (collection, spec),
            )
            .collect(),
        materializations: materializations
            .into_iter()
            .map(
                |tables::Materialization {
                     materialization,
                     spec,
                     ..
                 }| (materialization, spec),
            )
            .collect(),
        tests: tests
            .into_iter()
            .map(|tables::Test { test, spec, .. }| (test, spec))
            .collect(),

        // We deliberately omit storage mappings.
        // The control plane will inject these during its builds.
        storage_mappings: BTreeMap::new(),
    }
}

pub(crate) fn extend_from_catalog<P>(
    sources: &mut tables::Sources,
    catalog: models::Catalog,
    policy: P,
) -> usize
where
    P: Fn(&str, &url::Url, Option<&url::Url>) -> Vec<url::Url>,
{
    ::sources::merge::extend_from_catalog(sources, catalog, policy)
}

pub(crate) fn pick_policy(
    overwrite: bool,
    flat: bool,
) -> fn(&str, &url::Url, Option<&url::Url>) -> Vec<url::Url> {
    match (overwrite, flat) {
        (true, true) => ::sources::merge::flat_layout_replace,
        (true, false) => ::sources::merge::canonical_layout_replace,
        (false, true) => ::sources::merge::flat_layout_keep,
        (false, false) => ::sources::merge::canonical_layout_keep,
    }
}

pub(crate) struct Resolver {
    pub client: crate::controlplane::Client,
}

impl validation::ControlPlane for Resolver {
    fn resolve_collections<'a, 'b: 'a>(
        &'a self,
        collections: Vec<models::Collection>,
        // These parameters are currently required, but can be removed once we're
        // actually resolving fuzzy pre-built CollectionSpecs from the control plane.
        temp_build_id: &'b str,
        temp_storage_mappings: &'b [tables::StorageMapping],
    ) -> BoxFuture<'a, anyhow::Result<Vec<proto_flow::flow::CollectionSpec>>> {
        async move {
            // TODO(johnny): Introduce a new RPC for doing fuzzy-search given the list of
            // collection names, and use that instead to surface mis-spelt name suggestions.
            // Pair this with a transition to having built specifications in the live_specs table?

            // NameSelector will return *all* collections, rather than *no*
            // collections, if its selector is empty.
            if collections.is_empty() {
                tracing::debug!("there are no remote collections to resolve");
                return Ok(vec![]);
            }

            let list = crate::catalog::List {
                flows: false,
                name_selector: crate::catalog::NameSelector {
                    name: collections.into_iter().map(|c| c.to_string()).collect(),
                    prefix: Vec::new(),
                },
                type_selector: crate::catalog::SpecTypeSelector {
                    captures: Some(false),
                    collections: Some(true),
                    materializations: Some(false),
                    tests: Some(false),
                },
                deleted: false,
            };

            let columns = vec![
                "catalog_name",
                "id",
                "spec",
                "spec_type",
                "updated_at",
                "last_pub_user_email",
            ];
            let rows = crate::catalog::fetch_live_specs(self.client.clone(), &list, columns)
                .await
                .context("failed to fetch collection specs")?;

            tracing::debug!(name=?list.name_selector.name, rows=?rows.len(), "resolved remote collections");

            rows.into_iter()
                .map(|row| {
                    use crate::catalog::SpecRow;
                    let spec = row
                        .parse_spec::<models::CollectionDef>()
                        .context("parsing specification")?;

                    Ok(self.temp_build_collection_helper(
                        row.catalog_name,
                        spec,
                        temp_build_id,
                        temp_storage_mappings,
                    )?)
                })
                .collect::<anyhow::Result<_>>()
        }
        .boxed()
    }
}
