use futures::{future::BoxFuture, FutureExt, TryStreamExt};
use itertools::Itertools;

/// Load and validate sources and derivation connectors (only).
/// Capture and materialization connectors are not validated.
pub(crate) async fn load_and_validate(
    client: crate::controlplane::Client,
    source: &str,
) -> anyhow::Result<(tables::Sources, tables::Validations)> {
    let source = build::arg_source_to_url(source, false)?;
    let sources = surface_errors(load(&source).await.into_result())?;
    let (sources, validations) = validate(client, true, false, true, sources, "").await;
    Ok((sources, surface_errors(validations.into_result())?))
}

/// Load and validate sources and all connectors.
pub(crate) async fn load_and_validate_full(
    client: crate::controlplane::Client,
    source: &str,
    network: &str,
) -> anyhow::Result<(tables::Sources, tables::Validations)> {
    let source = build::arg_source_to_url(source, false)?;
    let sources = surface_errors(load(&source).await.into_result())?;
    let (sources, validations) = validate(client, false, false, false, sources, network).await;
    Ok((sources, surface_errors(validations.into_result())?))
}

/// Generate connector files by validating sources with derivation connectors.
pub(crate) async fn generate_files(
    client: crate::controlplane::Client,
    sources: tables::Sources,
) -> anyhow::Result<()> {
    let (mut sources, validations) = validate(client, true, false, true, sources, "").await;

    let project_root = build::project_root(&sources.fetches[0].resource);
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
    network: &str,
) -> (tables::Sources, tables::Validations) {
    let source = &sources.fetches[0].resource.clone();
    let project_root = build::project_root(source);

    let (sources, mut validations) = build::validate(
        true, // Allow local connectors.
        "local-build",
        network,
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
    validations.errors = validations
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

    let out = build::BuildOutput::new(sources, validations);

    // If DEBUG tracing is enabled, then write sources and validations to a
    // debugging database that can be inspected or shipped to Estuary for support.
    if tracing::enabled!(tracing::Level::DEBUG) {
        let seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let db_path = std::env::temp_dir().join(format!("flowctl_{seconds}.sqlite"));
        build::persist(Default::default(), &db_path, &out).expect("failed to write build DB");
        tracing::debug!(db_path=%db_path.to_string_lossy(), "wrote debugging database");
    }

    out.into_parts()
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
    ::sources::merge::into_catalog(sources)
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
    fn resolve_collections<'a>(
        &'a self,
        collections: Vec<models::Collection>,
    ) -> BoxFuture<'a, anyhow::Result<Vec<proto_flow::flow::CollectionSpec>>> {
        #[derive(serde::Deserialize, Clone)]
        struct Row {
            pub catalog_name: String,
            pub built_spec: Option<proto_flow::flow::CollectionSpec>,
        }

        let type_selector = crate::catalog::SpecTypeSelector {
            captures: Some(false),
            collections: Some(true),
            materializations: Some(false),
            tests: Some(false),
        };

        let rows = collections
            .into_iter()
            .chunks(API_FETCH_CHUNK_SIZE)
            .into_iter()
            .map(|names| {
                let builder = self
                    .client
                    .from("live_specs_ext")
                    .select("catalog_name,built_spec")
                    .in_("catalog_name", names);
                let builder = type_selector.add_live_specs_filters(builder, false);

                async move { crate::api_exec::<Vec<Row>>(builder).await }
            })
            .collect::<futures::stream::FuturesUnordered<_>>()
            .try_collect::<Vec<Vec<Row>>>();

        async move {
            let rows = rows.await?;

            rows
                .into_iter()
                .map(|chunk| chunk.into_iter().map(
                    |Row{ catalog_name, built_spec}| {
                        let Some(built_spec) = built_spec else {
                            anyhow::bail!("collection {catalog_name} is an old specification which must be upgraded to continue. Please contact support for assistance");
                        };
                        Ok(built_spec)
                    }
                ))
                .flatten()
                .try_collect()
        }
        .boxed()
    }

    fn get_inferred_schemas<'a>(
        &'a self,
        collections: Vec<models::Collection>,
    ) -> BoxFuture<
        'a,
        anyhow::Result<std::collections::BTreeMap<models::Collection, validation::InferredSchema>>,
    > {
        #[derive(serde::Deserialize, Clone)]
        struct Row {
            pub collection_name: models::Collection,
            pub schema: models::Schema,
            pub md5: String,
        }

        let rows = collections
            .into_iter()
            .chunks(API_FETCH_CHUNK_SIZE)
            .into_iter()
            .map(|names| {
                let builder = self
                    .client
                    .from("inferred_schemas")
                    .select("collection_name,schema,md5")
                    .in_("collection_name", names);

                async move { crate::api_exec::<Vec<Row>>(builder).await }
            })
            .collect::<futures::stream::FuturesUnordered<_>>()
            .try_collect::<Vec<Vec<Row>>>();

        async move {
            let rows = rows.await?;

            Ok(rows
                .into_iter()
                .map(|chunk| {
                    chunk.into_iter().map(
                        |Row {
                             collection_name,
                             schema,
                             md5,
                         }| {
                            (collection_name, validation::InferredSchema { schema, md5 })
                        },
                    )
                })
                .flatten()
                .collect())
        }
        .boxed()
    }
}

// API_BATCH_SIZE is used to chunk a set of API entities fetched in a single request.
// PostgREST passes query predicates as URL parameters, so if we don't chunk requests
// then we run into URL length limits.
const API_FETCH_CHUNK_SIZE: usize = 25;
