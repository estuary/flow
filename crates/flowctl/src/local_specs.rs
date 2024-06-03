use futures::{FutureExt, TryStreamExt};
use itertools::Itertools;
use proto_flow::flow;
use tables::CatalogResolver;

/// Load and validate sources and derivation connectors (only).
/// Capture and materialization connectors are not validated.
pub(crate) async fn load_and_validate(
    client: crate::controlplane::Client,
    source: &str,
) -> anyhow::Result<(tables::DraftCatalog, tables::Validations)> {
    let source = build::arg_source_to_url(source, false)?;
    let draft = surface_errors(load(&source).await.into_result())?;
    let (draft, _live, built) = validate(client, true, false, true, draft, "").await;
    Ok((draft, surface_errors(built.into_result())?))
}

/// Load and validate sources and all connectors.
pub(crate) async fn load_and_validate_full(
    client: crate::controlplane::Client,
    source: &str,
    network: &str,
) -> anyhow::Result<(tables::DraftCatalog, tables::Validations)> {
    let source = build::arg_source_to_url(source, false)?;
    let sources = surface_errors(load(&source).await.into_result())?;
    let (draft, _live, built) = validate(client, false, false, false, sources, network).await;
    Ok((draft, surface_errors(built.into_result())?))
}

/// Generate connector files by validating sources with derivation connectors.
pub(crate) async fn generate_files(
    client: crate::controlplane::Client,
    sources: tables::DraftCatalog,
) -> anyhow::Result<()> {
    let (mut draft, _live, built) = validate(client, true, false, true, sources, "").await;

    let project_root = build::project_root(&draft.fetches[0].resource);
    build::generate_files(&project_root, &built)?;

    draft.errors = draft
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

    if let Err(errors) = draft.into_result().and_then(|_| built.into_result()) {
        for tables::Error { scope, error } in errors.iter() {
            tracing::error!(%scope, ?error);
        }
        tracing::error!(
            "I may not have generated all files because the Flow specifications have errors.",
        );
    }

    Ok(())
}

pub(crate) async fn load(source: &url::Url) -> tables::DraftCatalog {
    // We never use a file root jail when loading on a user's machine.
    build::load(source, std::path::Path::new("/")).await
}

async fn validate(
    client: crate::controlplane::Client,
    noop_captures: bool,
    noop_derivations: bool,
    noop_materializations: bool,
    draft: tables::DraftCatalog,
    network: &str,
) -> (
    tables::DraftCatalog,
    tables::LiveCatalog,
    tables::Validations,
) {
    let source = &draft.fetches[0].resource.clone();
    let project_root = build::project_root(source);

    let live = Resolver { client }.resolve(draft.all_catalog_names()).await;

    let output = build::validate(
        models::Id::new([0xff; 8]), // Must be larger than all real last_pub_id's.
        models::Id::new([1; 8]),
        true, // Allow local connectors.
        network,
        false, // Don't generate ops collections.
        ops::tracing_log_handler,
        noop_captures,
        noop_derivations,
        noop_materializations,
        &project_root,
        draft,
        live,
    )
    .await;

    // If DEBUG tracing is enabled, then write sources and validations to a
    // debugging database that can be inspected or shipped to Estuary for support.
    if tracing::enabled!(tracing::Level::DEBUG) {
        let seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let db_path = std::env::temp_dir().join(format!("flowctl_{seconds}.sqlite"));
        build::persist(Default::default(), &db_path, &output).expect("failed to write build DB");
        tracing::debug!(db_path=%db_path.to_string_lossy(), "wrote debugging database");
    }

    output.into_parts()
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
    mut draft: tables::DraftCatalog,
) -> anyhow::Result<tables::DraftCatalog> {
    ::sources::indirect_large_files(&mut draft, 1 << 9);
    write_resources(draft)
}

pub(crate) fn write_resources(
    mut draft: tables::DraftCatalog,
) -> anyhow::Result<tables::DraftCatalog> {
    let source = &draft.fetches[0].resource.clone();
    let project_root = build::project_root(source);
    ::sources::rebuild_catalog_resources(&mut draft);

    build::write_files(
        &project_root,
        draft
            .resources
            .iter()
            .map(
                |tables::Resource {
                     resource, content, ..
                 }| (resource.clone(), content.to_vec()),
            )
            .collect(),
    )?;

    Ok(draft)
}

pub(crate) fn into_catalog(draft: tables::DraftCatalog) -> models::Catalog {
    ::sources::merge::into_catalog(draft)
}

pub(crate) fn extend_from_catalog<P>(
    sources: &mut tables::DraftCatalog,
    catalog: tables::DraftCatalog,
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

impl tables::CatalogResolver for Resolver {
    fn resolve<'a>(
        &'a self,
        catalog_names: Vec<&'a str>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = tables::LiveCatalog> + Send + 'a>> {
        async move {
            let result = futures::try_join!(
                self.resolve_specs(&catalog_names),
                self.resolve_inferred_schemas(&catalog_names),
            );

            match result {
                Ok((mut live, inferred_schemas)) => {
                    live.inferred_schemas = inferred_schemas;
                    live
                }
                Err(err) => {
                    let mut live = tables::LiveCatalog::default();
                    live.errors.push(tables::Error {
                        scope: url::Url::parse("flow://control").unwrap(),
                        error: err,
                    });
                    live
                }
            }
        }
        .boxed()
    }
}

impl Resolver {
    async fn resolve_specs(&self, catalog_names: &[&str]) -> anyhow::Result<tables::LiveCatalog> {
        use models::CatalogType;

        #[derive(serde::Deserialize)]
        struct LiveSpec {
            catalog_name: String,
            spec_type: CatalogType,
            #[serde(alias = "spec")]
            model: models::RawValue,
            built_spec: models::RawValue,
            last_pub_id: models::Id,
        }

        let rows = catalog_names
            .into_iter()
            .chunks(API_FETCH_CHUNK_SIZE)
            .into_iter()
            .map(|names| {
                let builder = self
                    .client
                    .from("live_specs_ext")
                    .select("catalog_name,spec_type,spec,built_spec,last_pub_id")
                    .not("is", "spec_type", "null")
                    .in_("catalog_name", names);

                async move { crate::api_exec::<Vec<LiveSpec>>(builder).await }
            })
            .collect::<futures::stream::FuturesUnordered<_>>()
            .try_collect::<Vec<Vec<LiveSpec>>>()
            .await?;

        let mut live = tables::LiveCatalog::default();

        for LiveSpec {
            catalog_name,
            spec_type,
            model,
            built_spec,
            last_pub_id,
        } in rows.into_iter().flat_map(|i| i.into_iter())
        {
            let scope = url::Url::parse(&format!("flow://control/{catalog_name}")).unwrap();

            match spec_type {
                CatalogType::Capture => live.captures.insert_row(
                    models::Capture::new(catalog_name),
                    scope,
                    last_pub_id,
                    serde_json::from_str::<models::CaptureDef>(model.get())?,
                    serde_json::from_str::<flow::CaptureSpec>(built_spec.get())?,
                ),
                CatalogType::Collection => live.collections.insert_row(
                    models::Collection::new(catalog_name),
                    scope,
                    last_pub_id,
                    serde_json::from_str::<models::CollectionDef>(model.get())?,
                    serde_json::from_str::<flow::CollectionSpec>(built_spec.get())?,
                ),
                CatalogType::Materialization => live.materializations.insert_row(
                    models::Materialization::new(catalog_name),
                    scope,
                    last_pub_id,
                    serde_json::from_str::<models::MaterializationDef>(model.get())?,
                    serde_json::from_str::<flow::MaterializationSpec>(built_spec.get())?,
                ),
                CatalogType::Test => live.tests.insert_row(
                    models::Test::new(catalog_name),
                    scope,
                    last_pub_id,
                    serde_json::from_str::<models::TestDef>(model.get())?,
                    serde_json::from_str::<flow::TestSpec>(built_spec.get())?,
                ),
            }
        }

        Ok(live)
    }

    async fn resolve_inferred_schemas(
        &self,
        catalog_names: &[&str],
    ) -> anyhow::Result<tables::InferredSchemas> {
        #[derive(serde::Deserialize, Clone)]
        struct Row {
            pub collection_name: models::Collection,
            pub schema: models::Schema,
            pub md5: String,
        }

        let rows = catalog_names
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
            .try_collect::<Vec<Vec<Row>>>()
            .await?;

        let mut inferred = tables::InferredSchemas::default();

        for Row {
            collection_name,
            schema,
            md5,
        } in rows.into_iter().flat_map(|i| i.into_iter())
        {
            inferred.insert_row(collection_name, schema, md5);
        }

        Ok(inferred)
    }
}

// API_BATCH_SIZE is used to chunk a set of API entities fetched in a single request.
// PostgREST passes query predicates as URL parameters, so if we don't chunk requests
// then we run into URL length limits.
const API_FETCH_CHUNK_SIZE: usize = 25;
