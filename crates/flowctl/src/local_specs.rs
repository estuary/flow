use futures::{FutureExt, TryStreamExt};
use itertools::Itertools;
use proto_flow::flow;
use tables::CatalogResolver;

/// Load and validate sources and derivation connectors (only).
/// Capture and materialization connectors are not validated.
pub(crate) async fn load_and_validate(
    client: &crate::Client,
    source: &str,
) -> anyhow::Result<(tables::DraftCatalog, tables::Validations)> {
    let source = build::arg_source_to_url(source, false)?;
    let draft = surface_errors(load(&source).await.into_result())?;
    let (draft, built) = validate(client, true, false, true, draft, "").await;
    Ok((draft, surface_errors(built.into_result())?))
}

/// Load and validate sources and all connectors.
pub(crate) async fn load_and_validate_full(
    client: &crate::Client,
    source: &str,
    network: &str,
) -> anyhow::Result<(tables::DraftCatalog, tables::Validations)> {
    let source = build::arg_source_to_url(source, false)?;
    let sources = surface_errors(load(&source).await.into_result())?;
    let (draft, built) = validate(client, false, false, false, sources, network).await;
    Ok((draft, surface_errors(built.into_result())?))
}

/// Generate connector files by validating sources with derivation connectors.
pub(crate) async fn generate_files(
    client: &crate::Client,
    sources: tables::DraftCatalog,
) -> anyhow::Result<()> {
    let (mut draft, built) = validate(client, true, false, true, sources, "").await;

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
    client: &crate::Client,
    noop_captures: bool,
    noop_derivations: bool,
    noop_materializations: bool,
    draft: tables::DraftCatalog,
    network: &str,
) -> (tables::DraftCatalog, tables::Validations) {
    let source = &draft.fetches[0].resource.clone();
    let project_root = build::project_root(source);

    let mut live = Resolver {
        client: client.clone(),
    }
    .resolve(draft.all_catalog_names())
    .await;

    let output = if !live.errors.is_empty() {
        // If there's a live catalog resolution error, surface it through built tables.
        // For historical reasons we don't return the LiveCatalog from this routine.
        let mut built = tables::Validations::default();
        built.errors = std::mem::take(&mut live.errors);
        build::Output { draft, live, built }
    } else {
        build::local(
            models::Id::new([0xff; 8]), // Must be larger than all real last_pub_id's.
            models::Id::new([0xff; 8]), // Must be larger than all real last_build_id's.
            network,
            ops::tracing_log_handler,
            noop_captures,
            noop_derivations,
            noop_materializations,
            &project_root,
            draft,
            live,
        )
        .await
    };

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

    let (draft, _live, built) = output.into_parts();
    (draft, built)
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
    pub client: crate::Client,
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

        // If we're unauthenticated then return a placeholder LiveCatalog.
        if !self.client.is_authenticated() {
            return Ok(build::NoOpCatalogResolver
                .resolve(catalog_names.to_vec())
                .await);
        }
        let mut live = tables::LiveCatalog::default();

        // Query storage mappings from the tenants of `catalog_names`.
        #[derive(serde::Deserialize)]
        struct StorageMappingRow {
            catalog_prefix: models::Prefix,
            id: models::Id,
            spec: models::StorageDef,
        }

        let tenant_filter = catalog_names
            .iter()
            .filter_map(|n| n.find('/').map(|pos| &n[..pos]))
            .sorted()
            .dedup()
            .map(|tenant| format!("catalog_prefix.like.{tenant}/*"))
            .join(",");

        let storage_mappings = crate::api_exec::<Vec<StorageMappingRow>>(
            self.client
                .from("storage_mappings")
                .select("catalog_prefix,id,spec")
                .or(&tenant_filter),
        )
        .await?;

        for row in storage_mappings {
            // TODO(johnny): The PostgREST API does not surface recovery/ mappings.
            // Work around for now, by synthesizing them. This should switch to GraphQL.
            if row.catalog_prefix.starts_with("recovery/") {
                continue; // Does not actually happen in practice.
            }

            live.storage_mappings.insert_row(
                &row.catalog_prefix,
                row.id,
                &row.spec.stores,
                &row.spec.data_planes,
            );
            live.storage_mappings.insert_row(
                models::Prefix::new(format!("recovery/{}", row.catalog_prefix)),
                models::Id::zero(),
                Vec::new(),
                Vec::new(),
            );
        }

        // Query all data planes.
        #[derive(serde::Deserialize)]
        struct DataPlaneRow {
            id: models::Id,
            data_plane_name: String,
        }

        let data_planes = crate::api_exec::<Vec<DataPlaneRow>>(
            self.client.from("data_planes").select("id,data_plane_name"),
        )
        .await?;

        for row in data_planes {
            live.data_planes.insert_row(
                row.id,
                row.data_plane_name,
                String::new(),                 // data_plane_fqdn
                Vec::new(),                    // hmac_keys
                models::RawValue::default(),   // encrypted_hmac_keys
                models::Collection::default(), // ops_logs_name
                models::Collection::default(), // ops_stats_name
                String::new(),                 // broker_address
                String::new(),                 // reactor_address
            );
        }

        #[derive(serde::Deserialize)]
        struct LiveSpec {
            id: models::Id,
            catalog_name: String,
            data_plane_id: models::Id,
            spec_type: CatalogType,
            #[serde(alias = "spec")]
            model: models::RawValue,
            built_spec: models::RawValue,
            last_pub_id: models::Id,
            last_build_id: models::Id,
            dependency_hash: Option<String>,
        }

        let rows = catalog_names
            .into_iter()
            .chunks(API_FETCH_CHUNK_SIZE)
            .into_iter()
            .map(|names| {
                let builder = self
                    .client
                    .from("live_specs_ext")
                    .select("id,catalog_name,data_plane_id,spec_type,spec,built_spec,last_pub_id,last_build_id")
                    .not("is", "spec_type", "null")
                    .in_("catalog_name", names);

                async move { crate::api_exec::<Vec<LiveSpec>>(builder).await }
            })
            .collect::<futures::stream::FuturesUnordered<_>>()
            .try_collect::<Vec<Vec<LiveSpec>>>()
            .await?;

        for LiveSpec {
            id,
            catalog_name,
            spec_type,
            model,
            built_spec,
            last_pub_id,
            last_build_id,
            data_plane_id,
            dependency_hash,
        } in rows.into_iter().flat_map(|i| i.into_iter())
        {
            match spec_type {
                CatalogType::Capture => live.captures.insert_row(
                    models::Capture::new(catalog_name),
                    id,
                    data_plane_id,
                    last_pub_id,
                    last_build_id,
                    serde_json::from_str::<models::CaptureDef>(model.get())?,
                    serde_json::from_str::<flow::CaptureSpec>(built_spec.get())?,
                    dependency_hash,
                ),
                CatalogType::Collection => live.collections.insert_row(
                    models::Collection::new(catalog_name),
                    id,
                    data_plane_id,
                    last_pub_id,
                    last_build_id,
                    serde_json::from_str::<models::CollectionDef>(model.get())?,
                    serde_json::from_str::<flow::CollectionSpec>(built_spec.get())?,
                    dependency_hash,
                ),
                CatalogType::Materialization => live.materializations.insert_row(
                    models::Materialization::new(catalog_name),
                    id,
                    data_plane_id,
                    last_pub_id,
                    last_build_id,
                    serde_json::from_str::<models::MaterializationDef>(model.get())?,
                    serde_json::from_str::<flow::MaterializationSpec>(built_spec.get())?,
                    dependency_hash,
                ),
                CatalogType::Test => live.tests.insert_row(
                    models::Test::new(catalog_name),
                    id,
                    last_pub_id,
                    last_build_id,
                    serde_json::from_str::<models::TestDef>(model.get())?,
                    serde_json::from_str::<flow::TestSpec>(built_spec.get())?,
                    dependency_hash,
                ),
            }
        }

        Ok(live)
    }

    async fn resolve_inferred_schemas(
        &self,
        catalog_names: &[&str],
    ) -> anyhow::Result<tables::InferredSchemas> {
        // If we're unauthenticated then return empty InferredSchemas rather than an error.
        if !self.client.is_authenticated() {
            return Ok(Default::default());
        }

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
