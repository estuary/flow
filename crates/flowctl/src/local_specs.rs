use crate::graphql::*;
use anyhow::Context;
use proto_flow::flow;

/// Load and validate sources and derivation connectors (only).
/// Capture and materialization connectors are not validated.
pub(crate) async fn load_and_validate(
    ctx: &crate::CliContext,
    source: &str,
) -> anyhow::Result<build::Output> {
    let source = build::arg_source_to_url(source, false)?;
    let draft = surface_errors(load(&source).await.into_result())?;
    let output = validate(ctx, true, false, true, draft, ops::tracing_log_handler).await;
    surface_errors(output.into_result())
}

/// Load and validate sources, and the connector of one task.
/// Returns the name of the task selected by `select_task`.
pub(crate) async fn load_and_validate_task(
    ctx: &crate::CliContext,
    source: &str,
    task: Option<&str>,
    log_handler: impl ops::LogHandler,
) -> anyhow::Result<(String, build::Output)> {
    let source = build::arg_source_to_url(source, false)?;
    let mut sources = surface_errors(load(&source).await.into_result())?;
    let task = select_task(&mut sources, task)?;
    let output = validate(ctx, false, false, false, sources, log_handler).await;
    Ok((task, surface_errors(output.into_result())?))
}

/// Select the task named `task`, or else the draft's only task.
/// Every other task is disabled, so that validation doesn't run its connector.
/// A connector Validate may reach out to its endpoint, and a user who means
/// one task shouldn't wait on, or see errors from, all the others.
pub(crate) fn select_task(
    draft: &mut tables::DraftCatalog,
    task: Option<&str>,
) -> anyhow::Result<String> {
    let captures = draft.captures.iter_mut().filter_map(|row| {
        let model = row.model.as_mut()?;
        Some((row.capture.as_str(), &mut model.shards))
    });
    let derivations = draft.collections.iter_mut().filter_map(|row| {
        let derive = row.model.as_mut()?.derive.as_mut()?;
        Some((row.collection.as_str(), &mut derive.shards))
    });
    let materializations = draft.materializations.iter_mut().filter_map(|row| {
        let model = row.model.as_mut()?;
        Some((row.materialization.as_str(), &mut model.shards))
    });
    let mut tasks: Vec<(&str, &mut models::ShardTemplate)> = captures
        .chain(derivations)
        .chain(materializations)
        .collect();

    let task = match (task, tasks.as_slice()) {
        (Some(task), _) => task.to_string(),
        (None, [(only, _)]) => only.to_string(),
        (None, []) => anyhow::bail!(
            "sourced specification files do not contain any tasks (captures, derivations, or materializations)"
        ),
        (None, _) => anyhow::bail!(
            "sourced specification files contain multiple tasks. Use --task to identify a specific one"
        ),
    };
    if !tasks.iter().any(|(name, _)| *name == task) {
        anyhow::bail!("could not find capture, derivation, or materialization {task}");
    }
    for (name, shards) in tasks.iter_mut() {
        if *name != task {
            shards.disable = true;
        }
    }
    Ok(task)
}

/// Generate connector files by validating sources with all connectors.
/// Callers disable tasks which shouldn't be validated (see `select_task`).
pub(crate) async fn generate_files(
    ctx: &crate::CliContext,
    sources: tables::DraftCatalog,
) -> anyhow::Result<()> {
    let build::Output {
        mut draft, built, ..
    } = validate(ctx, false, false, false, sources, ops::tracing_log_handler).await;

    build::generate_files(&draft.fetches[0].resource, &built)?;

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
    ctx: &crate::CliContext,
    noop_captures: bool,
    noop_derivations: bool,
    noop_materializations: bool,
    draft: tables::DraftCatalog,
    log_handler: impl ops::LogHandler,
) -> build::Output {
    let mut live = Resolver {
        rest: ctx.rest.clone(),
        access_token: ctx.access_token(),
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
            ctx.local_connector_router(),
            log_handler,
            noop_captures,
            noop_derivations,
            noop_materializations,
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

    output
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
// resources, then write them out (see `write_resources`). Nothing is written if
// tasks conflict over the content of a resource.
pub(crate) fn indirect_and_write_resources(
    mut draft: tables::DraftCatalog,
) -> anyhow::Result<tables::DraftCatalog> {
    ::sources::indirect_large_files(&mut draft, 1 << 9);
    write_resources(surface_errors(draft.into_result())?)
}

// Write out the local resources of `draft` which are underneath the directory
// of its root source (see `build::write_files`). Every catalog is rebuilt from
// its models, which drops its comments and formatting, so a catalog outside
// that directory, such as one imported by a `../` path, is left as it is.
pub(crate) fn write_resources(
    mut draft: tables::DraftCatalog,
) -> anyhow::Result<tables::DraftCatalog> {
    ::sources::rebuild_catalog_resources(&mut draft);

    build::write_files(
        &draft.fetches[0].resource,
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

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../control-plane-api/control-plane-api.graphql",
    query_path = "src/local-specs-query.graphql",
    extern_enums("CatalogType")
)]
struct ResolveLiveSpecs;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../control-plane-api/control-plane-api.graphql",
    query_path = "src/local-specs-query.graphql"
)]
struct ResolveStorageMappings;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../control-plane-api/control-plane-api.graphql",
    query_path = "src/local-specs-query.graphql"
)]
struct ResolveReadablePrefixes;

pub struct Resolver {
    pub rest: flow_client_next::rest::Client,
    pub access_token: Option<String>,
}

impl Resolver {
    pub async fn resolve(&self, catalog_names: Vec<&str>) -> tables::LiveCatalog {
        match self.resolve_specs(&catalog_names).await {
            Ok(live) => live,
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

    async fn resolve_specs(&self, catalog_names: &[&str]) -> anyhow::Result<tables::LiveCatalog> {
        let mut live = build::no_op_live_catalog();

        // If we're unauthenticated, or there's nothing to resolve, then return
        // the placeholder LiveCatalog.
        if self.access_token.is_none() || catalog_names.is_empty() {
            return Ok(live);
        }

        // Extract all unique slash-terminated prefixes from catalog names.
        // For example, "acmeCo/team-A/anvils/orders" produces:
        // ["acmeCo/", "acmeCo/team-A/", "acmeCo/team-A/anvils/"]
        let mut prefixes = Vec::new();
        for name in catalog_names.iter() {
            let mut index = 0;
            while let Some(pos) = name[index..].find('/') {
                index = index + pos + 1;
                prefixes.push(name[..index].to_string());
            }
        }
        prefixes.sort();
        prefixes.dedup();

        // Skip names that the caller can't read. Validation treats them as
        // missing, as it does deleted specs, whereas `liveSpecs` fails the
        // whole request if any name is unauthorized. `prefixes` matches its
        // filter exactly, so it returns just the grants covering these names.
        let mut readable = Vec::new();
        let mut after = None;
        loop {
            let vars = resolve_readable_prefixes::Variables {
                prefixes: prefixes.clone(),
                after: after.take(),
            };
            let grants = post_graphql::<ResolveReadablePrefixes>(
                &self.rest,
                self.access_token.as_deref(),
                vars,
            )
            .await
            .context("failed to fetch authorized prefixes")?
            .prefixes;

            readable.extend(
                grants
                    .edges
                    .into_iter()
                    .filter(|edge| {
                        edge.node.capabilities.iter().any(|bit| {
                            matches!(bit, resolve_readable_prefixes::CapabilityBit::CatalogRead)
                        })
                    })
                    .map(|edge| edge.node.prefix),
            );
            if !grants.page_info.has_next_page {
                break;
            }
            after = grants.page_info.end_cursor;
            assert!(after.is_some(), "prefixes pageInfo missing endCursor");
        }
        let names: Vec<Name> = catalog_names
            .iter()
            .filter(|name| {
                readable
                    .iter()
                    .any(|prefix| name.starts_with(prefix.as_str()))
            })
            .map(|name| Name::new(*name))
            .collect();

        // The API never returns `recovery/` mappings, so local builds carry no
        // recovery stores. Local builds also make no data-plane placement:
        // that happens when a draft is published.
        let mut after = None;
        loop {
            let vars = resolve_storage_mappings::Variables {
                prefixes: prefixes.clone(),
                after: after.take(),
            };
            let mappings = post_graphql::<ResolveStorageMappings>(
                &self.rest,
                self.access_token.as_deref(),
                vars,
            )
            .await
            .context("failed to fetch storage mappings")?
            .storage_mappings;

            for edge in mappings.edges {
                let stores = edge
                    .node
                    .fragment_stores
                    .into_iter()
                    .map(into_store)
                    .collect::<anyhow::Result<Vec<_>>>()?;

                live.storage_mappings.insert_row(
                    &edge.node.catalog_prefix,
                    models::Id::zero(), // Mapping IDs aren't exposed, and local builds don't use them.
                    &stores,
                    Vec::new(),
                );
            }
            if !mappings.page_info.has_next_page {
                break;
            }
            after = mappings.page_info.end_cursor;
            assert!(
                after.is_some(),
                "storageMappings pageInfo missing endCursor"
            );
        }

        if names.is_empty() {
            return Ok(live);
        }
        let mut after = None;
        loop {
            let vars = resolve_live_specs::Variables {
                names: names.clone(),
                after: after.take(),
            };
            let specs =
                post_graphql::<ResolveLiveSpecs>(&self.rest, self.access_token.as_deref(), vars)
                    .await
                    .context("failed to fetch live specs")?
                    .live_specs;

            for edge in specs.edges {
                // Deleted specs have a name but no live spec.
                let Some(resolve_live_specs::SelectLiveSpec {
                    live_spec_id: id,
                    catalog_type: spec_type,
                    data_plane_id,
                    last_pub_id,
                    last_build_id,
                    model,
                    built_spec,
                    inferred_schema,
                }) = edge.node.live_spec
                else {
                    continue;
                };
                let catalog_name = edge.node.catalog_name.to_string();
                let model =
                    model.with_context(|| format!("live spec {catalog_name} has no model"))?;
                let built_spec = built_spec
                    .with_context(|| format!("live spec {catalog_name} has no built spec"))?;

                if let Some(inferred) = inferred_schema {
                    live.inferred_schemas.insert_row(
                        models::Collection::new(&catalog_name),
                        models::Schema::new(inferred.schema),
                        inferred.md5,
                    );
                }

                match spec_type {
                    CatalogType::Capture => live.captures.insert_row(
                        models::Capture::new(catalog_name),
                        id,
                        data_plane_id,
                        last_pub_id,
                        last_build_id,
                        serde_json::from_str::<models::CaptureDef>(model.get())?,
                        serde_json::from_str::<flow::CaptureSpec>(built_spec.get())?,
                        None,
                    ),
                    CatalogType::Collection => live.collections.insert_row(
                        models::Collection::new(catalog_name),
                        id,
                        data_plane_id,
                        last_pub_id,
                        last_build_id,
                        serde_json::from_str::<models::CollectionDef>(model.get())?,
                        serde_json::from_str::<flow::CollectionSpec>(built_spec.get())?,
                        None,
                    ),
                    CatalogType::Materialization => live.materializations.insert_row(
                        models::Materialization::new(catalog_name),
                        id,
                        data_plane_id,
                        last_pub_id,
                        last_build_id,
                        serde_json::from_str::<models::MaterializationDef>(model.get())?,
                        serde_json::from_str::<flow::MaterializationSpec>(built_spec.get())?,
                        None,
                    ),
                    CatalogType::Test => live.tests.insert_row(
                        models::Test::new(catalog_name),
                        id,
                        last_pub_id,
                        last_build_id,
                        serde_json::from_str::<models::TestDef>(model.get())?,
                        serde_json::from_str::<flow::TestSpec>(built_spec.get())?,
                        None,
                    ),
                }
            }
            if !specs.page_info.has_next_page {
                break;
            }
            after = specs.page_info.end_cursor;
            assert!(after.is_some(), "liveSpecs pageInfo missing endCursor");
        }

        Ok(live)
    }
}

/// Map a fragment store, which flattens the fields of every provider,
/// into the store model of its provider.
fn into_store(
    store: resolve_storage_mappings::SelectFragmentStore,
) -> anyhow::Result<models::Store> {
    use resolve_storage_mappings::FragmentStoreProvider as Provider;

    let resolve_storage_mappings::SelectFragmentStore {
        provider,
        prefix,
        bucket,
        region,
        storage_account_name,
        container_name,
        account_tenant_id,
        endpoint,
    } = store;

    let prefix = prefix.map(models::Prefix::new);
    let bucket = bucket.context("fragment store has no bucket");

    Ok(match provider {
        Provider::S3 => models::Store::S3(models::S3StorageConfig {
            bucket: bucket?,
            prefix,
            region,
        }),
        Provider::GCS => models::Store::Gcs(models::GcsBucketAndPrefix {
            bucket: bucket?,
            prefix,
        }),
        Provider::AZURE => models::Store::Azure(models::AzureStorageConfig {
            account_tenant_id: account_tenant_id.context("Azure store has no tenant ID")?,
            storage_account_name: storage_account_name
                .context("Azure store has no storage account")?,
            container_name: container_name.context("Azure store has no container")?,
            prefix,
        }),
        Provider::CUSTOM => models::Store::Custom(models::CustomStore {
            bucket: bucket?,
            endpoint: models::StorageEndpoint::new(
                endpoint.context("custom store has no endpoint")?,
            ),
            prefix,
        }),
        Provider::Other(other) => anyhow::bail!("unknown fragment store provider {other}"),
    })
}

#[cfg(test)]
mod test {
    use super::select_task;

    #[test]
    fn select_task_disables_all_other_tasks() {
        let fixture = || -> tables::DraftCatalog {
            let catalog: models::Catalog = serde_json::from_value(serde_json::json!({
                "captures": {
                    "acmeCo/capture-one": {
                        "endpoint": {"connector": {"image": "an/image", "config": {}}},
                        "bindings": [],
                    },
                    "acmeCo/capture-two": {
                        "endpoint": {"connector": {"image": "an/image", "config": {}}},
                        "bindings": [],
                    },
                },
                "collections": {
                    "acmeCo/derived": {
                        "schema": {"type": "object"},
                        "key": ["/id"],
                        "derive": {"using": {"sqlite": {}}, "transforms": []},
                    },
                    "acmeCo/plain": {"schema": {"type": "object"}, "key": ["/id"]},
                },
                "materializations": {
                    "acmeCo/materialize": {
                        "endpoint": {"connector": {"image": "an/image", "config": {}}},
                        "bindings": [],
                    },
                },
            }))
            .unwrap();
            catalog.into()
        };
        let disabled = |draft: &tables::DraftCatalog| -> Vec<(String, bool)> {
            let captures = draft.captures.iter().map(|row| {
                let model = row.model.as_ref().unwrap();
                (row.capture.to_string(), model.shards.disable)
            });
            let derivations = draft.collections.iter().filter_map(|row| {
                let derive = row.model.as_ref()?.derive.as_ref()?;
                Some((row.collection.to_string(), derive.shards.disable))
            });
            let materializations = draft.materializations.iter().map(|row| {
                let model = row.model.as_ref().unwrap();
                (row.materialization.to_string(), model.shards.disable)
            });
            captures
                .chain(derivations)
                .chain(materializations)
                .collect()
        };

        let mut draft = fixture();
        let task = select_task(&mut draft, Some("acmeCo/derived")).unwrap();
        insta::assert_debug_snapshot!((task, disabled(&draft)), @r#"
        (
            "acmeCo/derived",
            [
                (
                    "acmeCo/capture-one",
                    true,
                ),
                (
                    "acmeCo/capture-two",
                    true,
                ),
                (
                    "acmeCo/derived",
                    false,
                ),
                (
                    "acmeCo/materialize",
                    true,
                ),
            ],
        )
        "#);

        let mut draft = fixture();
        insta::assert_snapshot!(select_task(&mut draft, None).unwrap_err(), @"sourced specification files contain multiple tasks. Use --task to identify a specific one");
        insta::assert_snapshot!(select_task(&mut draft, Some("acmeCo/plain")).unwrap_err(), @"could not find capture, derivation, or materialization acmeCo/plain");

        let mut draft = fixture();
        draft.captures.clear();
        draft.collections.clear();
        let task = select_task(&mut draft, None).unwrap();
        insta::assert_debug_snapshot!((task, disabled(&draft)), @r#"
        (
            "acmeCo/materialize",
            [
                (
                    "acmeCo/materialize",
                    false,
                ),
            ],
        )
        "#);

        draft.materializations.clear();
        insta::assert_snapshot!(select_task(&mut draft, None).unwrap_err(), @"sourced specification files do not contain any tasks (captures, derivations, or materializations)");
    }
}
