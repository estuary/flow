use anyhow::Context;
use futures::{FutureExt, TryStreamExt};
use proto_flow::flow;
use tables::CatalogResolver;

// Brings the GraphQL scalar types (notably `Prefix` and `JSON`) into module
// scope so the `graphql_client` derive below can resolve them by name.
use crate::graphql::*;

#[derive(graphql_client::GraphQLQuery)]
#[graphql(
    schema_path = "../flow-client/control-plane-api.graphql",
    query_path = "src/storage_mappings.graphql"
)]
struct StorageMappingsQuery;

/// Load and validate sources and derivation connectors (only).
/// Capture and materialization connectors are not validated.
pub(crate) async fn load_and_validate(
    ctx: &crate::CliContext,
    source: &str,
) -> anyhow::Result<(
    tables::DraftCatalog,
    tables::LiveCatalog,
    tables::Validations,
)> {
    let source = build::arg_source_to_url(source, false)?;
    let draft = surface_errors(load(&source).await.into_result())?;
    let (draft, live, built) =
        validate(ctx, true, false, true, draft, "", ops::tracing_log_handler).await;
    Ok((draft, live, surface_errors(built.into_result())?))
}

/// Load and validate sources and all connectors.
pub(crate) async fn load_and_validate_full(
    ctx: &crate::CliContext,
    source: &str,
    network: &str,
    log_handler: impl runtime::LogHandler,
) -> anyhow::Result<(
    tables::DraftCatalog,
    tables::LiveCatalog,
    tables::Validations,
)> {
    let source = build::arg_source_to_url(source, false)?;
    let sources = surface_errors(load(&source).await.into_result())?;
    let (draft, live, built) =
        validate(ctx, false, false, false, sources, network, log_handler).await;
    Ok((draft, live, surface_errors(built.into_result())?))
}

/// Generate connector files by validating sources with derivation connectors.
pub(crate) async fn generate_files(
    ctx: &crate::CliContext,
    sources: tables::DraftCatalog,
) -> anyhow::Result<()> {
    let (mut draft, _live, built) = validate(
        ctx,
        true,
        false,
        true,
        sources,
        "",
        ops::tracing_log_handler,
    )
    .await;

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
    ctx: &crate::CliContext,
    noop_captures: bool,
    noop_derivations: bool,
    noop_materializations: bool,
    draft: tables::DraftCatalog,
    network: &str,
    log_handler: impl runtime::LogHandler,
) -> (
    tables::DraftCatalog,
    tables::LiveCatalog,
    tables::Validations,
) {
    let source = &draft.fetches[0].resource.clone();
    let project_root = build::project_root(source);

    let mut live = Resolver {
        pg: ctx.pg.clone(),
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
            network,
            log_handler,
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

    let (draft, live, built) = output.into_parts();
    (draft, live, built)
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
    pub pg: postgrest::Postgrest,
    pub rest: flow_client_next::rest::Client,
    pub access_token: Option<String>,
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

        // Use NoOpCatalogResolver to prime with a catch-all storage mapping.
        let mut live = build::NoOpCatalogResolver.resolve(Vec::new()).await;

        // If we're unauthenticated then return the placeholder LiveCatalog.
        if self.access_token.is_none() {
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
                prefixes.push(&name[..index]);
            }
        }
        prefixes.sort();
        prefixes.dedup();

        // Fetch storage mappings through the control-plane GraphQL API, one
        // request per tenant. GraphQL authorizes reads against the snapshot grant
        // graph, which resolves a user's grant to any *ancestor* of a requested
        // prefix. A user scoped to a sub-prefix (e.g. an admin of
        // `acmeCo/team-A/`) can therefore still read the tenant-root mapping at
        // `acmeCo/`. PostgREST could not: its row-level security only walked
        // grants downward, so such users saw zero mappings and discovery failed
        // to resolve a task's data plane.
        let storage_mappings = group_prefixes_by_tenant(&prefixes)
            .into_iter()
            .map(|prefixes| {
                let rest = self.rest.clone();
                let access_token = self.access_token.clone();

                async move {
                    fetch_tenant_storage_mappings(&rest, access_token.as_deref(), prefixes).await
                }
            })
            .collect::<futures::stream::FuturesUnordered<_>>()
            .try_collect::<Vec<Vec<(models::Prefix, models::StorageDef)>>>()
            .await?;

        for (catalog_prefix, spec) in storage_mappings.into_iter().flatten() {
            // A storage mapping is (today) stored as two rows: this collection-data
            // mapping and an empty `recovery/` mapping that `walk_prefix` tolerates.
            // The GraphQL API returns only the former, so synthesize the latter.
            // `control_id` is not consulted by validation, so pass a zero Id.
            live.storage_mappings.insert_row(
                &catalog_prefix,
                models::Id::zero(),
                &spec.stores,
                &spec.data_planes,
            );
            live.storage_mappings.insert_row(
                models::Prefix::new(format!("recovery/{catalog_prefix}")),
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

        let data_planes = flow_client_next::postgrest::exec::<Vec<DataPlaneRow>>(
            self.pg.from("data_planes").select("id,data_plane_name"),
            self.access_token.as_deref(),
        )
        .await
        .context("failed to fetch data planes")?;

        for row in data_planes {
            live.data_planes.insert_row(
                row.id,
                row.data_plane_name,
                String::new(),                 // data_plane_fqdn
                false,                         // closed
                Vec::new(),                    // hmac_keys
                models::RawValue::default(),   // encrypted_hmac_keys
                models::Collection::default(), // ops_logs_name
                models::Collection::default(), // ops_stats_name
                String::new(),                 // broker_address
                String::new(),                 // reactor_address
                None,                          // dekaf_address
                None,                          // dekaf_registry_address
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

        let rows = chunk_names(catalog_names)
            .into_iter()
            .map(|names| {
                let builder = self
                    .pg
                    .from("live_specs_ext")
                    .select("id,catalog_name,data_plane_id,spec_type,spec,built_spec,last_pub_id,last_build_id")
                    .not("is", "spec_type", "null")
                    .in_("catalog_name", names);
                let access_token = self.access_token.clone();

                async move {
                    flow_client_next::postgrest::exec::<Vec<LiveSpec>>(
                        builder,
                        access_token.as_deref(),
                    )
                    .await
                }
            })
            .collect::<futures::stream::FuturesUnordered<_>>()
            .try_collect::<Vec<Vec<LiveSpec>>>()
            .await
            .context("failed to fetch live specs")?;

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
        if self.access_token.is_none() {
            return Ok(Default::default());
        }

        #[derive(serde::Deserialize, Clone)]
        struct Row {
            pub collection_name: models::Collection,
            pub schema: models::Schema,
            pub md5: String,
        }

        let rows = chunk_names(catalog_names)
            .into_iter()
            .map(|names| {
                let builder = self
                    .pg
                    .from("inferred_schemas")
                    .select("collection_name,schema,md5")
                    .in_("collection_name", names);
                let access_token = self.access_token.clone();

                async move {
                    flow_client_next::postgrest::exec::<Vec<Row>>(builder, access_token.as_deref())
                        .await
                }
            })
            .collect::<futures::stream::FuturesUnordered<_>>()
            .try_collect::<Vec<Vec<Row>>>()
            .await
            .context("failed to fetch inferred schemas")?;

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

/// Page size for the control-plane `storageMappings` query. A tenant usually has
/// only a handful of storage mappings, so a single page typically suffices.
const STORAGE_MAPPINGS_PAGE_SIZE: i64 = 100;

/// Group slash-terminated catalog prefixes by tenant (their first path segment),
/// preserving each tenant's prefixes in one group.
///
/// The control-plane `storageMappings` query authorizes a request as a whole and
/// rejects it entirely if the user lacks read access to any requested prefix.
/// Keeping each tenant's prefixes in their own request means an unreadable
/// foreign tenant — reachable through a cross-tenant reference — fails only its
/// own request rather than hiding a readable tenant's mappings.
fn group_prefixes_by_tenant(prefixes: &[&str]) -> Vec<Vec<models::Prefix>> {
    let mut groups: std::collections::BTreeMap<&str, Vec<models::Prefix>> =
        std::collections::BTreeMap::new();

    for &prefix in prefixes {
        // Prefixes are slash-terminated, so the tenant spans up to and including
        // the first '/'.
        let tenant = match prefix.find('/') {
            Some(pos) => &prefix[..=pos],
            None => prefix,
        };
        groups
            .entry(tenant)
            .or_default()
            .push(models::Prefix::new(prefix));
    }

    groups.into_values().collect()
}

/// Fetch the storage mappings for one tenant's `prefixes` via the control-plane
/// GraphQL API, following pagination. Returns each mapping's prefix paired with
/// its parsed [`models::StorageDef`].
///
/// A GraphQL-level error — most commonly an authorization denial for a prefix in
/// this group — yields an empty result rather than propagating. This preserves
/// the silent row-level filtering the prior PostgREST/RLS path performed for
/// prefixes the user could not read, so discovery keeps working across
/// cross-tenant references. Transport and HTTP errors still propagate.
async fn fetch_tenant_storage_mappings(
    rest: &flow_client_next::rest::Client,
    access_token: Option<&str>,
    prefixes: Vec<models::Prefix>,
) -> anyhow::Result<Vec<(models::Prefix, models::StorageDef)>> {
    use graphql_client::GraphQLQuery;

    let mut out = Vec::new();
    let mut after: Option<String> = None;

    loop {
        let variables = storage_mappings_query::Variables {
            // Resolve exactly this tenant's requested prefixes in one request via
            // the `in` exact-set predicate (the going-forward replacement for the
            // deprecated `by: { exactPrefixes }`).
            filter: Some(storage_mappings_query::StorageMappingsFilter {
                catalog_prefix: Some(storage_mappings_query::PrefixFilter {
                    starts_with: None,
                    in_: Some(prefixes.iter().map(|p| p.to_string()).collect()),
                }),
            }),
            after: after.take(),
            first: Some(STORAGE_MAPPINGS_PAGE_SIZE),
        };
        let body = StorageMappingsQuery::build_query(variables);

        let response: graphql_client::Response<storage_mappings_query::ResponseData> =
            crate::graphql::agent_unary(rest, access_token, crate::graphql::GRAPHQL_PATH, &body)
                .await
                .context("failed to fetch storage mappings")?;

        if let Some(errors) = response.errors.filter(|e| !e.is_empty()) {
            tracing::debug!(
                ?errors,
                ?prefixes,
                "storage mappings query returned errors; treating this tenant as having no visible mappings"
            );
            return Ok(Vec::new());
        }

        let connection = response
            .data
            .context("storage mappings query returned no data and no errors")?
            .storage_mappings;

        for edge in connection.edges {
            let spec = serde_json::from_str::<models::StorageDef>(edge.node.spec.get())
                .context("failed to parse storage mapping spec")?;
            out.push((edge.node.catalog_prefix, spec));
        }

        if !connection.page_info.has_next_page {
            break;
        }
        after = connection.page_info.end_cursor;
        // The control plane always returns an endCursor alongside hasNextPage.
        assert!(
            after.is_some(),
            "storageMappings pageInfo missing endCursor"
        );
    }

    Ok(out)
}

// PostgREST passes query predicates (like `column=in.(a,b,c)`) as URL query
// parameters, so fetching a large set of catalog names in a single request can
// produce a URL that exceeds a server or proxy length limit. We therefore split
// fetches into chunks bounded by the encoded length of the names they contain,
// which adapts to how long the names are: deeply-nested names (which
// percent-encode each `/` to `%2F`) can be well over 100 characters each.
//
// Encoded-length budget, in characters, for the names within a single chunk.
// Chosen so the overall URL stays comfortably below the ~2048 byte limit that
// stricter proxies impose, after accounting for the scheme, host, and path.
const API_FETCH_URL_BUDGET: usize = 1800;

/// Estimate the percent-encoded length that `name` contributes to a URL query.
/// Unreserved characters map to a single byte, and everything else (notably the
/// `/` path separators within catalog names) expands to a three-byte `%XX`
/// escape. This intentionally over-estimates rather than under-estimates.
fn encoded_len(name: &str) -> usize {
    name.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
                1
            } else {
                3
            }
        })
        .sum()
}

/// Split `names` into chunks whose combined encoded length stays within
/// [`API_FETCH_URL_BUDGET`]. A name longer than the budget on its own still
/// occupies a chunk by itself, as that's the best we can do without a different
/// query mechanism.
fn chunk_names<'a>(names: &[&'a str]) -> Vec<Vec<&'a str>> {
    let mut chunks = Vec::new();
    let mut chunk: Vec<&'a str> = Vec::new();
    let mut chunk_len = 0;

    for &name in names {
        let cost = encoded_len(name) + 3; // +3 for the `%2C` value separator.

        if !chunk.is_empty() && chunk_len + cost > API_FETCH_URL_BUDGET {
            chunks.push(std::mem::take(&mut chunk));
            chunk_len = 0;
        }
        chunk.push(name);
        chunk_len += cost;
    }
    if !chunk.is_empty() {
        chunks.push(chunk);
    }
    chunks
}

#[cfg(test)]
mod test {
    use super::{API_FETCH_URL_BUDGET, chunk_names, encoded_len, group_prefixes_by_tenant};

    #[test]
    fn groups_prefixes_by_tenant() {
        // Prefixes spanning two tenants must land in separate per-tenant groups so
        // one tenant's authorization outcome never masks another's. Callers pass
        // sorted prefixes (as the resolver does), and grouping by tenant keeps them
        // sorted; tenants themselves come back in sorted order.
        let prefixes = vec![
            "acmeCo/",
            "acmeCo/team-A/",
            "acmeCo/team-A/anvils/",
            "beetleCo/",
            "beetleCo/widgets/",
        ];
        let groups = group_prefixes_by_tenant(&prefixes);

        let as_strings: Vec<Vec<&str>> = groups
            .iter()
            .map(|group| group.iter().map(|p| p.as_str()).collect())
            .collect();
        assert_eq!(
            as_strings,
            vec![
                vec!["acmeCo/", "acmeCo/team-A/", "acmeCo/team-A/anvils/"],
                vec!["beetleCo/", "beetleCo/widgets/"],
            ]
        );
    }

    fn chunk_encoded_len(chunk: &[&str]) -> usize {
        chunk.iter().map(|n| encoded_len(n) + 3).sum()
    }

    // Every chunk must fit within the budget, unless it holds a single name that
    // is itself larger than the budget (which we can't split further).
    fn assert_chunks_within_budget(chunks: &[Vec<&str>], expected_total: usize) {
        for chunk in chunks {
            assert!(
                chunk_encoded_len(chunk) <= API_FETCH_URL_BUDGET || chunk.len() == 1,
                "chunk of {} names exceeded budget at {} chars",
                chunk.len(),
                chunk_encoded_len(chunk),
            );
        }
        assert_eq!(chunks.iter().map(Vec::len).sum::<usize>(), expected_total);
    }

    #[test]
    fn short_names_pack_into_a_single_chunk() {
        let names: Vec<String> = (0..60).map(|i| format!("acmeCo/c{i}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();

        let chunks = chunk_names(&refs);
        // 60 short names encode to well under the budget, so they all fit in one
        // request.
        assert_eq!(chunks.len(), 1);
        assert_chunks_within_budget(&chunks, refs.len());
    }

    #[test]
    fn long_names_split_below_the_url_budget() {
        // Deeply-nested names, each of which percent-encodes to over 100 chars.
        let names: Vec<String> = (0..40)
            .map(|i| {
                format!("acmeCo/prod/source/aurora_postgres/data/main_db/documents/table_{i:03}")
            })
            .collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();

        let chunks = chunk_names(&refs);
        assert!(chunks.len() > 1, "long names should split into many chunks");
        assert_chunks_within_budget(&chunks, refs.len());
    }

    #[test]
    fn a_single_oversized_name_gets_its_own_chunk() {
        let huge = "x/".repeat(API_FETCH_URL_BUDGET); // Far larger than the budget.
        let refs = vec!["a", huge.as_str(), "b"];

        let chunks = chunk_names(&refs);
        assert_eq!(chunks, vec![vec!["a"], vec![huge.as_str()], vec!["b"]]);
    }

    #[test]
    fn empty_input_yields_no_chunks() {
        assert!(chunk_names(&[]).is_empty());
    }
}
