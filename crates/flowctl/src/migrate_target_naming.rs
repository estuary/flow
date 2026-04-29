use anyhow::Context;
use futures::stream::{self, StreamExt};
use models::{MaterializationDef, MaterializationEndpoint, TargetNaming, TargetNamingStrategy};
use proto_flow::flow::MaterializationSpec;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

/// Migrate existing materializations to use explicit targetNaming strategies.
///
/// Analyzes all materializations and determines the appropriate TargetNamingStrategy
/// based on current source.targetNaming and endpoint configuration.
/// Prints a detailed report of every decision. Without --execute, this is a dry run.
#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct MigrateTargetNaming {
    /// Filter to materializations under a catalog prefix (e.g. "acmeCo/" or "acmeCo/prod/my-mat").
    #[clap(long)]
    prefix: Option<String>,
    /// Actually publish the migration changes. Without this flag, only a dry-run report is printed.
    #[clap(long)]
    execute: bool,
}

#[derive(Debug)]
enum Action {
    /// Connector doesn't support schemas (no x-schema-name in resource spec).
    SkipNoSchemaSupport,
    /// targetNaming is already set on this materialization.
    SkipAlreadySet,
    /// Not a connector-based materialization (Dekaf or Local).
    SkipNotConnector,
    /// Task is disabled and has no built spec, so there's nothing to analyze.
    SkipDisabledNoBuiltSpec,
    /// Connector supports x-schema-name but isn't in the connector_info() map,
    /// so we can't safely compute schema_idx or the compat-path behavior.
    SkipUnknownConnector,
    /// Can't determine schema automatically.
    NeedsManualIntervention { reason: String },
    /// Standard migration: set targetNaming + fill x-schema-name.
    Migrate,
}

struct MaterializationAnalysis {
    catalog_name: String,
    connector_image: Option<String>,
    /// `last_pub_id` observed at analysis time. Passed as `expect_pub_id` at
    /// publish time so that any intervening publication (user or controller)
    /// fails the concurrency check rather than silently overwriting intent
    /// derived from a now-stale spec.
    last_pub_id: models::Id,
    /// JSON pointer to x-schema-name in the resource config (e.g. "/schema").
    schema_ptr: Option<String>,
    legacy_naming: Option<TargetNaming>,
    endpoint_schema: Option<String>,
    detected_schema: Option<String>,
    proposed_target_naming: Option<TargetNamingStrategy>,
    action: Action,
    binding_analyses: Vec<BindingAnalysis>,
}

struct BindingAnalysis {
    index: usize,
    collection_name: String,
    current_schema: Option<String>,
    proposed_schema: Option<String>,
    current_path: Vec<String>,
    would_change_path: bool,
    is_disabled: bool,
    /// Path would normally change, but connector compat behavior preserves it
    /// (e.g. Snowflake keeps 1-element paths when schema matches endpoint default).
    path_preserved_by_compat: bool,
    /// When set, binding already has x-schema-name but it differs from what
    /// the strategy would produce. Not blocking (existing value is preserved),
    /// but future bindings added by auto-discover would get this value instead.
    schema_mismatch_warning: Option<String>,
    /// Binding is missing x-schema-name, and filling it in would change the
    /// schema from what the connector actually resolved in the resource path.
    /// This is blocking: the migration would move the binding to a different schema.
    would_change_schema: bool,
    /// When the strategy-derived schema differs from the actual schema in the
    /// resource path, this records what the strategy would have produced.
    /// The binding gets the actual schema (in proposed_schema) to preserve
    /// current behavior; this field is for reporting only.
    strategy_schema_override: Option<String>,
}

#[derive(serde::Deserialize)]
struct LiveSpecRow {
    catalog_name: String,
    last_pub_id: models::Id,
    /// Kept as raw JSON so we can distinguish explicit vs defaulted fields
    /// (e.g. whether `source.targetNaming` was set by the user or filled in
    /// by serde's default). Parsed into `MaterializationDef` at use sites.
    spec: Option<Value>,
    built_spec: Option<MaterializationSpec>,
    connector_image_name: Option<String>,
    connector_image_tag: Option<String>,
}

/// Returns true iff the raw spec JSON has an explicit `source.targetNaming`.
/// A bare-string source (just a capture name) and an object source without
/// a `targetNaming` key both count as "no explicit intent."
fn has_explicit_source_target_naming(spec_raw: &Value) -> bool {
    spec_raw
        .get("source")
        .and_then(|s| s.as_object())
        .is_some_and(|obj| obj.contains_key("targetNaming"))
}

struct ConnectorInfo {
    /// Endpoint-config field that holds the schema/dataset value (e.g. "schema", "dataset").
    endpoint_schema_field: &'static str,
    /// Index within the connector's emitted resource path that corresponds to
    /// x-schema-name. For most SQL connectors this is 0 (path = [schema, table]).
    /// For BigQuery/MotherDuck/Fabric it's 1 (path = [dataset, schema, table]).
    schema_path_index: usize,
    /// Whether the connector returns a 1-element resource path when the binding's
    /// schema equals the endpoint schema (Snowflake backwards-compat behavior).
    schema_aware_path_compat: bool,
    /// The connector's well-known default schema when none is set in endpoint config.
    /// Used for compat connectors to determine when a 1-element path would be preserved.
    default_schema: Option<&'static str>,
}

fn connector_info(connector_image: &str) -> Option<ConnectorInfo> {
    match connector_short_name(connector_image) {
        "materialize-postgres"
        | "materialize-alloydb"
        | "materialize-supabase-postgres"
        | "materialize-timescaledb"
        | "materialize-amazon-rds-postgres"
        | "materialize-amazon-aurora-postgres"
        | "materialize-google-cloud-sql-postgres"
        | "materialize-cratedb"
        | "materialize-spanner"
        | "materialize-sqlserver"
        | "materialize-amazon-rds-sqlserver"
        | "materialize-google-cloud-sql-sqlserver"
        | "materialize-redshift"
        | "materialize-starburst" => Some(ConnectorInfo {
            endpoint_schema_field: "schema",
            schema_path_index: 0,
            schema_aware_path_compat: false,
            default_schema: None,
        }),
        "materialize-snowflake" => Some(ConnectorInfo {
            endpoint_schema_field: "schema",
            schema_path_index: 0,
            schema_aware_path_compat: true,
            default_schema: Some("PUBLIC"),
        }),
        "materialize-bigquery" => Some(ConnectorInfo {
            endpoint_schema_field: "dataset",
            schema_path_index: 1,
            schema_aware_path_compat: false,
            default_schema: None,
        }),
        "materialize-databricks" => Some(ConnectorInfo {
            endpoint_schema_field: "schema_name",
            schema_path_index: 0,
            schema_aware_path_compat: false,
            default_schema: None,
        }),
        "materialize-iceberg" => Some(ConnectorInfo {
            endpoint_schema_field: "namespace",
            schema_path_index: 0,
            schema_aware_path_compat: false,
            default_schema: None,
        }),
        "materialize-motherduck" | "materialize-azure-fabric-warehouse" => Some(ConnectorInfo {
            endpoint_schema_field: "schema",
            schema_path_index: 1,
            schema_aware_path_compat: false,
            default_schema: None,
        }),
        _ => None,
    }
}

pub async fn do_migrate_target_naming(
    ctx: &mut crate::CliContext,
    args: &MigrateTargetNaming,
) -> anyhow::Result<()> {
    tracing::info!("fetching materializations");
    let rows = fetch_materializations(&ctx.client, args.prefix.as_deref()).await?;
    tracing::info!(count = rows.len(), "fetched materializations");

    tracing::info!("fetching resource spec schemas from connector_tags");
    let schema_pointers = fetch_resource_spec_pointers(&ctx.client, &rows).await?;

    let analyses: Vec<MaterializationAnalysis> = rows
        .iter()
        .map(|row| analyze_materialization(row, &schema_pointers))
        .collect();

    print_report(&analyses, &schema_pointers);

    if !args.execute {
        return Ok(());
    }

    execute_migration(ctx, &analyses).await
}

/// `schema_pointers` maps full connector image (name+tag) to the x-schema-name
/// JSON pointer from `pointer_for_schema()`, or None if the connector doesn't
/// support x-schema-name.
fn analyze_materialization(
    row: &LiveSpecRow,
    schema_pointers: &HashMap<String, Option<String>>,
) -> MaterializationAnalysis {
    let empty = |action: Action| MaterializationAnalysis {
        catalog_name: row.catalog_name.clone(),
        connector_image: row.connector_image_name.clone(),
        last_pub_id: row.last_pub_id,
        schema_ptr: None,
        legacy_naming: None,
        endpoint_schema: None,
        detected_schema: None,
        proposed_target_naming: None,
        action,
        binding_analyses: Vec::new(),
    };

    let spec_raw = match &row.spec {
        Some(s) => s,
        None => return empty(Action::SkipNotConnector),
    };
    let spec: MaterializationDef = match serde_json::from_value(spec_raw.clone()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(catalog_name = %row.catalog_name, error = %e, "failed to parse materialization spec");
            return empty(Action::SkipNotConnector);
        }
    };

    if !matches!(spec.endpoint, MaterializationEndpoint::Connector(_)) {
        return empty(Action::SkipNotConnector);
    }
    if spec.target_naming.is_some() {
        return empty(Action::SkipAlreadySet);
    }
    if spec.shards.disable && row.built_spec.is_none() {
        return empty(Action::SkipDisabledNoBuiltSpec);
    }

    let connector_image = row.connector_image_name.as_deref().unwrap_or("");
    let connector_tag = row.connector_image_tag.as_deref().unwrap_or("");
    let full_image = format!("{connector_image}{connector_tag}");

    // Look up x-schema-name pointer from connector_tags resource spec schema.
    let schema_ptr: Option<&str> = schema_pointers
        .get(&full_image)
        .and_then(|opt| opt.as_deref());

    // No x-schema-name pointer means the connector doesn't support schemas.
    if schema_ptr.is_none() {
        return empty(Action::SkipNoSchemaSupport);
    }

    // If the connector supports x-schema-name but isn't in our info map, we
    // can't safely read schema_idx or the compat-path flag. Bail rather than
    // best-effort with defaults, which would silently read the wrong path
    // index for connectors where x-schema-name lives past position 0.
    let ci = match connector_info(connector_image) {
        Some(ci) => ci,
        None => return empty(Action::SkipUnknownConnector),
    };
    // `legacy_naming` is Some only when the user explicitly set
    // `source.targetNaming`. A bare-string source or an object source
    // without `targetNaming` carries no customer intent, so we treat it
    // the same as a missing source (None) and fall through to
    // MatchSourceStructure with SingleSchema fallback.
    let legacy_naming = if has_explicit_source_target_naming(spec_raw) {
        spec.source
            .as_ref()
            .map(|s| s.to_normalized_def().target_naming)
    } else {
        None
    };

    let endpoint_schema = {
        let config = match &spec.endpoint {
            MaterializationEndpoint::Connector(c) => Some(&c.config),
            _ => None,
        };
        config.and_then(|c| {
            let config_value: Value = serde_json::from_str(c.get()).ok()?;
            config_value
                .get(ci.endpoint_schema_field)
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
        })
    };

    let schema_idx = ci.schema_path_index;

    let detected_schema = if endpoint_schema.is_none() {
        row.built_spec
            .as_ref()
            .and_then(|bs| detect_schema_from_paths(bs, schema_idx))
    } else {
        None
    };

    let default_schema = ci.default_schema;
    let resolved_schema = endpoint_schema
        .as_deref()
        .or(detected_schema.as_deref())
        .or(default_schema);

    let (mut action, mut proposed_target_naming) =
        match propose_target_naming(legacy_naming.as_ref(), resolved_schema) {
            Some(proposed) => (Action::Migrate, Some(proposed)),
            None => (
                Action::NeedsManualIntervention {
                    reason:
                        "no endpoint schema and no consistent schema detected from resource paths"
                            .into(),
                },
                None,
            ),
        };

    let compat_paths = ci.schema_aware_path_compat;

    // Effective endpoint schema: the explicit endpoint config value, falling
    // back to the connector's well-known default (e.g. Snowflake -> PUBLIC).
    // This is used by the compat check to determine whether a 1-element path
    // would be preserved when x-schema-name is set.
    let effective_endpoint_schema = endpoint_schema.as_deref().or(default_schema);

    let mut binding_analyses = analyze_bindings(
        &spec,
        row.built_spec.as_ref(),
        proposed_target_naming.as_ref(),
        schema_ptr,
        schema_idx,
        compat_paths,
        effective_endpoint_schema,
        legacy_naming.is_some(),
    );

    // Escalate to MANUAL if filling in x-schema-name on any binding would
    // change the schema from what the connector actually resolved in the
    // resource path. This only fires when legacy_naming is absent (no
    // customer intent). When legacy_naming IS set, analyze_bindings handles
    // the mismatch per-binding by filling in the actual schema from the
    // resource path, preserving the customer's strategy for future bindings.
    //
    // Before giving up, try SingleSchema with the resolved endpoint schema.
    // MatchSourceStructure can fail when collection names don't match the
    // actual path schemas, but all paths might still agree on one schema.
    let schema_change_count = binding_analyses
        .iter()
        .filter(|b| b.would_change_schema && !b.is_disabled)
        .count();
    if schema_change_count > 0 && matches!(action, Action::Migrate) {
        let mut fell_back = false;
        if let Some(schema) = resolved_schema {
            let alt = TargetNamingStrategy::SingleSchema {
                schema: schema.to_string(),
                table_template: None,
            };
            let alt_bindings = analyze_bindings(
                &spec,
                row.built_spec.as_ref(),
                Some(&alt),
                schema_ptr,
                schema_idx,
                compat_paths,
                effective_endpoint_schema,
                false,
            );
            if alt_bindings
                .iter()
                .all(|b| !b.would_change_schema || b.is_disabled)
            {
                proposed_target_naming = Some(alt);
                binding_analyses = alt_bindings;
                fell_back = true;
            }
        }
        if !fell_back {
            action = Action::NeedsManualIntervention {
                reason: format!(
                    "{schema_change_count} binding(s) would change schema if x-schema-name is set to the value from {:?}",
                    proposed_target_naming,
                ),
            };
        }
    }

    // would_change_path: filling in x-schema-name would change the resource path
    // (e.g. ["my_table"] becomes ["public", "my_table"]), changing the state key
    // and requiring a backfill via feature flags.
    let path_change_count = binding_analyses
        .iter()
        .filter(|b| b.would_change_path)
        .count();
    if path_change_count > 0 && matches!(action, Action::Migrate) {
        action = Action::NeedsManualIntervention {
            reason: format!(
                "{path_change_count} binding(s) have 1-element resource paths that would change when x-schema-name is set (e.g. [\"table\"] -> [\"schema\", \"table\"]). To migrate safely, use retain_existing_data_on_backfill, allow_existing_tables_for_new_bindings, and notBefore"
            ),
        };
    }

    MaterializationAnalysis {
        catalog_name: row.catalog_name.clone(),
        connector_image: row.connector_image_name.clone(),
        last_pub_id: row.last_pub_id,
        schema_ptr: schema_ptr.map(|s| s.to_string()),
        legacy_naming,
        endpoint_schema,
        detected_schema,
        proposed_target_naming,
        action,
        binding_analyses,
    }
}

/// Map legacy source.targetNaming to a TargetNamingStrategy.
/// Returns None when the strategy requires a schema but none is available.
///
/// WithSchema and no-source-capture both map to MatchSourceStructure, which
/// derives schema from collection names and doesn't need a resolved schema.
/// MatchSourceStructure was also the de facto default before targetNaming
/// existed (update_materialization_resource_spec unconditionally derived
/// x-schema-name from collection names).
fn propose_target_naming(
    legacy: Option<&TargetNaming>,
    resolved_schema: Option<&str>,
) -> Option<TargetNamingStrategy> {
    match legacy {
        Some(TargetNaming::WithSchema) | None => Some(TargetNamingStrategy::MatchSourceStructure {
            table_template: None,
            schema_template: None,
        }),
        Some(TargetNaming::PrefixSchema) => {
            resolved_schema.map(|s| TargetNamingStrategy::PrefixTableNames {
                schema: s.to_string(),
                skip_common_defaults: false,
                table_template: None,
            })
        }
        Some(TargetNaming::PrefixNonDefaultSchema) => {
            resolved_schema.map(|s| TargetNamingStrategy::PrefixTableNames {
                schema: s.to_string(),
                skip_common_defaults: true,
                table_template: None,
            })
        }
        Some(TargetNaming::NoSchema) => {
            resolved_schema.map(|s| TargetNamingStrategy::SingleSchema {
                schema: s.to_string(),
                table_template: None,
            })
        }
    }
}

async fn fetch_materializations(
    client: &crate::Client,
    prefix: Option<&str>,
) -> anyhow::Result<Vec<LiveSpecRow>> {
    let page_size: usize = 50;
    let concurrency: usize = 2;

    let mut total: usize = 0;
    let mut offset: usize = 0;
    loop {
        let mut builder = client
            .from("live_specs_ext")
            .select("catalog_name")
            .eq("spec_type", "materialization")
            .not("is", "spec", "null")
            .range(offset, offset + 999);

        if let Some(p) = prefix {
            builder = builder.like("catalog_name", &format!("{p}%"));
        }

        let page: Vec<serde_json::Value> = crate::api_exec(builder)
            .await
            .with_context(|| "counting materializations")?;

        let count = page.len();
        total += count;
        if count < 1000 {
            break;
        }
        offset += 1000;
    }
    tracing::info!(total, "counted materializations, fetching concurrently");

    // Now fetch full pages concurrently.
    let offsets: Vec<usize> = (0..total).step_by(page_size).collect();
    let prefix_owned = prefix.map(|p| p.to_string());

    let mut all_rows = Vec::with_capacity(total);
    let mut page_stream = stream::iter(offsets)
        .map(|offset| {
            let client = client.clone();
            let prefix_owned = prefix_owned.clone();
            async move {
                let mut builder = client
                    .from("live_specs_ext")
                    .select("catalog_name,last_pub_id,spec,built_spec,connector_image_name,connector_image_tag")
                    .eq("spec_type", "materialization")
                    .not("is", "spec", "null")
                    .range(offset, offset + page_size - 1);

                if let Some(p) = &prefix_owned {
                    builder = builder.like("catalog_name", &format!("{p}%"));
                }

                crate::api_exec::<Vec<LiveSpecRow>>(builder)
                    .await
                    .with_context(|| format!("fetching materializations at offset {offset}"))
            }
        })
        .buffer_unordered(concurrency);

    while let Some(page_result) = page_stream.next().await {
        all_rows.extend(page_result?);
        tracing::info!(count = all_rows.len(), total, "fetched page");
    }

    tracing::info!(count = all_rows.len(), "fetched all materializations");
    Ok(all_rows)
}

/// Fetch the x-schema-name JSON pointer for each connector by reading
/// `connector_tags.resource_spec_schema` and running `pointer_for_schema()`.
///
/// Returns a map from full connector image (name+tag) to the x-schema-name
/// JSON pointer path (e.g. "/schema", "/dataset"), or None if the connector
/// doesn't support x-schema-name.
async fn fetch_resource_spec_pointers(
    client: &crate::Client,
    rows: &[LiveSpecRow],
) -> anyhow::Result<HashMap<String, Option<String>>> {
    let mut cache: HashMap<String, Option<String>> = HashMap::new();

    // Collect unique (image_name, image_tag) pairs.
    let unique_images: std::collections::HashSet<(&str, &str)> = rows
        .iter()
        .filter_map(|r| {
            Some((
                r.connector_image_name.as_deref()?,
                r.connector_image_tag.as_deref()?,
            ))
        })
        .collect();

    tracing::info!(
        count = unique_images.len(),
        "looking up connector image resource spec schemas"
    );

    #[derive(serde::Deserialize)]
    struct ConnectorRow {
        connector_tags: Vec<ConnectorTagRow>,
    }
    #[derive(serde::Deserialize)]
    struct ConnectorTagRow {
        resource_spec_schema: Option<Value>,
    }

    for (image_name, image_tag) in unique_images {
        let full_image = format!("{image_name}{image_tag}");

        let schema_ptr = match async {
            let response = client
                .pg_client()
                .from("connectors")
                .select("connector_tags(resource_spec_schema)")
                .eq("image_name", image_name)
                .eq("connector_tags.image_tag", image_tag)
                .single()
                .execute()
                .await
                .context("querying connector_tags")?;

            if !response.status().is_success() {
                return anyhow::Ok(None);
            }

            let body = response.text().await?;
            let row: ConnectorRow = serde_json::from_str(&body)
                .with_context(|| format!("parsing connector_tags response for {image_name}"))?;

            Ok(row
                .connector_tags
                .into_iter()
                .next()
                .and_then(|t| t.resource_spec_schema))
        }
        .await
        {
            Ok(Some(schema_json)) => {
                let schema_str = schema_json.to_string();
                match tables::utils::pointer_for_schema(&schema_str) {
                    Ok(ptrs) => ptrs.x_schema_name.map(|p| p.to_string()),
                    Err(e) => {
                        tracing::warn!(connector = image_name, error = %e, "failed to parse resource spec schema");
                        None
                    }
                }
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(connector = image_name, error = %e, "failed to fetch resource spec schema");
                None
            }
        };

        cache.insert(full_image, schema_ptr);
    }

    Ok(cache)
}

/// If all active built bindings agree on `path[schema_idx]`, return it.
/// Returns None if any active binding has a non-empty path that's too short
/// to contain a schema at `schema_idx` (e.g. single-element paths mixed with
/// multi-element paths), since the short-path bindings may be in a different
/// schema and the inference would be unreliable.
fn detect_schema_from_paths(built_spec: &MaterializationSpec, schema_idx: usize) -> Option<String> {
    let mut seen: Option<&str> = None;

    for binding in &built_spec.bindings {
        let path = &binding.resource_path;
        if path.len() >= schema_idx + 2 {
            // Has enough elements to read schema.
        } else if !path.is_empty() {
            return None; // Too short to read schema; unsafe to infer.
        } else {
            continue;
        }
        let schema = path[schema_idx].as_str();
        if schema.is_empty() {
            return None;
        }

        match seen {
            None => seen = Some(schema),
            Some(prev) if prev == schema => {}
            Some(_) => return None, // Disagreement.
        }
    }

    seen.map(|s| s.to_string())
}

/// For each binding, determine what would happen if we apply the proposed
/// `TargetNamingStrategy`: would x-schema-name need to be filled in, and if
/// so, would that change the resource path or target a different database schema?
///
/// Matches spec bindings to their built counterparts by resource path
/// (extracted from `_meta.path` in the resource config JSON).
fn analyze_bindings(
    spec: &MaterializationDef,
    built_spec: Option<&MaterializationSpec>,
    strategy: Option<&TargetNamingStrategy>,
    schema_ptr: Option<&str>,
    schema_idx: usize,
    compat_paths: bool,
    endpoint_schema: Option<&str>,
    has_legacy_naming: bool,
) -> Vec<BindingAnalysis> {
    let built_by_path: HashMap<&[String], _> = built_spec
        .map(|b| {
            b.bindings
                .iter()
                .map(|b| (b.resource_path.as_slice(), b))
                .collect()
        })
        .unwrap_or_default();
    let mut results = Vec::new();

    for (idx, spec_binding) in spec.bindings.iter().enumerate() {
        let disabled = spec_binding.disable;

        let collection_name = spec_binding.source.collection().as_str().to_string();

        let current_schema = schema_ptr.and_then(|ptr| {
            let resource: Value = serde_json::from_str(spec_binding.resource.get()).ok()?;
            resource
                .pointer(ptr)
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
        });

        let model_path =
            validation::load_resource_meta_path(spec_binding.resource.get().as_bytes());
        let current_path: Vec<String> = built_by_path
            .get(model_path.as_slice())
            .map(|b| b.resource_path.clone())
            .unwrap_or_default();

        let mut proposed = match strategy {
            Some(TargetNamingStrategy::MatchSourceStructure { .. }) => {
                collection_name.rsplit('/').nth(1).map(|s| s.to_string())
            }
            Some(TargetNamingStrategy::SingleSchema { schema, .. })
            | Some(TargetNamingStrategy::PrefixTableNames { schema, .. }) => Some(schema.clone()),
            None => None,
        };

        let needs_update = schema_ptr.is_some() && proposed.is_some() && current_schema.is_none();

        // Connectors with schema-aware path compat (e.g. Snowflake) return a
        // 1-element path when the binding schema matches the endpoint schema,
        // so filling in x-schema-name won't actually change the resource path.
        // Snowflake uses case-insensitive comparison (schemasEqual / EqualFold)
        // for simple identifiers, so we must do the same here.
        let compat_preserves_path = compat_paths
            && match (proposed.as_deref(), endpoint_schema) {
                (Some(p), Some(e)) => p.eq_ignore_ascii_case(e),
                _ => false,
            };

        let path_preserved_by_compat =
            !disabled && needs_update && current_path.len() == 1 && compat_preserves_path;

        // For compat connectors with 1-element paths where the proposed schema
        // DIFFERS from the effective endpoint default: this is a schema change,
        // not just a path change. The binding sits in the endpoint default schema,
        // but the strategy wants a different schema. This feeds into the
        // MatchSourceStructure -> SingleSchema fallback in the caller.
        let compat_would_change_schema = !disabled
            && needs_update
            && compat_paths
            && current_path.len() == 1
            && !compat_preserves_path;

        // Non-compat connectors with 1-element paths: the path would grow from
        // [table] to [schema, table], changing the state key. Compat connectors
        // that would change schema are handled separately above.
        let would_change_path = !disabled
            && needs_update
            && current_path.len() == 1
            && !compat_preserves_path
            && !compat_paths;

        // Binding HAS x-schema-name: warn if it differs from what the strategy
        // would produce. The existing value is preserved, but future bindings
        // added by auto-discover would get the strategy's value instead.
        let schema_mismatch_warning = match (current_schema.as_deref(), proposed.as_deref()) {
            (Some(current), Some(prop)) if current != prop => Some(prop.to_string()),
            _ => None,
        };

        // Binding MISSING x-schema-name with a multi-element resource path:
        // check if filling in the proposed value would change the schema from
        // what the connector actually resolved in the resource path.
        //
        // When the customer expressed intent (has_legacy_naming), override
        // proposed_schema with the actual schema from the resource path instead
        // of escalating. This preserves the customer's targetNaming strategy
        // for future bindings while filling in existing bindings with where
        // their data actually lives.
        let mut would_change_schema = false;
        let mut strategy_schema_override: Option<String> = None;

        if needs_update && !disabled {
            let actual_schema = if compat_would_change_schema {
                // Compat connector, 1-element path: data lives in the endpoint default schema.
                endpoint_schema.map(|s| s.to_string())
            } else if let Some(prop) = proposed.as_deref() {
                // Multi-element path: check if path[schema_idx] differs from proposed.
                if current_path.len() >= schema_idx + 2 {
                    let path_schema = &current_path[schema_idx];
                    if path_schema != prop {
                        Some(path_schema.clone())
                    } else {
                        None // No mismatch.
                    }
                } else {
                    None
                }
            } else {
                None
            };

            if let Some(actual) = actual_schema {
                if has_legacy_naming {
                    // Customer expressed intent: keep their strategy, fill in actual schema.
                    strategy_schema_override = proposed.clone();
                    proposed = Some(actual);
                } else {
                    would_change_schema = true;
                }
            }
        }

        results.push(BindingAnalysis {
            index: idx,
            collection_name,
            current_schema,
            proposed_schema: if needs_update { proposed } else { None },
            current_path,
            would_change_path,
            path_preserved_by_compat,
            is_disabled: disabled,
            schema_mismatch_warning,
            would_change_schema,
            strategy_schema_override,
        });
    }

    results
}

fn print_report(
    analyses: &[MaterializationAnalysis],
    schema_pointers: &HashMap<String, Option<String>>,
) {
    let action_order: Vec<Action> = vec![
        Action::Migrate,
        Action::NeedsManualIntervention {
            reason: String::new(),
        },
        Action::SkipNoSchemaSupport,
        Action::SkipAlreadySet,
        Action::SkipNotConnector,
        Action::SkipDisabledNoBuiltSpec,
        Action::SkipUnknownConnector,
    ];

    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for a in analyses {
        *counts.entry(a.action.to_string()).or_default() += 1;
    }

    println!("\n=== Target Naming Migration: Dry-Run Report ===\n");

    println!("Connector x-schema-name pointers (from connector_tags):");
    let mut sorted_pointers: Vec<_> = schema_pointers
        .iter()
        .filter_map(|(image, ptr)| ptr.as_ref().map(|p| (image.as_str(), p.as_str())))
        .collect();
    sorted_pointers.sort();
    sorted_pointers.dedup();
    for (image, ptr) in &sorted_pointers {
        println!("  {image} -> {ptr}");
    }

    println!("\nSummary:");
    for (action, count) in &counts {
        println!("  {action:25} {count}");
    }
    println!("  {:25} {}", "TOTAL", analyses.len());

    for action_kind in &action_order {
        let matching: Vec<&MaterializationAnalysis> = analyses
            .iter()
            .filter(|a| std::mem::discriminant(&a.action) == std::mem::discriminant(action_kind))
            .collect();
        if matching.is_empty() {
            continue;
        }

        println!("\n--- {action_kind} ({}) ---", matching.len());
        for a in &matching {
            match &a.action {
                Action::Migrate => {
                    println!("{}", a.format_migrate_detail());
                }
                Action::NeedsManualIntervention { .. } => {
                    println!("{}", a.format_manual_detail());
                }
                _ => {
                    println!("- {} [{}]", a.catalog_name, a.connector_short());
                }
            }
        }
    }

    // Warnings section.
    let mut warnings = Vec::new();

    for (image, ptr) in schema_pointers {
        if ptr.is_some() && connector_info(image).is_none() {
            let short = connector_short_name(image);
            warnings.push(format!(
                "{short} supports x-schema-name but is NOT in the connector info map; all such materializations were classified SKIP_UNKNOWN_CONNECTOR. Add it to connector_info() so they can be analyzed."
            ));
        }
    }

    if !warnings.is_empty() {
        println!("\n--- Warnings ---");
        for w in &warnings {
            println!("  {w}");
        }
    }
}

async fn execute_migration(
    ctx: &mut crate::CliContext,
    analyses: &[MaterializationAnalysis],
) -> anyhow::Result<()> {
    let to_migrate: Vec<&MaterializationAnalysis> = analyses
        .iter()
        .filter(|a| matches!(a.action, Action::Migrate))
        .collect();

    if to_migrate.is_empty() {
        println!("\nNo materializations to migrate.");
        return Ok(());
    }

    println!(
        "\n=== Execute Mode ===\n\nWill publish changes to {} materializations.",
        to_migrate.len(),
    );
    println!("Each materialization will be published individually.\n");

    if !prompt_to_continue("migrate").await {
        anyhow::bail!("migration cancelled");
    }

    let mut success_count = 0usize;
    let mut fail_count = 0usize;

    for (idx, a) in to_migrate.iter().enumerate() {
        // Re-fetch the spec to get the latest version for modification.
        // This also gives us the current last_pub_id for optimistic concurrency.
        let row: LiveSpecRow = match crate::api_exec(
            ctx.client
                .from("live_specs_ext")
                .select("catalog_name,last_pub_id,spec,built_spec,connector_image_name,connector_image_tag")
                .eq("spec_type", "materialization")
                .eq("catalog_name", &a.catalog_name)
                .single(),
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                println!("  [{}/{}] {} SKIPPED (re-fetch failed: {e})", idx + 1, to_migrate.len(), a.catalog_name);
                fail_count += 1;
                continue;
            }
        };

        let spec_raw = match &row.spec {
            Some(s) => s,
            None => {
                println!(
                    "  [{}/{}] {} SKIPPED (spec disappeared)",
                    idx + 1,
                    to_migrate.len(),
                    a.catalog_name
                );
                fail_count += 1;
                continue;
            }
        };
        let spec: MaterializationDef = match serde_json::from_value(spec_raw.clone()) {
            Ok(s) => s,
            Err(e) => {
                println!(
                    "  [{}/{}] {} FAILED to parse re-fetched spec: {e}",
                    idx + 1,
                    to_migrate.len(),
                    a.catalog_name
                );
                fail_count += 1;
                continue;
            }
        };

        // Bail if the spec has been published since analysis: the proposed
        // strategy and per-binding x-schema-name values are derived from the
        // dry-run spec, and a newer spec could make them stale (e.g. the user
        // changed source.targetNaming, the endpoint config schema, or shard
        // disable). Re-running dry-run re-classifies against current state.
        if row.last_pub_id != a.last_pub_id {
            println!(
                "  [{}/{}] {} SKIPPED (spec changed since analysis: last_pub_id {} -> {}; re-run dry-run)",
                idx + 1,
                to_migrate.len(),
                a.catalog_name,
                a.last_pub_id,
                row.last_pub_id,
            );
            fail_count += 1;
            continue;
        }

        // Verify binding structure hasn't changed since analysis. Redundant
        // with the last_pub_id check above, but cheap and clarifies the cause
        // when it does fire.
        let refetched_collections: Vec<&str> = spec
            .bindings
            .iter()
            .map(|b| b.source.collection().as_str())
            .collect();
        let analyzed_collections: Vec<&str> = a
            .binding_analyses
            .iter()
            .map(|b| b.collection_name.as_str())
            .collect();
        if refetched_collections != analyzed_collections {
            println!(
                "  [{}/{}] {} SKIPPED (bindings changed since analysis)",
                idx + 1,
                to_migrate.len(),
                a.catalog_name
            );
            fail_count += 1;
            continue;
        }

        let modified_spec = match build_modified_spec(a, &spec, a.schema_ptr.as_deref()) {
            Ok(s) => s,
            Err(e) => {
                println!(
                    "  [{}/{}] {} FAILED to build spec: {e}",
                    idx + 1,
                    to_migrate.len(),
                    a.catalog_name
                );
                fail_count += 1;
                continue;
            }
        };

        let detail = format!("migrate-target-naming:\n{}", a.format_migrate_detail());

        match publish_one(ctx, &a.catalog_name, a.last_pub_id, &modified_spec, &detail).await {
            Ok(()) => {
                println!("  [{}/{}] {} OK", idx + 1, to_migrate.len(), a.catalog_name);
                success_count += 1;
            }
            Err(e) => {
                println!(
                    "  [{}/{}] {} FAILED: {e}",
                    idx + 1,
                    to_migrate.len(),
                    a.catalog_name
                );
                fail_count += 1;
            }
        }
    }

    println!(
        "\nMigration complete: {success_count} succeeded, {fail_count} failed out of {} total.",
        to_migrate.len(),
    );

    if fail_count > 0 {
        anyhow::bail!("{fail_count} materialization(s) failed to publish");
    }

    Ok(())
}

/// Build the modified spec JSON for a materialization.
///
/// Sets `targetNaming` and fills in x-schema-name on bindings that need it.
fn build_modified_spec(
    a: &MaterializationAnalysis,
    original_spec: &MaterializationDef,
    schema_ptr: Option<&str>,
) -> anyhow::Result<Value> {
    let mut spec: Value =
        serde_json::to_value(original_spec).context("serializing spec to JSON")?;

    // Set targetNaming.
    let strategy = a
        .proposed_target_naming
        .as_ref()
        .context("analysis has no proposed targetNaming")?;
    spec["targetNaming"] = serde_json::to_value(strategy).context("serializing targetNaming")?;

    // Update bindings.
    let bindings = spec
        .get_mut("bindings")
        .and_then(|v| v.as_array_mut())
        .context("spec missing bindings array")?;

    for ba in &a.binding_analyses {
        let binding = bindings
            .get_mut(ba.index)
            .with_context(|| format!("binding index {} out of range", ba.index))?;

        // Fill in x-schema-name on bindings that need it.
        if let (Some(proposed), Some(ptr)) = (&ba.proposed_schema, schema_ptr) {
            let resource = binding
                .get_mut("resource")
                .context("binding missing resource")?;

            if let Some(target) = resource.pointer_mut(ptr) {
                *target = Value::String(proposed.clone());
            } else {
                // Field doesn't exist yet; create it. For single-segment pointers
                // like "/schema" this is equivalent to resource["schema"] = ...
                let field = ptr.strip_prefix('/').unwrap_or(ptr);
                resource[field] = Value::String(proposed.clone());
            }
        }
    }

    Ok(spec)
}

/// Publish a single materialization spec change through the draft/publish cycle.
async fn publish_one(
    ctx: &mut crate::CliContext,
    catalog_name: &str,
    expect_pub_id: models::Id,
    spec: &Value,
    detail: &str,
) -> anyhow::Result<()> {
    // Create a draft.
    let draft = crate::draft::create_draft(&ctx.client).await?;

    // Upsert the modified spec into the draft.
    #[derive(serde::Serialize)]
    struct DraftSpec<'a> {
        draft_id: models::Id,
        catalog_name: &'a str,
        spec_type: &'static str,
        spec: &'a Value,
        expect_pub_id: models::Id,
    }

    let draft_spec = DraftSpec {
        draft_id: draft.id,
        catalog_name,
        spec_type: "materialization",
        spec,
        expect_pub_id,
    };

    crate::api_exec::<Vec<Value>>(
        ctx.client
            .from("draft_specs")
            .upsert(serde_json::to_string(&[&draft_spec]).unwrap())
            .on_conflict("draft_id,catalog_name"),
    )
    .await
    .context("upserting draft spec")?;

    // Publish the draft.
    #[derive(serde::Deserialize)]
    struct PubRow {
        id: models::Id,
        logs_token: String,
    }

    let PubRow { id, logs_token } = crate::api_exec(
        ctx.client
            .from("publications")
            .select("id,logs_token")
            .insert(
                serde_json::json!({
                    "detail": detail,
                    "draft_id": draft.id,
                    "dry_run": false,
                })
                .to_string(),
            )
            .single(),
    )
    .await
    .context("creating publication")?;

    let outcome = crate::poll_while_queued(&ctx.client, "publications", id, &logs_token).await?;

    crate::draft::print_draft_errors(ctx, draft.id).await?;

    if outcome != "success" {
        let _ = crate::draft::delete_draft(&ctx.client, draft.id).await;
        anyhow::bail!("publication {id} failed with status: {outcome}");
    }

    Ok(())
}

async fn prompt_to_continue(confirmation_word: &str) -> bool {
    let word = confirmation_word.to_string();
    tokio::task::spawn_blocking(move || {
        println!("Enter the word '{word}' to continue, or anything else to abort:");
        let mut buf = String::with_capacity(16);

        match std::io::stdin().read_line(&mut buf) {
            Ok(_) => buf.trim() == word,
            Err(err) => {
                tracing::error!(error = %err, "failed to read from stdin");
                false
            }
        }
    })
    .await
    .expect("failed to join spawned task")
}

impl MaterializationAnalysis {
    fn proposed_label(&self) -> String {
        self.proposed_target_naming
            .as_ref()
            .map(|s| format!("{s:?}"))
            .unwrap_or_else(|| "?".to_string())
    }

    fn connector_short(&self) -> &str {
        self.connector_image
            .as_deref()
            .map(connector_short_name)
            .unwrap_or("?")
    }

    /// Format the detail for a MIGRATE materialization.
    /// Used for both the dry-run report and the publication detail string.
    fn format_migrate_detail(&self) -> String {
        let short = self.connector_short();
        let proposed = self.proposed_label();

        let mut lines = vec![
            format!("- {} [{short}]", self.catalog_name),
            format!("  proposed: {proposed}"),
            format!("  reasoning: {}", format_reasoning(self)),
        ];

        let disabled_count = self
            .binding_analyses
            .iter()
            .filter(|b| b.is_disabled)
            .count();
        let has_built_spec = self
            .binding_analyses
            .iter()
            .any(|b| !b.current_path.is_empty());
        let bindings_missing = self
            .binding_analyses
            .iter()
            .filter(|b| b.proposed_schema.is_some())
            .count();
        let path_changes = self
            .binding_analyses
            .iter()
            .filter(|b| b.would_change_path)
            .count();
        let total = self.binding_analyses.len();

        if !has_built_spec {
            lines.push(
                "  WARNING: no built spec available, cannot verify resource paths".to_string(),
            );
        }

        let mut summary = if bindings_missing > 0 {
            let mut s = format!("  {bindings_missing}/{total} bindings missing x-schema-name");
            if disabled_count > 0 {
                s.push_str(&format!(" ({disabled_count} disabled)"));
            }
            s
        } else {
            let distinct: BTreeMap<&str, usize> = self
                .binding_analyses
                .iter()
                .filter_map(|b| b.current_schema.as_deref())
                .fold(BTreeMap::new(), |mut m, s| {
                    *m.entry(s).or_default() += 1;
                    m
                });
            let values: Vec<String> = distinct
                .iter()
                .map(|(s, c)| format!("\"{s}\" ({c})"))
                .collect();
            let mismatch_count = self
                .binding_analyses
                .iter()
                .filter(|b| b.schema_mismatch_warning.is_some())
                .count();
            if mismatch_count > 0 {
                format!(
                    "  {total} bindings, all already have x-schema-name [{}], no x-schema-name changes needed but {mismatch_count} differ from strategy (new bindings will get the strategy's value)",
                    values.join(", "),
                )
            } else {
                format!(
                    "  {total} bindings, all already have x-schema-name [{}], no changes needed",
                    values.join(", "),
                )
            }
        };
        if path_changes > 0 {
            summary.push_str(&format!(", {path_changes} would change resource path"));
        }
        lines.push(summary);

        // Per-binding details for bindings with interesting changes.
        for b in self.binding_analyses.iter().filter(|b| {
            if b.is_disabled && b.schema_mismatch_warning.is_none() {
                return false;
            }
            b.proposed_schema.is_some()
                || b.would_change_path
                || b.would_change_schema
                || b.path_preserved_by_compat
                || b.schema_mismatch_warning.is_some()
                || b.strategy_schema_override.is_some()
        }) {
            let disabled = if b.is_disabled { " [DISABLED]" } else { "" };
            lines.push(format!("  - [{}] {}{disabled}", b.index, b.collection_name,));
            if let Some(strategy_schema) = &b.schema_mismatch_warning {
                let existing = b.current_schema.as_deref().unwrap_or("?");
                lines.push(format!(
                    "    x-schema-name: \"{existing}\" (kept, but strategy will produce \"{strategy_schema}\" for new bindings)",
                ));
            } else if let Some(strategy_would) = &b.strategy_schema_override {
                let cur = b.current_schema.as_deref().unwrap_or("(empty)");
                let prop = b.proposed_schema.as_deref().unwrap_or("?");
                lines.push(format!(
                    "    x-schema-name: {cur} -> {prop} (actual; strategy would produce \"{strategy_would}\" for new bindings)",
                ));
            } else {
                let cur = b.current_schema.as_deref().unwrap_or("(empty)");
                let prop = b.proposed_schema.as_deref().unwrap_or("(unchanged)");
                lines.push(format!("    x-schema-name: {cur} -> {prop}"));
            }
            if b.current_path.is_empty() {
                lines.push("    path: (missing built spec)".to_string());
            } else if b.would_change_path || b.would_change_schema {
                lines.push(format!("    path: {:?} WOULD CHANGE", b.current_path));
            } else if b.path_preserved_by_compat {
                lines.push(format!(
                    "    path: {:?} (preserved by connector compat, schema matches endpoint default)",
                    b.current_path,
                ));
            } else {
                lines.push(format!("    path: {:?}", b.current_path));
            }
        }

        lines.join("\n")
    }

    /// Format the detail for a MANUAL (NeedsManualIntervention) materialization.
    fn format_manual_detail(&self) -> String {
        let short = self.connector_short();
        let reason = match &self.action {
            Action::NeedsManualIntervention { reason } => reason.as_str(),
            _ => "?",
        };
        let legacy = match &self.legacy_naming {
            Some(tn) => format!("{tn:?}"),
            None => "(no source capture)".to_string(),
        };

        let mut lines = vec![
            format!("- {} [{short}]", self.catalog_name),
            format!("  source.targetNaming={legacy}"),
        ];

        if self.proposed_target_naming.is_some() {
            lines.push(format!(
                "  rejected: {} ({})",
                self.proposed_label(),
                format_reasoning(self),
            ));
        }
        lines.push(format!("  reason: {reason}"));

        let problematic: Vec<&BindingAnalysis> = self
            .binding_analyses
            .iter()
            .filter(|b| b.would_change_schema || b.would_change_path)
            .collect();
        let pi = self
            .connector_image
            .as_deref()
            .and_then(connector_info)
            .map(|ci| ci.schema_path_index);

        if !problematic.is_empty() {
            for b in &problematic {
                let prop = b.proposed_schema.as_deref().unwrap_or("?");
                if b.would_change_schema {
                    let path_schema = pi
                        .and_then(|i| b.current_path.get(i))
                        .map(|s| s.as_str())
                        .unwrap_or("?");
                    lines.push(format!(
                        "  - [{}] {}  path: {:?}, path[{}] has \"{path_schema}\", would set \"{prop}\"",
                        b.index, b.collection_name, b.current_path, pi.unwrap_or(0),
                    ));
                } else {
                    lines.push(format!(
                        "  - [{}] {}  path: {:?} WOULD ADD SCHEMA PREFIX \"{prop}\"",
                        b.index, b.collection_name, b.current_path,
                    ));
                }
            }
        } else {
            let mut path_len_counts: BTreeMap<usize, usize> = BTreeMap::new();
            let mut schemas: BTreeMap<&str, usize> = BTreeMap::new();
            let pi_val = pi.unwrap_or(0);
            for b in &self.binding_analyses {
                if b.is_disabled || b.current_path.is_empty() {
                    continue;
                }
                *path_len_counts.entry(b.current_path.len()).or_default() += 1;
                if let Some(s) = b.current_path.get(pi_val) {
                    *schemas.entry(s.as_str()).or_default() += 1;
                }
            }
            let total = self.binding_analyses.len();
            let disabled = self
                .binding_analyses
                .iter()
                .filter(|b| b.is_disabled)
                .count();
            lines.push(format!(
                "  {total} bindings ({disabled} disabled), path lengths: {path_len_counts:?}",
            ));
            if !schemas.is_empty() {
                let schema_list: Vec<String> = schemas
                    .iter()
                    .map(|(s, c)| format!("\"{s}\" ({c})"))
                    .collect();
                lines.push(format!(
                    "  path[{pi_val}] values: {}",
                    schema_list.join(", "),
                ));
            }
        }

        lines.join("\n")
    }
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SkipNoSchemaSupport => write!(f, "SKIP_NO_SCHEMA"),
            Self::SkipAlreadySet => write!(f, "SKIP_ALREADY_SET"),
            Self::SkipNotConnector => write!(f, "SKIP_NOT_CONNECTOR"),
            Self::SkipDisabledNoBuiltSpec => write!(f, "SKIP_DISABLED_NO_BUILT_SPEC"),
            Self::SkipUnknownConnector => write!(f, "SKIP_UNKNOWN_CONNECTOR"),
            Self::NeedsManualIntervention { .. } => write!(f, "MANUAL"),
            Self::Migrate => write!(f, "MIGRATE"),
        }
    }
}

fn format_reasoning(a: &MaterializationAnalysis) -> String {
    let legacy_label = match &a.legacy_naming {
        Some(tn) => format!("source.targetNaming={tn:?}"),
        None => "no source capture".to_string(),
    };

    let proposed_label = a.proposed_label();

    let is_match_source = matches!(
        a.proposed_target_naming,
        Some(TargetNamingStrategy::MatchSourceStructure { .. })
    );

    // MatchSourceStructure derives schema from collection names, so the
    // endpoint/detected schema is irrelevant to the strategy.
    let schema_source = if is_match_source {
        String::new()
    } else {
        match (&a.endpoint_schema, &a.detected_schema) {
            (Some(val), _) => {
                let field = a
                    .connector_image
                    .as_deref()
                    .and_then(connector_info)
                    .map(|ci| ci.endpoint_schema_field)
                    .unwrap_or("?");
                format!("; schema \"{val}\" from endpoint config \"{field}\"")
            }
            (_, Some(val)) => {
                format!("; schema \"{val}\" detected from existing resource paths")
            }
            _ => String::new(),
        }
    };

    format!("{legacy_label} -> {proposed_label}{schema_source}")
}

fn connector_short_name(image: &str) -> &str {
    let name = image.rsplit('/').next().unwrap_or(image);
    name.split(':').next().unwrap_or(name)
}
