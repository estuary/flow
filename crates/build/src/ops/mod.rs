//! The Flow runtime automatically publishes statistics and logs related to each task. It publishes
//! this data to Flow collections, so that users can create derivations and materializations of
//! that data. This module generates the Flow specs and schemas for these collections.
use models::names;
use protocol::flow::ContentType;
use serde_json::Value;
use std::collections::BTreeSet;
use url::Url;

struct GeneratedSchema {
    url: Url,
    content: &'static [u8],
}

macro_rules! gen_schema {
    ($name:expr) => {
        GeneratedSchema {
            content: &*include_bytes!($name),
            url: builtin_url($name),
        }
    };
}

/// Adds ops collections to the given partial built catalog. The tables will be modified in place
/// to add the resources required for the ops (logs and stats) collections.
pub fn generate_ops_collections(tables: &mut sources::Tables) {
    let tenants = all_tenant_names(tables);
    let shard_schema = gen_schema!("ops-shard-schema.json");
    let stats_schema = gen_schema!("ops-stats-schema.json");
    let log_schema = gen_schema!("ops-log-schema.json");

    for schema in &[&shard_schema, &stats_schema, &log_schema] {
        tables.resources.insert_row(
            schema.url.clone(),
            ContentType::JsonSchema,
            bytes::Bytes::from_static(schema.content),
        );
        let dom = serde_json::from_slice::<Value>(schema.content)
            .expect("failed to parse builtin schema");
        tables.schema_docs.insert_row(schema.url.clone(), dom);
    }

    let importers = tables
        .resources
        .iter()
        .filter(|r| r.content_type == ContentType::CatalogSpec)
        .map(|r| r.resource.clone())
        .collect::<Vec<_>>();
    for importer in importers {
        tables.imports.insert_row(
            ops_collection_resource_url(),
            importer,
            ops_collection_resource_url(),
        );
    }

    for tenant in tenants {
        let logs_collection_name = format!("ops/{}/logs", tenant);
        let stats_collection_name = format!("ops/{}/stats", tenant);
        add_ops_collection(
            logs_collection_name,
            log_schema.url.clone(),
            Some(names::JsonPointer::new("/ts")),
            tables,
        );
        add_ops_collection(
            stats_collection_name,
            stats_schema.url.clone(),
            None,
            tables,
        );
    }
}

fn ops_collection_resource_url() -> Url {
    builtin_url("ops/generated/collections")
}

fn add_ops_collection(
    name: String,
    schema_url: Url,
    add_key: Option<names::JsonPointer>,
    tables: &mut sources::Tables,
) {
    let scope = ops_collection_resource_url();

    let name = names::Collection::new(name);
    let mut key = vec![
        names::JsonPointer::new("/shard/name"),
        names::JsonPointer::new("/shard/keyBegin"),
        names::JsonPointer::new("/shard/rClockBegin"),
    ];
    if let Some(add) = add_key {
        key.push(add);
    }

    tables.collections.insert_row(
        scope.clone(),
        name.clone(),
        schema_url,
        names::CompositeKey::new(key),
    );

    // Ops collections are partitioned by shard, so that each shard has dedicated journals for
    // logs and stats. The names of these partition fields are important because partitions are
    // ordered lexicographically, and we want the partitions to be ordered from most general to
    // most specific (kind -> name -> rangeKeyBegin -> rangeRClockBegin). This corresponds to the
    // hierarchy that will be used for storing fragments.
    let projections = &[
        ("kind", "/shard/kind"),
        ("name", "/shard/name"),
        ("rangeKeyBegin", "/shard/keyBegin"),
        ("rangeRClockBegin", "/shard/rClockBegin"),
    ];
    for projection in projections {
        tables.projections.insert_row(
            scope.clone(),
            name.clone(),
            projection.0.to_string(),
            names::JsonPointer::new(projection.1),
            true,
            true,
        );
    }
}

fn all_tenant_names(tables: &sources::Tables) -> BTreeSet<String> {
    let mut tenants = BTreeSet::new();
    let captures = tables.captures.iter().map(|c| c.capture.as_str());
    let derivations = tables.derivations.iter().map(|d| d.derivation.as_str());
    let materializations = tables
        .materializations
        .iter()
        .map(|m| m.materialization.as_str());
    let iter = captures.chain(derivations).chain(materializations);
    for name in iter {
        let tenant = first_path_component(name);
        if !tenants.contains(tenant) {
            tenants.insert(tenant.to_string());
        }
    }
    tenants
}

fn first_path_component(task_name: &str) -> &str {
    match task_name.split_once('/') {
        Some((first, _)) => first,
        None => task_name,
    }
}

fn builtin_url(name: &str) -> Url {
    Url::parse(&format!("builtin://flow/{}", name)).unwrap()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn ops_collections_are_generated() {
        use models::names;

        let mut tables = sources::Tables::default();
        tables.captures.insert_row(
            builtin_url("test-cap.flow.yaml#/collections/acmeCo~1foo"),
            names::Capture::new("acmeCo/foo"),
            protocol::flow::EndpointType::AirbyteSource,
            serde_json::json!({}),
            7u32,
        );
        tables.captures.insert_row(
            builtin_url("test-cap.flow.yaml#/collections/shamazon~1bar"),
            names::Capture::new("shamazon/bar"),
            protocol::flow::EndpointType::AirbyteSource,
            serde_json::json!({}),
            8u32,
        );
        tables.derivations.insert_row(
            builtin_url("test-der.flow.yaml#/collections/gooble~1ads"),
            names::Collection::new("gooble/ads"),
            builtin_url(
                "test-der.flow.yaml?ptr=/collections/shamazon~1bar/derivation/register/schema",
            ),
            Value::Null,
        );
        tables.derivations.insert_row(
            builtin_url("test-der.flow.yaml#/collections/acmeCo~1tnt"),
            names::Collection::new("acmeCo/tnt"),
            builtin_url(
                "test-der.flow.yaml?ptr=/collections/acmeCo~1tnt/derivation/register/schema",
            ),
            Value::Null,
        );
        tables.materializations.insert_row(
            builtin_url("test-mat.flow.yaml#/collections/justme"),
            names::Materialization::new("justme"),
            protocol::flow::EndpointType::Postgresql,
            Value::Null,
        );

        generate_ops_collections(&mut tables);

        insta::assert_debug_snapshot!("ops_generated_schema_docs", &tables.schema_docs);
        insta::assert_debug_snapshot!("ops_generated_collections", &tables.collections);
        insta::assert_debug_snapshot!("ops_generated_projections", &tables.projections);
    }
}
