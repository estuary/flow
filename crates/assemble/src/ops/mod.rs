//! The Flow runtime automatically publishes statistics and logs related to each task. It publishes
//! this data to Flow collections, so that users can create derivations and materializations of
//! that data. This module generates the Flow specs and schemas for these collections.
use models;
use proto_flow::flow;
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
pub fn generate_ops_collections(tables: &mut tables::DraftCatalog) {
    let shard_schema = gen_schema!("../../../../ops-catalog/ops-shard-schema.json");
    let stats_schema = gen_schema!("../../../../ops-catalog/ops-stats-schema.json");
    let log_schema = gen_schema!("../../../../ops-catalog/ops-log-schema.json");

    for schema in &[&shard_schema, &stats_schema, &log_schema] {
        tables.resources.insert_row(
            schema.url.clone(),
            flow::ContentType::JsonSchema,
            bytes::Bytes::from_static(schema.content),
            serde_json::from_slice::<models::RawValue>(schema.content)
                .expect("builtin schema is JSON"),
        );
    }

    // Track that stats_schema and log_schema each import shard_schema.
    tables
        .imports
        .insert_row(&stats_schema.url, &shard_schema.url);
    tables
        .imports
        .insert_row(&log_schema.url, &shard_schema.url);

    if !tables.captures.is_empty()
        || !tables.materializations.is_empty()
        || !tables.collections.is_empty()
    {
        let logs_collection_name = format!("ops.us-central1.v1/logs");
        let stats_collection_name = format!("ops.us-central1.v1/stats");

        if !has_collection(&*tables, &logs_collection_name) {
            add_ops_collection(logs_collection_name, log_schema.url.clone(), tables);
        }
        if !has_collection(&*tables, &stats_collection_name) {
            add_ops_collection(stats_collection_name, stats_schema.url.clone(), tables);
        }
    }
}

fn has_collection(tables: &tables::DraftCatalog, name: &str) -> bool {
    tables
        .collections
        .iter()
        .any(|c| c.catalog_name.as_str() == name)
}

fn add_ops_collection(name: String, schema_url: Url, tables: &mut tables::DraftCatalog) {
    let mut scope = builtin_url("ops.yaml");
    scope.set_fragment(Some(&format!("/collections/{name}")));

    let mut schema_scope = scope.clone();
    schema_scope.set_fragment(Some(&format!("{}/schema", scope.fragment().unwrap())));

    let name = models::Collection::new(name);
    let key = vec![
        models::JsonPointer::new("/shard/name"),
        models::JsonPointer::new("/shard/keyBegin"),
        models::JsonPointer::new("/shard/rClockBegin"),
        models::JsonPointer::new("/ts"),
    ];

    tables.collections.insert_row(
        name.clone(),
        scope.clone(),
        None,
        Some(
            serde_json::from_value::<models::CollectionDef>(serde_json::json!({
                "key": key,
                "schema": schema_url.to_string(),
                "projections": {
                    "kind": {
                        "location": "/shard/kind",
                        "partition": true,
                    },
                    "name": {
                        "location": "/shard/name",
                        "partition": true,
                    }
                },
            }))
            .unwrap(),
        ),
    );
    tables.imports.insert_row(schema_scope, schema_url);
}

fn builtin_url(name: &str) -> Url {
    Url::parse(&format!("builtin://flow/{}", name)).unwrap()
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::{from_value, json};

    #[test]
    fn ops_collections_are_generated() {
        let mut tables = tables::DraftCatalog::default();
        tables.captures.insert_row(
            models::Capture::new("acmeCo/foo"),
            builtin_url("test-cap.flow.yaml#/collections/acmeCo~1foo"),
            None,
            Some(
            from_value::<models::CaptureDef>(
                json!({"endpoint":{"connector": {"image": "foo/bar", "config": {}}}, "bindings":[]}),
            )
            .unwrap()),
        );

        // Add an ops collection to the tables so that we can assert that a duplicate ops
        // collection is not generated. Note that this collection is intentionally different from
        // the one that would be generated, and would be invalid to use as a stats collection. But
        // the difference is used to assert that the collection from tables takes precedence.
        let spec: models::CollectionDef = serde_json::from_value(json!({
            "schema": "test://foo.bar/schema",
            "key": ["/not/a/real/key"]
        }))
        .unwrap();

        tables.collections.insert_row(
            models::Collection::new("ops.test-dataplane/logs"),
            Url::parse("test://foo.bar/collection").unwrap(),
            None,
            Some(spec),
        );

        generate_ops_collections(&mut tables);
        insta::assert_debug_snapshot!(&tables);
    }
}
