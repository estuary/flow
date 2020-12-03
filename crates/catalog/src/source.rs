use super::{
    specs, sql_params, Collection, ContentType, Endpoint, Materialization, MaterializationTarget,
    Resource, Result, Scope, TestCase,
};
use url::Url;

/// Source represents a top-level catalog build input.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct Source {
    pub resource: Resource,
}

impl Source {
    /// Register an Estuary Catalog specification with the catalog database.
    pub fn register(scope: Scope, uri: Url) -> Result<Source> {
        let resource = Resource::register(scope.db, ContentType::CatalogSpec, &uri)?;
        let scope = scope.push_resource(resource);

        if resource.is_processed(scope.db)? {
            return Ok(Source { resource });
        }
        resource.mark_as_processed(scope.db)?;

        let spec = resource.content(scope.db)?;
        let spec: serde_yaml::Value = serde_yaml::from_slice(&spec)?;
        let spec: serde_yaml::Value = yaml_merge_keys::merge_keys_serde(spec)?;
        let spec: specs::Catalog = serde_yaml::from_value(spec)?;

        // The order of operations here is significant. We register endpoints so that an imported
        // flow yaml may reference an endpoint defined in the file that imports it. Imports are
        // handled after endpoints, then collections, with materializations and captures going last.
        // Materializations and captures may reference collections from either the current file or
        // one imported by it, so they need to be done after registering collections.
        // Tests must be registered after collections, since they may also reference collections
        // defined in the current and imported files.
        for (endpoint_name, endpoint) in spec.endpoints.iter() {
            scope
                .push_prop("endpoints")
                .push_prop(endpoint_name.as_str())
                .then(|s| Endpoint::register(s, endpoint_name.as_str(), endpoint))?;
        }

        for (index, url) in spec.import.iter().enumerate() {
            scope.push_prop("import").push_item(index).then(|scope| {
                let url = resource.join(scope.db, url.as_ref())?;
                let import = Self::register(scope, url)?;
                Resource::register_import(scope, import.resource)
            })?;
        }

        for (package, version) in spec.node_dependencies.iter() {
            scope
                .push_prop("nodeDependencies")
                .push_prop(package)
                .then(|scope| {
                    Ok(scope
                        .db
                        .prepare_cached(
                            "INSERT INTO nodejs_dependencies (package, version) VALUES (?, ?);",
                        )?
                        .execute(sql_params![package, version])?)
                })?;
        }

        for (index, spec) in spec.collections.iter().enumerate() {
            scope
                .push_prop("collections")
                .push_item(index)
                .then(|scope| Collection::register(scope, spec))?;
        }

        for (name, materialization) in spec.materialization_targets.iter() {
            scope
                .push_prop("materializationTargets")
                .push_prop(name)
                .then(|scope| MaterializationTarget::register(&scope, name, materialization))?;
        }

        for (i, materialization) in spec.materializations.iter().enumerate() {
            scope
                .push_prop("materializations")
                .push_item(i)
                .then(|scope| Materialization::register(scope, materialization))?;
        }

        for (name, spec) in spec.tests.iter() {
            scope
                .push_prop("tests")
                .push_prop(name)
                .then(|scope| TestCase::register(scope, name, spec))?;
        }

        Ok(Source { resource })
    }
}

#[cfg(test)]
mod test {
    use super::{
        super::{create, dump_tables, Source},
        *,
    };
    use rusqlite::params as sql_params;
    use serde_json::json;

    #[test]
    fn test_register() -> Result<()> {
        let db = create(":memory:")?;

        let fixture = json!({
            "import": [
                "../other/spec",
                "test://example/other/spec",
            ],
            "nodeDependencies": {
                "package-one": "v0.1.2",
                "pkg-2": "~v2",
            },
            "collections": [
                {
                    "name": "a/collection",
                    "schema": "test://example/schema",
                    "key": ["/key"],
                },
            ],
        });
        db.execute(
            "INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                    (1, 'application/vnd.estuary.dev-catalog-spec+yaml', CAST(? AS BLOB), FALSE),
                    (2, 'application/vnd.estuary.dev-catalog-spec+yaml', CAST('{}' AS BLOB), FALSE),
                    (10, 'application/schema+yaml', CAST('true' AS BLOB), FALSE);",
            sql_params![fixture],
        )?;
        db.execute(
            "INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                    (1, 'test://example/main/spec', TRUE),
                    (2, 'test://example/other/spec', TRUE),
                    (10, 'test://example/schema', TRUE);",
            sql_params![],
        )?;

        let url = Url::parse("test://example/main/spec")?;
        Source::register(Scope::empty(&db), url)?;

        // Expect other catalog spec & schema were processed.
        assert!(Resource { id: 2 }.is_processed(&db)?);
        assert!(Resource { id: 10 }.is_processed(&db)?);

        assert_eq!(
            dump_tables(
                &db,
                &["resource_imports", "collections", "nodejs_dependencies"]
            )?,
            json!({
                "resource_imports": [[1, 2], [1, 10]],
                "collections": [[1, "a/collection", "test://example/schema", ["/key"], 1]],
                "nodejs_dependencies": [["package-one", "v0.1.2"], ["pkg-2", "~v2"]],
            }),
        );

        Ok(())
    }
}
