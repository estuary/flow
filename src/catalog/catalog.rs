use super::{sql_params, Collection, ContentType, Resource, Result, Scope};
use crate::specs::build as specs;
use url::Url;

/// Source represents a top-level catalog build input.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct Catalog {
    pub resource: Resource,
}

impl Catalog {
    /// Register an Estuary Catalog specification with the catalog database.
    pub fn register(scope: Scope, uri: Url) -> Result<Catalog> {
        let resource = Resource::register(scope.db, ContentType::CatalogSpec, &uri)?;
        let scope = scope.push_resource(resource);

        if resource.is_processed(scope.db)? {
            return Ok(Catalog { resource });
        }
        resource.mark_as_processed(scope.db)?;

        let spec = resource.content(scope.db)?;
        let spec: specs::Catalog = serde_yaml::from_slice(&spec)?;

        for (index, url) in spec.import.iter().enumerate() {
            scope.push_prop("import").push_item(index).then(|scope| {
                let url = resource.join(scope.db, url)?;
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

        Ok(Catalog { resource })
    }
}

#[cfg(test)]
mod test {
    use super::{
        super::{dump_tables, init_db_schema, open, Catalog},
        *,
    };
    use rusqlite::params as sql_params;
    use serde_json::json;

    #[test]
    fn test_register() -> Result<()> {
        let db = open(":memory:")?;
        init_db_schema(&db)?;

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
        Catalog::register(Scope::empty(&db), url)?;

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
