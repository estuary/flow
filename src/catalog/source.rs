use super::{sql_params, Collection, ContentType, Error, Resource, Result, DB};
use crate::specs::build as specs;
use url::Url;

/// Source represents a top-level catalog build input.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct Source {
    pub resource: Resource,
}

impl Source {
    /// Register an Estuary Source specification with the catalog.
    pub fn register(db: &DB, uri: Url) -> Result<Source> {
        let source = Source {
            resource: Resource::register(db, ContentType::CatalogSpec, &uri)?,
        };
        if source.resource.is_processed(db)? {
            return Ok(source);
        }
        source.resource.mark_as_processed(db)?;

        let spec = source.resource.content(db)?;
        let spec: specs::Catalog = serde_yaml::from_slice(&spec)?;

        for uri in &spec.import {
            let uri = source.resource.join(db, uri)?;
            let import = Self::register(db, uri.clone()).map_err(|err| Error::At {
                loc: format!("import {:?}", uri),
                detail: Box::new(err),
            })?;
            Resource::register_import(db, source.resource, import.resource)?;
        }

        for (package, version) in spec.node_dependencies.iter() {
            db.prepare_cached("INSERT INTO nodejs_dependencies (package, version) VALUES (?, ?);")?
                .execute(sql_params![package, version])?;
        }

        for spec in &spec.collections {
            Collection::register(db, source, spec).map_err(|err| Error::At {
                loc: format!("collection {:?}", spec.name),
                detail: Box::new(err),
            })?;
        }
        Ok(source)
    }
}

#[cfg(test)]
mod test {
    use super::{
        super::{dump_tables, init_db_schema, open, Source},
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
        Source::register(&db, Url::parse("test://example/main/spec")?)?;

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
