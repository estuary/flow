use super::{specs, ContentType, Resource, Result, Scope};
use url::Url;

/// Lambda represents a Lambda function of the catalog.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct Lambda {
    pub id: i64,
}

impl Lambda {
    /// Register a Lambda with the catalog.
    pub fn register(scope: Scope, spec: &specs::Lambda) -> Result<Lambda> {
        let db = scope.db;
        match spec {
            specs::Lambda::Remote(endpoint) => {
                Url::parse(endpoint)?; // Must be a base URI.

                db.prepare_cached("INSERT INTO lambdas (runtime, inline) VALUES ('remote', ?)")?
                    .execute(&[endpoint])?;
            }
            specs::Lambda::Sqlite(body) => {
                db.prepare_cached("INSERT INTO lambdas (runtime, inline) VALUES ('sqlite', ?)")?
                    .execute(&[body])?;
            }
            specs::Lambda::NodeJS(body) => {
                db.prepare_cached("INSERT INTO lambdas (runtime, inline) VALUES ('nodeJS', ?)")?
                    .execute(&[body])?;
            }
            specs::Lambda::SqliteFile(url) => {
                let url = scope.resource().join(scope.db, url.as_ref())?;
                let import = Resource::register(scope.db, ContentType::Sql, &url)?;
                Resource::register_import(scope, import)?;

                db.prepare_cached(
                    "INSERT INTO lambdas (runtime, resource_id) VALUES ('sqliteFile', ?)",
                )?
                .execute(&[import.id])?;
            }
        };

        Ok(Lambda {
            id: db.last_insert_rowid(),
        })
    }
}

#[cfg(test)]
mod test {
    use super::{
        super::{create, dump_tables},
        *,
    };
    use serde_json::{json, Value};

    #[test]
    fn test_register() -> Result<()> {
        let db = create(":memory:")?;

        db.execute_batch(
            "
            INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                (1, 'application/vnd.estuary.dev-catalog-spec+yaml', 'root spec', true),
                (2, 'application/sql', 'sql content', false);
            INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                (1, 'http://example/path/spec.yaml', TRUE),
                (2, 'http://example/sibling/some.sql', TRUE);
        ",
        )?;

        let root = Url::parse("http://example/path/spec.yaml")?;
        let root = Resource::register(&db, ContentType::CatalogSpec, &root)?;

        use specs::Lambda::*;
        let fixtures = [
            Sqlite("block 1".to_owned()),
            NodeJS("block 2".to_owned()),
            Remote("http://example/remote?query".to_owned()),
            SqliteFile("../sibling/some.sql".into()),
        ];

        for (index, fixture) in fixtures.iter().enumerate() {
            Scope::empty(&db)
                .push_resource(root)
                .push_item(index)
                .then(|scope| Lambda::register(scope, fixture))?;
        }

        assert_eq!(
            dump_tables(&db, &["resource_imports", "lambdas"])?,
            json!({
                "lambdas": [
                    [1, "sqlite", "block 1", Value::Null],
                    [2, "nodeJS", "block 2", Value::Null],
                    [3, "remote", "http://example/remote?query", Value::Null],
                    [4, "sqliteFile", Value::Null, 2],
                ],
                "resource_imports": [[1, 2]],
            }),
        );
        Ok(())
    }
}
