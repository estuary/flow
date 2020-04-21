use super::{Resource, Result};
use crate::specs::build as specs;
use rusqlite::{params as sql_params, Connection as DB, Error as DBError};
use url::Url;

/// Lambda represents a Lambda function of the catalog.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct Lambda {
    pub id: i64,
    pub resource: Resource,
}

// Constants for runtime types.
static TYPESCRIPT: &str = "typescript";
static SQLITE: &str = "sqlite";
static REMOTE: &str = "remote";

impl Lambda {
    /// Register a Lambda with the catalog.
    pub fn register(db: &DB, res: Resource, spec: &specs::Lambda) -> Result<Lambda> {
        use specs::Lambda::*;
        let (embed, runtime, body) = match spec {
            Remote(endpoint) => {
                Url::parse(endpoint)?; // Must be a base URI.
                (true, REMOTE, endpoint)
            }
            Sqlite(body) => (true, SQLITE, body),
            SqliteFile(uri) => (false, SQLITE, uri),
            Typescript(body) => (true, TYPESCRIPT, body),
            TypescriptFile(uri) => (false, TYPESCRIPT, uri),
        };

        if embed {
            // Embedded lambdas always insert a new row.
            db.prepare_cached("INSERT INTO lambdas (runtime, body, resource_id) VALUES (?, ?, ?)")?
                .execute(sql_params![runtime, body, res.id])?;
            return Ok(Lambda {
                id: db.last_insert_rowid(),
                resource: res,
            });
        }

        // This lambda specification is an indirect to a file.

        let import = res.join(db, body)?;
        let import = Resource::register(db, import)?;
        Resource::register_import(db, res, import)?;

        // Attempt to find an existing row for this lambda.
        // We don't use import.added because *technically* the lambda could be
        // represented twice with different runtimes. This is almost certainly
        // a bug, but here we just represent what the spec says and trust we'll
        // fail later with a better error (i.e., compilation failed).

        let row = db
            .prepare_cached("SELECT id FROM lambdas WHERE runtime = ? AND resource_id = ?")?
            .query_row(sql_params![runtime, import.id], |row| row.get(0));

        match row {
            Ok(id) => {
                return Ok(Lambda {id, resource: import})
            } // Found.
            Err(DBError::QueryReturnedNoRows) => (),  // Not found. Fall through to insert.
            Err(err) => return Err(err.into()), // Other DBError.
        }

        db.prepare_cached("INSERT INTO lambdas (runtime, body, resource_id) VALUES (?, ?, ?)")?
            .execute(sql_params![runtime, import.fetch_to_string(db)?, import.id])?;
        Ok(Lambda {
            id: db.last_insert_rowid(),
            resource: import,
        })
    }
}

#[cfg(test)]
mod test {
    use super::{super::db, *};
    use serde_json::json;
    use std::fs;
    use tempfile;

    #[test]
    fn test_register() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();

        // Two lambda fixture files.
        fs::write(dir.path().join("lambda.one"), "file one")?;
        fs::write(dir.path().join("lambda.two"), "file two")?;

        let db = DB::open_in_memory()?;
        db::init(&db)?;

        let root = Url::from_file_path(dir.path().join("root.spec")).unwrap();
        let root = Resource::register(&db, root)?;
        let file_1 = Resource::register(&db, root.join(&db, "lambda.one")?)?;
        let file_2 = Resource::register(&db, root.join(&db, "lambda.two")?)?;

        use specs::Lambda::*;
        let fixtures = [
            Sqlite("block 1".to_owned()),
            Sqlite("block 2".to_owned()),
            Typescript("block 3".to_owned()),
            Remote("http://host".to_owned()),
            SqliteFile("lambda.one".to_owned()),
            SqliteFile("lambda.one".to_owned()), // De-duplicated repeat.
            TypescriptFile("lambda.two".to_owned()),
            TypescriptFile("lambda.one".to_owned()), // Repeat with different runtime.
        ];

        for fixture in fixtures.iter() {
            Lambda::register(&db, root, fixture)?;
        }

        assert_eq!(
            db::dump_tables(&db, &["resource_imports", "lambdas"])?,
            json!({
                "resource_imports": [[root.id, file_1.id], [root.id, file_2.id]],
                "lambdas": [
                    [1, "sqlite", "block 1", root.id],
                    [2, "sqlite", "block 2", root.id],
                    [3, "typescript", "block 3", root.id],
                    [4, "remote", "http://host", root.id],
                    [5, "sqlite", "file one", file_1.id],
                    [6, "typescript", "file two", file_2.id],
                    [7, "typescript", "file one", file_1.id],
                ],
            }),
        );
        Ok(())
    }
}
