use super::{sql_params, ContentType, Error, Result, Scope, DB};
use rusqlite::Error as DBError;
use url::Url;

/// Resource within the catalog: a file of some kind
/// that's addressable via an associated and canonical URI.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct Resource {
    /// Assigned ID of this resource.
    pub id: i64,
}

impl Resource {
    pub fn get_by_url(db: &DB, ct: ContentType, url: &Url) -> Result<Option<Resource>> {
        // Look for an existing resource row.
        let mut stmt = db.prepare_cached(
            "SELECT resource_id, content_type
                    FROM resources NATURAL JOIN resource_urls WHERE url = ?",
        )?;
        let mut row = stmt.query(&[url])?;

        // Return an existing row, after asserting content types are the same.
        if let Some(row) = row.next()? {
            if ct != row.get(1)? {
                Err(Error::ContentTypeMismatch {
                    next: ct,
                    prev: row.get(1)?,
                })
            } else {
                Ok(Some(Resource { id: row.get(0)? }))
            }
        } else {
            Ok(None)
        }
    }

    pub fn register_content(
        db: &DB,
        ct: ContentType,
        url: &Url,
        content: &[u8],
    ) -> Result<Resource> {
        if let Some(resource) = Resource::get_by_url(db, ct, url)? {
            return Ok(resource);
        }
        Resource::register_content_unchecked(db, ct, url, content)
    }

    fn register_content_unchecked(
        db: &DB,
        ct: ContentType,
        url: &Url,
        content: &[u8],
    ) -> Result<Resource> {
        db.prepare_cached(
            "INSERT INTO resources (content_type, content, is_processed)
                    VALUES (?, ?, FALSE)",
        )?
        .execute(sql_params![ct, content])?;

        let id = db.last_insert_rowid();
        db.prepare_cached(
            "INSERT INTO resource_urls (resource_id, url, is_primary)
                    VALUES (?, ?, TRUE)",
        )?
        .execute(sql_params![id, url])?;

        Ok(Resource { id })
    }

    /// Register a resource with URL and Content-Type to the catalog, if not already known.
    pub fn register(db: &DB, ct: ContentType, url: &Url) -> Result<Resource> {
        if let Some(resource) = Resource::get_by_url(db, ct, url)? {
            return Ok(resource);
        }
        let content = Self::fetch(url).map_err(|e| Error::Fetch {
            url: url.clone(),
            detail: Box::new(e),
        })?;
        Resource::register_content_unchecked(db, ct, url, content.as_slice())
    }

    fn fetch(url: &Url) -> Result<Vec<u8>> {
        log::info!("fetching resource {:?}", url);
        match url.scheme() {
            "file" => {
                let path = url.to_file_path().map_err(|_| Error::FetchNotSupported)?;
                Ok(std::fs::read(path)?)
            }
            "http" | "https" => Ok(reqwest::blocking::get(url.as_str())?.bytes()?.to_vec()),
            _ => Err(Error::FetchNotSupported),
        }
    }

    /// Register an alternate URL under which this Resource may be accessed.
    pub fn register_alternate_url(&self, db: &DB, url: &Url) -> Result<()> {
        // Silently ignore if this URL is already registered with this resource.
        // We don't ON CONFLICT IGNORE because we want to raise UNIQUE violations
        // (eg, if the URL is registered with another Resource).
        let mut stmt =
            db.prepare_cached("SELECT 1 FROM resource_urls WHERE resource_id = ? AND url = ?")?;
        let mut rows = stmt.query(sql_params![self.id, url])?;

        if rows.next()?.is_some() {
            return Ok(());
        }

        db.prepare_cached("INSERT INTO resource_urls (resource_id, url) VALUES (?, ?)")?
            .execute(sql_params![self.id, url])?;
        Ok(())
    }

    /// Returns whether the Resource is marked as having been processed.
    pub fn is_processed(&self, db: &DB) -> Result<bool> {
        let v = db
            .prepare_cached("SELECT is_processed FROM resources WHERE resource_id = ?")?
            .query_row(&[self.id], |r| r.get(0))?;
        Ok(v)
    }
    /// Marks the Resource as having been processed.
    pub fn mark_as_processed(&self, db: &DB) -> Result<()> {
        db.prepare_cached("UPDATE resources SET is_processed = TRUE WHERE resource_id = ?")?
            .execute(&[self.id])?;
        Ok(())
    }

    /// Retrieve raw content of this resource.
    pub fn content(&self, db: &DB) -> Result<Vec<u8>> {
        let b: Vec<u8> = db
            .prepare_cached("SELECT content FROM resources WHERE resource_id = ?;")?
            .query_row(&[self.id], |row| row.get(0))?;
        Ok(b)
    }

    /// Retrieve the primary URL of this Resource.
    pub fn primary_url(&self, db: &DB) -> Result<Url> {
        let url: Url = db
            .prepare_cached("SELECT url FROM resource_urls WHERE resource_id = ? AND is_primary;")?
            .query_row(&[self.id], |row| row.get(0))?;
        Ok(url)
    }

    /// Join a relative URL using this primary URL of this Resource as its base.
    pub fn join(&self, db: &DB, relative_uri: &str) -> Result<Url> {
        let base = self.primary_url(db)?;
        match base.join(relative_uri) {
            Ok(url) => Ok(url),
            Err(detail) => Err(Error::URLJoinErr {
                relative: relative_uri.to_owned(),
                base,
                detail,
            }),
        }
    }

    /// Registers an import of Resource |import| by the Scope's Resource.
    pub fn register_import(scope: Scope, import: Resource) -> Result<()> {
        if scope.resource().id == import.id {
            return Ok(()); // A resource implicitly imports itself.
        }
        let mut s = scope.db.prepare_cached(
            "INSERT INTO resource_imports (resource_id, import_id)
                    VALUES (?, ?) ON CONFLICT DO NOTHING;",
        )?;
        s.execute(&[scope.resource().id, import.id])?;

        Ok(())
    }

    /// Verify that a transitive import path from |source| to |import| exists.
    pub fn verify_import(scope: Scope, import: Resource) -> Result<()> {
        let result = scope.db
            .prepare_cached(
                "SELECT 1 FROM resource_transitive_imports WHERE resource_id = ? AND import_id = ?;")?
            .query_row(&[scope.resource().id, import.id], |_| Ok(()));
        match result {
            Ok(()) => Ok(()),
            Err(DBError::QueryReturnedNoRows) => Err(Error::MissingImport {
                source_uri: scope.resource().primary_url(&scope.db)?.into_string(),
                import_uri: import.primary_url(&scope.db)?.into_string(),
            }),
            Err(err) => Err(err.into()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{
        super::{create, dump_table},
        *,
    };
    use serde_json::{json, Value};
    use std::io::Write;
    use tempfile;

    #[test]
    fn test_registration_does_not_exist() -> Result<()> {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.as_file_mut().write(b"file content!").unwrap();
        let file_url = Url::from_file_path(file.path()).unwrap();

        let db = create(":memory:")?;

        let r = Resource::register(&db, ContentType::CatalogSpec, &file_url)?;
        assert_eq!(r, Resource { id: 1 });
        assert!(!r.is_processed(&db)?);

        // A duplicate registration returns the same Resource.
        let r = Resource::register(&db, ContentType::CatalogSpec, &file_url)?;
        assert_eq!(r, Resource { id: 1 });

        assert_eq!(r.primary_url(&db)?, file_url);
        assert_eq!(&r.content(&db)?, b"file content!");

        assert_eq!(
            dump_table(&db, "resources")?,
            json!([(1, ContentType::CatalogSpec.as_str(), "file content!", false)]),
        );
        assert_eq!(
            dump_table(&db, "resource_urls")?,
            json!([(1, file_url.into_string(), true)]),
        );
        Ok(())
    }

    #[test]
    fn test_registration_unprocessed() -> Result<()> {
        let db = create(":memory:")?;

        db.execute_batch(
            "
            INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                (42, 'application/schema+yaml', CAST('content!' AS BLOB), FALSE);
            INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                (42, 'test://a/path', TRUE);
        ",
        )?;

        let u = Url::parse("test://a/path")?;
        let r = Resource::register(&db, ContentType::Schema, &u)?;
        assert_eq!(r, Resource { id: 42 });
        assert!(!r.is_processed(&db)?);
        assert_eq!(&r.content(&db)?, b"content!");

        r.mark_as_processed(&db)?;
        assert!(r.is_processed(&db)?);

        // Expect row was updated.
        assert_eq!(
            dump_table(&db, "resources")?,
            json!([(42, ContentType::Schema.as_str(), "content!", true)]),
        );

        Ok(())
    }

    #[test]
    fn test_registration_via_alt_url() -> Result<()> {
        let db = create(":memory:")?;

        db.execute_batch(
            "
            INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                (42, 'application/schema+yaml', X'1234', TRUE);
            INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                (42, 'test://primary/path', TRUE),
                (42, 'test://alt/path/one', NULL),
                (42, 'test://alt/path/two', NULL);
        ",
        )?;

        let u = Url::parse("test://alt/path/two")?;
        let r = Resource::register(&db, ContentType::Schema, &u)?;
        assert_eq!(r, Resource { id: 42 });
        assert!(r.is_processed(&db)?);
        assert_eq!(r.primary_url(&db)?, Url::parse("test://primary/path")?);

        Ok(())
    }

    #[test]
    fn test_registration_with_different_content_type() -> Result<()> {
        let db = create(":memory:")?;

        db.execute_batch(
            "
            INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                (42, 'application/schema+yaml', X'1234', FALSE);
            INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                (42, 'test://a/path', TRUE);
        ",
        )?;

        let u = Url::parse("test://a/path")?;
        let r = Resource::register(&db, ContentType::Sql, &u);

        assert_eq!(format!("{}", r.unwrap_err()),
                   "resource has content-type application/sql, but is already registered with type application/schema+yaml");
        Ok(())
    }

    #[test]
    fn test_register_alternate_urls() -> Result<()> {
        let db = create(":memory:")?;

        db.execute_batch(
            "
            INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                    (42, 'application/schema+yaml', X'1234', FALSE);
            INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                    (42, 'test://a/path', TRUE);
        ",
        )?;
        let r = Resource { id: 42 };

        r.register_alternate_url(&db, &Url::parse("test://alt/path-1")?)?;
        r.register_alternate_url(&db, &Url::parse("test://alt/path-2")?)?;

        assert_eq!(
            dump_table(&db, "resource_urls")?,
            json!([
                (42, "test://a/path", true),
                (42, "test://alt/path-1", Value::Null),
                (42, "test://alt/path-2", Value::Null),
            ]),
        );
        Ok(())
    }

    #[test]
    fn test_joining() -> Result<()> {
        let db = create(":memory:")?;

        db.execute_batch(
            "
            INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                (42, 'application/schema+yaml', X'1234', FALSE);
            INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                (42, 'test://a/dir/and/base.path', TRUE),
                (42, 'test://alt/location', NULL);
        ",
        )?;
        let r = Resource { id: 42 };

        // Case: join with a relative file.
        assert_eq!(
            r.join(&db, "other/file.path")?,
            Url::parse("test://a/dir/and/other/file.path")?
        );
        // Case: join with a relative file in parent directory.
        assert_eq!(
            r.join(&db, "../sibling/file.path")?,
            Url::parse("test://a/dir/sibling/file.path")?
        );
        // Case: join with a URI which is it's own base.
        assert_eq!(
            r.join(&db, "test://example/file.path")?,
            Url::parse("test://example/file.path")?
        );
        Ok(())
    }

    #[test]
    fn test_import_tracking() -> Result<()> {
        let db = create(":memory:")?;

        db.execute_batch(
            "
            INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                (1, 'application/sql', X'', FALSE),
                (2, 'application/sql', X'', FALSE),
                (3, 'application/sql', X'', FALSE),
                (4, 'application/sql', X'', FALSE);
            INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                (1, 'test://a', TRUE),
                (2, 'test://b', TRUE),
                (3, 'test://c', TRUE),
                (4, 'test://d', TRUE);
        ",
        )?;
        let a = Resource { id: 1 };
        let b = Resource { id: 2 };
        let c = Resource { id: 3 };
        let d = Resource { id: 4 };
        let scope = Scope::empty(&db);
        let s_a = scope.push_resource(a);
        let s_b = scope.push_resource(b);
        let s_c = scope.push_resource(c);
        let s_d = scope.push_resource(d);

        // Marking a self-import is a no-op (and doesn't break CTE evaluation).
        Resource::register_import(s_a, a)?;

        // A => B => D.
        Resource::register_import(s_a, b)?;
        Resource::register_import(s_b, d)?;
        Resource::verify_import(s_a, d)?;

        // It's not okay for D => A (since A => B => D).
        assert_eq!(
            "catalog database error: Import creates an cycle (imports must be acyclic)",
            format!("{}", Resource::register_import(s_d, a).unwrap_err())
        );
        // Or for D => B (since B => D).
        Resource::register_import(s_d, b).unwrap_err();

        // Imports may form a DAG. Create some valid alternate paths.
        // A => C => D.
        Resource::register_import(s_c, d)?;
        Resource::register_import(s_a, c)?;
        // A => B => C => D.
        Resource::register_import(s_b, c)?;
        // A => D.
        Resource::register_import(s_a, d)?;

        Resource::verify_import(s_a, b)?;
        Resource::verify_import(s_a, c)?;
        Resource::verify_import(s_a, d)?;
        Resource::verify_import(s_b, d)?;
        Resource::verify_import(s_b, c)?;
        Resource::verify_import(s_c, d)?;

        // C still does not import B, however.
        assert_eq!(
            format!("{}", Resource::verify_import(s_c, b).unwrap_err()),
            "\"test://c\" references \"test://b\" without directly or indirectly importing it",
        );

        assert_eq!(
            dump_table(&db, "resource_imports")?,
            json!([
                (1, 2), // A => B.
                (2, 4), // B => D.
                (3, 4), // C => D.
                (1, 3), // A => C.
                (2, 3), // B => C.
                (1, 4), // A => D.
            ]),
        );

        Ok(())
    }
}
