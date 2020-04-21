use super::{Error, Result};
use rusqlite::{Connection as DB, Error as DBError};
use url::Url;

/// Resource within the catalog: a file of some kind
/// that's addressable via an associated and canonical URI.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct Resource {
    /// Assigned ID of this resource.
    pub id: i64,
    /// If interned, whether the resource was just added.
    pub added: bool,
}

impl Resource {
    /// Register a resource URI to the catalog, if not already known.
    /// Any fragment component of the URI is discarded before indexing.
    pub fn register(db: &DB, mut uri: Url) -> Result<Resource> {
        uri.set_fragment(None);

        // Intern the URI, if it hasn't been already.
        let mut s =
            db.prepare_cached("INSERT INTO resources (uri) VALUES (?) ON CONFLICT DO NOTHING;")?;
        let added = s.execute(&[&uri])? == 1;

        // Fetch the assigned resource ID.
        let mut s = db.prepare_cached("SELECT id FROM resources WHERE uri = ?;")?;
        let id = s.query_row(&[&uri], |row| row.get(0))?;

        Ok(Resource { id, added })
    }

    /// Fetch the URI of this resource.
    pub fn uri(&self, db: &DB) -> Result<Url> {
        let mut s = db.prepare_cached("SELECT uri FROM resources WHERE id = ?;")?;
        let base: String = s.query_row(&[self.id], |row| row.get(0))?;
        Ok(url::Url::parse(&base)?)
    }

    /// Join a relative URI using this Resource as its base URI.
    pub fn join(&self, db: &DB, relative_uri: &str) -> Result<Url> {
        let base = self.uri(db)?;
        match base.join(relative_uri) {
            Ok(url) => Ok(url),
            Err(detail) => Err(Error::URLJoinErr {
                relative: relative_uri.to_owned(),
                base,
                detail,
            }),
        }
    }

    /// Attempt to fetch the contents of this resource into a string.
    /// This is only expected to work when building the catalog.
    /// After that, resources may not be reachable and the catalog is
    /// expected to be fully self-contained.
    pub fn fetch_to_string(&self, db: &DB) -> Result<String> {
        let uri = self.uri(db)?;
        match uri.scheme() {
            "file" => {
                let path = uri
                    .to_file_path()
                    .map_err(|_| Error::FetchErr(uri.clone()))?;
                Ok(std::fs::read_to_string(path)?)
            }
            _ => Err(Error::FetchErr(uri)),
        }
    }

    /// Registers an import of Resource |import| by |source|.
    pub fn register_import(db: &DB, source: Resource, import: Resource) -> Result<()> {
        if source.id == import.id {
            return Ok(()); // A source implicitly imports itself.
        }
        // Don't fail if this import already exists.
        // Note that a CHECK constraint ensures a cycle cannot be created.
        let mut s = db.prepare_cached(
            "INSERT INTO resource_imports (source_id, import_id)
                    VALUES (?, ?) ON CONFLICT DO NOTHING;",
        )?;
        s.execute(&[source.id, import.id])?;

        Ok(())
    }

    /// Verify that a transitive import path from |source| to |import| exists.
    pub fn verify_import(db: &DB, source: Resource, import: Resource) -> Result<()> {
        let mut s = db.prepare_cached(
            "SELECT 1 FROM resource_transitive_imports
                    WHERE source_id = ? AND import_id = ?;",
        )?;

        match s.query_row(&[source.id, import.id], |_| Ok(())) {
            Ok(()) => Ok(()),
            Err(DBError::QueryReturnedNoRows) => Err(Error::MissingImport {
                source_uri: source.uri(db)?.into_string(),
                import_uri: import.uri(db)?.into_string(),
            }),
            Err(e) => Err(Error::SQLiteErr(e)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{super::db, *};
    use serde_json::json;
    use std::io::Write;
    use tempfile;

    #[test]
    fn test_interning() -> Result<()> {
        let db = DB::open_in_memory()?;
        db::init(&db)?;

        let u1 = Url::parse("file:///1")?;
        let u2 = Url::parse("https://host/path?query")?;
        let u3 = Url::parse("file:///1#ignored")?;

        let r = Resource::register(&db, u1.clone())?;
        assert_eq!(r, Resource { id: 1, added: true });
        assert_eq!(r.uri(&db)?, u1); // Expect it fetches back to it's URL.

        let r = Resource::register(&db, u2.clone())?;
        assert_eq!(r, Resource { id: 2, added: true });
        assert_eq!(r.uri(&db)?, u2);

        let r = Resource::register(&db, u3.clone())?;
        assert!(!r.added);
        assert_eq!(r.uri(&db)?, u1);

        assert_eq!(
            db::dump_table(&db, "resources")?,
            json!([(1, "file:///1"), (2, "https://host/path?query")])
        );
        Ok(())
    }

    #[test]
    fn test_joining() -> Result<()> {
        let db = DB::open_in_memory()?;
        db::init(&db)?;
        let r = Resource::register(&db, Url::parse("file:///a/dir/base.path")?)?;

        // Case: join with a relative file.
        assert_eq!(
            r.join(&db, "other/file.path")?,
            Url::parse("file:///a/dir/other/file.path")?
        );
        // Case: join with a relative file in parent directory.
        assert_eq!(
            r.join(&db, "../sibling/file.path")?,
            Url::parse("file:///a/sibling/file.path")?
        );
        // Case: join with a URI which is it's own base.
        assert_eq!(
            r.join(&db, "http://example/file.path")?,
            Url::parse("http://example/file.path")?
        );
        Ok(())
    }

    #[test]
    fn test_fetch_file() -> Result<()> {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.as_file_mut().write("hello!".as_bytes()).unwrap();

        let uri = Url::from_file_path(file.path()).unwrap();

        let db = DB::open_in_memory()?;
        db::init(&db)?;

        let r = Resource::register(&db, uri)?;
        assert_eq!(r.fetch_to_string(&db)?, "hello!");
        Ok(())
    }

    #[test]
    fn test_import_tracking() -> Result<()> {
        let db = DB::open_in_memory()?;
        db::init(&db)?;

        let a = Resource::register(&db, Url::parse("file:///a")?)?;
        let b = Resource::register(&db, Url::parse("https://b")?)?;
        let c = Resource::register(&db, Url::parse("file:///c")?)?;
        let d = Resource::register(&db, Url::parse("http://d")?)?;

        // A resource may implicitly reference itself (only).
        Resource::verify_import(&db, a, a)?;
        assert_eq!(
            "\"file:///a\" references \"http://d/\" without directly or indirectly importing it",
            format!("{}", Resource::verify_import(&db, a, d).unwrap_err())
        );

        // Marking a self-import is a no-op (and doesn't break CTE evaluation).
        Resource::register_import(&db, a, a)?;

        // A => B => D.
        Resource::register_import(&db, a, b)?;
        Resource::register_import(&db, b, d)?;
        Resource::verify_import(&db, a, d)?;

        // It's not okay for D => A (since A => B => D).
        assert_eq!(
            "catalog database error: Import creates an cycle (imports must be acyclic)",
            format!("{}", Resource::register_import(&db, d, a).unwrap_err()));
        // Or for D => B (since B => D).
        Resource::register_import(&db, d, b).unwrap_err();

        // Imports may form a DAG. Create some valid alternate paths.
        // A => C => D.
        Resource::register_import(&db, c, d)?;
        Resource::register_import(&db, a, c)?;
        // A => B => C => D.
        Resource::register_import(&db, b, c)?;
        // A => D.
        Resource::register_import(&db, a, d)?;

        Resource::verify_import(&db, a, b)?;
        Resource::verify_import(&db, a, c)?;
        Resource::verify_import(&db, a, d)?;
        Resource::verify_import(&db, b, d)?;
        Resource::verify_import(&db, b, c)?;
        Resource::verify_import(&db, c, d)?;

        // C still does not import B, however.
        assert_eq!(
            "\"file:///c\" references \"https://b/\" without directly or indirectly importing it",
            format!("{}", Resource::verify_import(&db, c, b).unwrap_err())
        );

        assert_eq!(
            db::dump_tables(&db, &["resources", "resource_imports"])?,
            json!({
                "resources": [
                (1, "file:///a"),
                (2, "https://b/"),
                (3, "file:///c"),
                (4, "http://d/"),
            ],
                "resource_imports": [
                (1, 2), // A => B.
                (2, 4), // B => D.
                (3, 4), // C => D.
                (1, 3), // A => C.
                (2, 3), // B => C.
                (1, 4), // A => D.
            ],
            }),
        );

        Ok(())
    }
}
