mod capture;
mod collection;
mod content_type;
mod db;
mod derivation;
mod endpoint;
mod error;
mod extraction;
mod inference;
mod lambda;
mod materialization;
mod nodejs;
mod projections;
mod regexp_sql_fn;
mod resource;
mod schema;
mod scope;
mod selector;
mod source;
pub mod specs;
mod str_edit_distance;
mod test_case;
mod unicode_collation;

use std::convert::TryFrom;
use std::path::Path;
use url::Url;

pub use capture::Capture;
pub use collection::Collection;
pub use content_type::ContentType;
pub use derivation::Derivation;
pub use endpoint::Endpoint;
pub use error::{Error, NoSuchEntity};
pub use lambda::Lambda;
pub use materialization::{Materialization, MaterializationTarget};
pub use resource::Resource;
pub use rusqlite::{params as sql_params, Connection as DB};
pub use schema::Schema;
pub use scope::Scope;
pub use selector::Selector;
pub use source::Source;
pub use test_case::TestCase;

pub static FLOW_VERSION: &'static str = env!("CARGO_PKG_VERSION");

pub type Result<T> = std::result::Result<T, Error>;

/// Create a new and empty catalog database, returning an open connection.
/// Any existing database at the given path is truncated.
pub fn create(path: &str) -> Result<DB> {
    if path != ":memory:" {
        // Create or truncate the database at |path|.
        std::fs::write(path, &[])?;
    }
    let c = open_unchecked(path)?;
    c.execute_batch("BEGIN;")?;
    db::init(&c)?;
    c.execute_batch("COMMIT;")?;
    Ok(c)
}

/// Open an existing catalog database, which must already exist and be successfully built by the
/// same version of flowctl.
pub fn open(path: impl AsRef<Path>) -> Result<DB> {
    let db = open_unchecked(path)?;
    ensure_database_is_built(&db)?;
    Ok(db)
}

pub fn open_unchecked(path: impl AsRef<Path>) -> Result<DB> {
    let c = DB::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    regexp_sql_fn::install(&c)?; // Install support for REGEXP operator.
    unicode_collation::install(&c)?; // Overrides NOCASE so it handles unicode
    str_edit_distance::install(&c)?; // add edit_distance scalar function
    Ok(c)
}

pub use nodejs::build_package as build_nodejs_package;

const BUILD_COMPLETE_DESCRIPTION: &str = "completed catalog build";

/// Builds a catalog. This function assumes that the database schema is initialized, and that the
/// database is otherwise empty. The `build_info` table will be populated with messages that act as
/// a coarse-grained progress log. The very last message is always `BUILD_COMPLETE_DESCRIPTION`,
/// which is used to indicate that the catalog is successfully built and can be used with
/// subcommands that require a built database.
pub fn build(db: &DB, spec_url: Url, nodejs_dir: &Path) -> Result<()> {
    db.execute_batch("BEGIN;")?;
    let result = try_build(db, spec_url, nodejs_dir);

    // We'll add the build error description to the catalog on a best-effort basis. This allows us
    // to retain some information on the original error in case we need to debug this catalog.
    if let Err(err) = result.as_ref() {
        let err_message = format!("catalog build failed with error: {:?}", err);

        // Ignore any additional errors here since we already have `err`.
        let _ = db.execute(
            "INSERT INTO build_info (description) VALUES (?);",
            rusqlite::params![err_message],
        );
    }

    db.execute_batch("COMMIT;")?;
    result
}

/// Builds a catalog using the resources from an existing catalog as the source.
pub fn build_from_catalog(dest: &DB, source: &DB, nodejs_dir: &Path) -> Result<()> {
    // will be set to the url of the first resource
    let mut primary_source_url: Option<Url> = None;

    let mut insert_url_stmt = dest
        .prepare("INSERT INTO resource_urls (resource_id, url, is_primary) VALUES (?, ?, TRUE)")?;
    let mut insert_resource_stmt = dest.prepare(
        "INSERT INTO resources (resource_id, content_type, content, is_processed) \
            VALUES (?, ?, ?, FALSE)",
    )?;

    let mut read_stmt = source.prepare(
        "SELECT resource_id, url, content_type, content \
           FROM resource_urls \
           NATURAL JOIN resources \
           WHERE resource_urls.is_primary \
           ORDER BY resource_id;",
    )?;
    let mut resources = read_stmt.query(rusqlite::NO_PARAMS)?;
    let mut resource_count = 0;
    while let Some(row) = resources.next()? {
        let id: i64 = row.get(0)?;
        let url: String = row.get(1)?;
        let content_type: String = row.get(2)?;
        let content: Vec<u8> = row.get(3)?;
        log::debug!(
            "Adding: resource_id: {}, url: '{}', content_type: '{}'",
            id,
            url,
            content_type
        );

        // Sanity check to ensure that this first resource is actually a flow catalog spec. If it's
        // not, then we'll just take the first one that is, or return a `MissingSourceResource`.
        if primary_source_url.is_none()
            && ContentType::try_from(content_type.as_str()).ok() == Some(ContentType::CatalogSpec)
        {
            let parsed: Url = url.parse()?;
            primary_source_url = Some(parsed);
        }

        insert_resource_stmt.execute(rusqlite::params![id, content_type, content])?;
        insert_url_stmt.execute(rusqlite::params![id, url])?;
        resource_count += 1;
    }

    if let Some(source_url) = primary_source_url {
        log::info!(
            "Starting build using {} resources from source catalog",
            resource_count
        );
        try_build(dest, source_url, nodejs_dir)
    } else {
        Err(Error::MissingSourceResource)
    }
}

fn try_build(db: &DB, spec_url: Url, nodejs_dir: &Path) -> Result<()> {
    Source::register(Scope::empty(db), spec_url)?;
    db.execute(
        "INSERT INTO build_info (description) VALUES ('finished registering source');",
        rusqlite::NO_PARAMS,
    )?;

    extraction::verify_extracted_fields(db)?;
    db.execute(
        "INSERT INTO build_info (description) VALUES ('verified extracted fields');",
        rusqlite::NO_PARAMS,
    )?;

    build_nodejs_package(db, nodejs_dir)?;
    db.execute(
        "INSERT INTO build_info (description) VALUES ('built nodejs package');",
        rusqlite::NO_PARAMS,
    )?;

    // This is the very last thing we do, since this particular message is what indicates that a
    // catalog has been fully built.
    db.execute(
        "INSERT INTO build_info (description) VALUES (?);",
        rusqlite::params![BUILD_COMPLETE_DESCRIPTION],
    )?;

    // sanity check to make sure that we've got the proper indicators in place
    if cfg!(debug_assertions) {
        ensure_database_is_built(db).unwrap();
    }
    Ok(())
}

/// Returns `Ok(())` if the database was build was completed successfully by the current version of
/// flowctl. The `build_info` table is queried in order to determine whether the build was
/// completed successfully, and the `flow_version` table is checked to determine the version that
/// completed the build. We don't do any sort of sophisticated up-to-date checks, since the ability
/// to skip re-building the database requires an explicit opt-in.
pub fn ensure_database_is_built(db: &DB) -> Result<()> {
    let build_timestamp = get_build_timestamp(db)?.ok_or(Error::CatalogNotBuilt)?;
    let build_version = get_build_version(db)?;
    log::debug!(
        "catalog was built at: '{}' by flow version: '{}'",
        build_timestamp,
        build_version
    );
    if build_version == FLOW_VERSION {
        Ok(())
    } else {
        Err(Error::CatalogVersionMismatch(build_version))
    }
}

fn get_build_version(db: &DB) -> Result<String> {
    db.query_row(
        "SELECT version FROM flow_version;",
        rusqlite::NO_PARAMS,
        |r| r.get(0),
    )
    .map_err(Into::into)
}

/// Returns the timestamp corresponding to when the catalog was finished building. If the catalog
/// build never completed successfully, this will return None.
fn get_build_timestamp(db: &DB) -> Result<Option<String>> {
    use rusqlite::OptionalExtension;

    let sql = "SELECT time FROM build_info WHERE description = ?";
    let mut stmt = db.prepare(sql)?;
    let params = rusqlite::params![BUILD_COMPLETE_DESCRIPTION];
    let timestamp = stmt
        .query_row(params, |r| r.get::<usize, String>(0))
        .optional()?;
    Ok(timestamp)
}

#[cfg(test)]
fn test_register(spec: &str) -> Result<DB> {
    //fn try_build(db: &DB, spec_url: Url, nodejs_dir: &Path) -> Result<()> {

    let db = create(":memory:")?;
    let uri = Url::parse("test://flow-catalog-test-register/flow.yaml").unwrap();
    let resource =
        Resource::register_content(&db, ContentType::CatalogSpec, &uri, spec.as_bytes())?;
    let scope = Scope {
        db: &db,
        parent: None,
        resource: Some(resource),
        location: json::Location::Root,
    };
    Source::register(scope, uri)?;

    Ok(db)
}

// Not public; used for testing within sub-modules.
#[cfg(test)]
use db::test::{dump_table, dump_tables};

#[cfg(test)]
mod test {
    use super::*;
    use std::env;
    use std::path::PathBuf;
    use std::process::Command;

    #[test]
    fn ensure_database_is_built_returns_ok_if_both_build_info_and_flow_version_are_populated() {
        let db = create(":memory:").unwrap();

        // nothing has been built
        assert!(matches!(
            ensure_database_is_built(&db),
            Err(Error::CatalogNotBuilt)
        ));

        db.execute(
            "INSERT INTO build_info (description) VALUES (?);",
            rusqlite::params![BUILD_COMPLETE_DESCRIPTION],
        )
        .unwrap();

        // Now it should be ok, since the catalog is always initialized with the current version string
        assert!(ensure_database_is_built(&db).is_ok());

        db.execute(
            "update flow_version set version = 'canary' where id = 1;",
            rusqlite::NO_PARAMS,
        )
        .unwrap();

        assert!(matches!(
            ensure_database_is_built(&db),
            Err(Error::CatalogVersionMismatch(v)) if v == "canary"
        ));
    }

    #[test]
    fn run_catalog_test() {
        let mut path = PathBuf::from(&env::var("CARGO_MANIFEST_DIR").unwrap());
        path.extend(&["src", "test_catalog.sh"]);

        let status = Command::new(path.as_os_str())
            .spawn()
            .expect("failed to start test_catalog.sh")
            .wait()
            .expect("failed to wait for command");

        assert!(status.success());
    }
}
