mod db;
mod regexp_sql_fn;
mod unicode_collation;

mod collection;
mod content_type;
mod derivation;
mod error;
mod lambda;
mod materialization;
mod nodejs;
mod projections;
mod resource;
mod schema;
mod scope;
mod source;

use std::path::Path;
use url::Url;

pub use catalog::Source;
pub use collection::Collection;
pub use content_type::ContentType;
pub use derivation::Derivation;
pub use error::Error;
pub use lambda::Lambda;
pub use materialization::{Materialization, ProjectionsError};
pub use resource::Resource;
pub use rusqlite::{params as sql_params, Connection as DB};
pub use schema::Schema;
pub use scope::Scope;
pub use source::Source;

pub type Result<T> = std::result::Result<T, Error>;

/// Open a new connection to a catalog database.
pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<DB> {
    let db = DB::open(path)?;
    regexp_sql_fn::install(&db)?; // Install support for REGEXP operator.
    unicode_collation::install(&db)?;
    Ok(db)
}

pub use db::init as init_db_schema;
pub use nodejs::build_package as build_nodejs_package;

const BUILD_COMPLETE_DESCRIPTION: &str = "completed catalog build";

/// Builds a catalog
pub fn build(db: &DB, spec_url: Url, nodejs_dir: &Path) -> Result<()> {
    db.execute_batch("BEGIN;")?;
    init_db_schema(db)?;

    Source::register(Scope::empty(db), spec_url)?;
    build_nodejs_package(db, nodejs_dir)?;
    db.execute(
        "INSERT INTO build_info (description) VALUES (?);",
        rusqlite::params![BUILD_COMPLETE_DESCRIPTION],
    )?;

    db.execute_batch("COMMIT;")?;
    Ok(())
}

/// Returns true if the database `build_info` table indicates that a build was completed.
/// We don't do any sort of sophisticated up-to-date checks, since the ability to skip re-building
/// the database requires an explicit opt-in.
pub fn database_is_built(db: &DB) -> bool {
    let sql = "SELECT time FROM build_info WHERE description = ?";
    if let Ok(mut stmt) = db.prepare(sql) {
        let params = rusqlite::params![BUILD_COMPLETE_DESCRIPTION];
        if let Ok(timestamp) = stmt.query_row(params, |r| r.get::<usize, String>(0)) {
            log::debug!("database was built at: {}", timestamp);
            return true;
        }
    }
    false
}

// Not public; used for testing within sub-modules.
#[cfg(test)]
use db::test::{dump_table, dump_tables};

#[cfg(test)]
mod test {
    use std::env;
    use std::path::PathBuf;
    use std::process::Command;

    #[test]
    fn run_catalog_test() {
        let mut path = PathBuf::from(&env::var("CARGO_MANIFEST_DIR").unwrap());
        path.extend(&["src", "catalog", "test_catalog.sh"]);

        let status = Command::new(path.as_os_str())
            .spawn()
            .expect("failed to start test_catalog.sh")
            .wait()
            .expect("failed to wait for command");

        assert!(status.success());
    }
}
