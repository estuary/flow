mod db;
mod regexp_sql_fn;

mod collection;
mod content_type;
mod derivation;
mod error;
mod lambda;
mod nodejs;
mod resource;
mod schema;
mod source;

pub use collection::Collection;
pub use content_type::ContentType;
pub use derivation::Derivation;
pub use error::Error;
pub use lambda::Lambda;
pub use resource::Resource;
pub use rusqlite::{params as sql_params, Connection as DB};
pub use schema::Schema;
pub use source::Source;

pub type Result<T> = std::result::Result<T, Error>;

/// Open a new connection to a catalog database.
pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<DB> {
    let db = DB::open(path)?;
    regexp_sql_fn::install(&db)?; // Install support for REGEXP operator.
    Ok(db)
}

pub use db::init as init_db_schema;
pub use nodejs::build_package as build_nodejs_package;

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
