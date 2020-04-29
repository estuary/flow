pub mod db;
mod regexp_sql_fn;

mod collection;
mod content_type;
mod derivation;
mod error;
mod lambda;
mod resource;
mod schema;
mod source;

// Experimental.
pub mod typescript;

pub use collection::Collection;
pub use content_type::ContentType;
pub use derivation::Derivation;
pub use error::Error;
pub use lambda::Lambda;
pub use resource::Resource;
pub use schema::Schema;
pub use source::Source;

pub type Result<T> = std::result::Result<T, Error>;

use rusqlite::Connection as DB;
use url;

pub fn build_catalog(db: &DB, uri: url::Url) -> Result<()> {
    db::init(db)?;
    Source::register(db, uri)?;
    Ok(())
}

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
