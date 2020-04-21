use estuary::catalog::{build_catalog, Result};
use pretty_env_logger;
use rusqlite::Connection;
use std::{env, path::PathBuf};
use url::Url;

#[test]
fn test_examples() -> Result<()> {
    pretty_env_logger::init();

    let mut path = PathBuf::from(&env::var("CARGO_MANIFEST_DIR").unwrap());
    path.extend(["examples", "root.yaml"].iter());

    let db = Connection::open_in_memory()?;
    build_catalog(&db, Url::from_file_path(path).unwrap())?;

    Ok(())
}
