use estuary::catalog;
use pretty_env_logger;
use std::{env, path::PathBuf};
use url::Url;

#[test]
fn test_examples() -> catalog::Result<()> {
    pretty_env_logger::init();

    let mut path = PathBuf::from(&env::var("CARGO_MANIFEST_DIR").unwrap());
    path.extend(["examples", "catalog.yaml"].iter());

    let db = catalog::open(":memory:")?;
    catalog::init_db_schema(&db)?;

    let url = Url::from_file_path(&path).unwrap();
    catalog::Source::register(catalog::Scope::empty(&db), url)?;

    Ok(())
}
