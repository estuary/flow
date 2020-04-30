use estuary::catalog;
use pretty_env_logger;
use std::{env, path::PathBuf};
use url::Url;

#[test]
fn test_examples() -> catalog::Result<()> {
    pretty_env_logger::init();

    let mut path = PathBuf::from(&env::var("CARGO_MANIFEST_DIR").unwrap());
    path.extend(["examples", "root.yaml"].iter());
    let path = Url::from_file_path(&path).unwrap();

    let db = catalog::open(":memory:")?;
    catalog::init_db_schema(&db)?;
    catalog::Resource::register(&db, catalog::ContentType::CatalogSpec, &path)?;

    Ok(())
}
