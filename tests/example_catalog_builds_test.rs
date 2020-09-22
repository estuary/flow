use estuary::catalog;
use pretty_env_logger;
use std::{env, path::PathBuf};
use url::Url;

#[test]
fn test_examples() {
    pretty_env_logger::init();

    let root_dir = PathBuf::from(&env::var("CARGO_MANIFEST_DIR").unwrap());
    let catalog_path = root_dir.join("examples/catalog.yaml");
    let nodejs_dir = root_dir.join("target/nodejs/");

    let db = catalog::create(":memory:").unwrap();
    let url = Url::from_file_path(&catalog_path).unwrap();
    catalog::build(&db, url, &nodejs_dir).expect("failed to build catalog");
}
