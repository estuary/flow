use estuary::catalog;
use estuary_protocol::flow::CollectionSpec;
use pretty_env_logger;
use std::{env, path::PathBuf};
use url::Url;

#[test]
fn test_examples() {
    pretty_env_logger::init();

    let root_dir = PathBuf::from(&env::var("CARGO_MANIFEST_DIR").unwrap());
    let catalog_path = root_dir.join("examples/flow.yaml");
    let nodejs_dir = root_dir.join("target/nodejs/");

    let db = catalog::create(":memory:").unwrap();
    let url = Url::from_file_path(&catalog_path).unwrap();
    catalog::build(&db, url, &nodejs_dir).expect("failed to build catalog");

    // Now we'll re-build the example, using the first catalog as the source.
    let dest = catalog::create(":memory:").unwrap();
    catalog::build_from_catalog(&dest, &db, &nodejs_dir).expect("failed to re-build catalog");

    // Comparing CollectionSpecs seemed like a reasonable way to assert that the two catalogs are
    // functionally the same, since CollectionSpec rolls up a lot of different tables.
    let a_collections = query_collections(&db);
    let b_collections = query_collections(&dest);
    assert_eq!(a_collections.len(), b_collections.len());
    for (a, b) in a_collections.into_iter().zip(b_collections) {
        assert_eq!(a, b);
    }
}

fn query_collections(db: &catalog::DB) -> Vec<CollectionSpec> {
    let mut stmt = db
        .prepare("SELECT spec_json FROM collections_json;")
        .unwrap();
    let result = stmt
        .query(rusqlite::NO_PARAMS)
        .expect("executing query")
        .mapped(|row| {
            let json = row.get::<usize, String>(0)?;
            match serde_json::from_str::<CollectionSpec>(json.as_str()) {
                Ok(coll) => Ok(coll),
                Err(err) => panic!(
                    "Failed to deserialize collection spec: {} \ninvalid json:\n{}",
                    err, json
                ),
            }
        })
        .collect::<Result<Vec<CollectionSpec>, rusqlite::Error>>();
    result.expect("Failed to query collections")
}
