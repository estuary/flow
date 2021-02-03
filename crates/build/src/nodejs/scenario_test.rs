use lazy_static::lazy_static;
use std::path;

use super::generate_package;

lazy_static! {
    static ref MODEL: serde_json::Value =
        serde_yaml::from_slice(include_bytes!("model.yaml")).unwrap();
}

#[test]
fn test_scenario() {
    let sources::Tables {
        collections,
        derivations,
        errors,
        named_schemas,
        npm_dependencies,
        resources,
        schema_docs,
        transforms,
        ..
    } = sources::scenarios::evaluate_fixtures(Default::default(), &MODEL);

    if !errors.is_empty() {
        eprintln!("{:?}", &errors);
        panic!("unexpected errors");
    }

    let intents = generate_package(
        &path::Path::new("/package"),
        &collections,
        &derivations,
        &named_schemas,
        &npm_dependencies,
        &resources,
        &schema_docs,
        &transforms,
    )
    .unwrap();

    insta::assert_debug_snapshot!(intents);
}
