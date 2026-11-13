mod common;

const MODEL_YAML: &str = include_str!("shared_specs.yaml");

/// A drafted derivation and a drafted test both reference `the/derived`,
/// a live derivation which isn't drafted.
#[test]
fn test_live_derivation_is_shared_without_its_derivation() {
    let outcome = common::run(MODEL_YAML, "{}");
    assert!(outcome.errors.is_empty(), "{:?}", outcome.errors);

    // The built row of the live derivation keeps its derivation,
    // which catalog tests require in order to run it.
    let derived = outcome
        .built_collections
        .get_key(&models::Collection::new("the/derived"))
        .unwrap();
    assert!(tables::BuiltRow::is_passthrough(derived));
    assert!(derived.spec.as_ref().unwrap().derivation.is_some());

    let reader = outcome
        .built_collections
        .get_key(&models::Collection::new("the/reader"))
        .unwrap();
    let reader = reader.spec.as_ref().unwrap().derivation.as_ref().unwrap();

    // Transforms which read it share one copy, which has no derivation.
    let [first, second] = reader.transforms.as_slice() else {
        panic!("expected two transforms");
    };
    let (first, second) = (
        first.collection.as_ref().unwrap(),
        second.collection.as_ref().unwrap(),
    );
    assert_eq!(first.name, "the/derived");
    assert!(first.derivation.is_none());
    assert!(proto_flow::Arc::ptr_eq(first, second));
}
