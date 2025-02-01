mod common;

const MODEL_YAML: &str = include_str!("transitions.yaml");

#[test]
fn test_updates() {
    let outcome = common::run(MODEL_YAML, "{}");
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_update_but_does_not_exist() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
driver:
  liveCaptures: null
  liveCollections: null
  liveMaterializations: null
  liveTests: null
    "#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_update_collection_becomes_derivation() {
    let outcome = common::run(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/collection: null
  captures: null
  materializations: null
  tests: null

driver:
  liveCaptures: null
  liveCollections:
    the/collection:
      # Source schema must be compatible with the shuffle key.
      schema:
        x-live: pass-through
        type: object
        properties:
          f_one: {type: integer}
          f_two: {type: string}
      derivation: true # For funsies, pretend we're a sourced derivation.
    the/derivation:
      derivation: false # Not yet a derivation.
  liveMaterializations: null
  liveTests: null
        "#,
    );
    // Collection's partition prefix is passed-through but a new
    // Shard ID prefix is created for this publication.
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_live_last_build_id_is_larger_than_current_build_id() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/collection:
      expectPubId: null
    the/derivation:
      expectPubId: null
  captures:
    the/capture:
      expectPubId: null
  materializations:
    the/materialization:
      expectPubId: null
  tests:
    the/test:
      expectPubId: null

driver:
  liveCaptures:
    the/capture:
      lastPubId: "19:19:19:19:19:19:19:19"
      lastBuildId: "99:99:99:99:99:99:99:99"
  liveCollections:
    the/collection:
      lastPubId: "19:19:19:19:19:19:19:19"
      lastBuildId: "99:99:99:99:99:99:99:99"
    the/derivation:
      lastPubId: "19:19:19:19:19:19:19:19"
      lastBuildId: "99:99:99:99:99:99:99:99"
  liveMaterializations:
    the/materialization:
      lastPubId: "19:19:19:19:19:19:19:19"
      lastBuildId: "99:99:99:99:99:99:99:99"
  liveTests:
    the/test:
      lastPubId: "19:19:19:19:19:19:19:19"
      lastBuildId: "99:99:99:99:99:99:99:99"
    "#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_live_last_pub_id_is_larger_then_current_pub_id() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/collection:
      expectPubId: null
    the/derivation:
      expectPubId: null
  captures:
    the/capture:
      expectPubId: null
  materializations:
    the/materialization:
      expectPubId: null
  tests:
    the/test:
      expectPubId: null

driver:
  liveCaptures:
    the/capture:
      lastPubId: "90:90:90:90:90:90:90:90"
  liveCollections:
    the/collection:
      lastPubId: "90:90:90:90:90:90:90:90"
    the/derivation:
      lastPubId: "90:90:90:90:90:90:90:90"
  liveMaterializations:
    the/materialization:
      lastPubId: "90:90:90:90:90:90:90:90"
  liveTests:
    the/test:
      lastPubId: "90:90:90:90:90:90:90:90"
    "#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_inserts() {
    let outcome = common::run(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/collection:
      expectPubId: "00:00:00:00:00:00:00:00"
    the/derivation:
      expectPubId: "00:00:00:00:00:00:00:00"
  captures:
    the/capture:
      expectPubId: "00:00:00:00:00:00:00:00"
  materializations:
    the/materialization:
      expectPubId: "00:00:00:00:00:00:00:00"
  tests:
    the/test:
      expectPubId: "00:00:00:00:00:00:00:00"

driver:
  liveCaptures: null
  liveCollections: null
  liveMaterializations: null
  liveTests: null

    "#,
    );
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_insert_but_already_exists() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/collection:
      expectPubId: "00:00:00:00:00:00:00:00"
    the/derivation:
      expectPubId: "00:00:00:00:00:00:00:00"
  captures:
    the/capture:
      expectPubId: "00:00:00:00:00:00:00:00"
  materializations:
    the/materialization:
      expectPubId: "00:00:00:00:00:00:00:00"
  tests:
    the/test:
      expectPubId: "00:00:00:00:00:00:00:00"
    "#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_deletions() {
    let outcome = common::run(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/collection:
      delete: true
    the/derivation:
      delete: true
  captures:
    the/capture:
      delete: true
  materializations:
    the/materialization:
      delete: true
  tests:
    the/test:
      delete: true
    "#,
    );
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_deletion_of_used_collection() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/collection:
      delete: true
    "#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_deletion_but_does_not_exist() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/collection:
      expectPubId: null
      delete: true
    the/derivation:
      expectPubId: null
      delete: true
  captures:
    the/capture:
      expectPubId: null
      delete: true
  materializations:
    the/materialization:
      expectPubId: null
      delete: true
  tests:
    the/test:
      expectPubId: null
      delete: true

driver:
  liveCaptures: null
  liveCollections: null
  liveMaterializations: null
  liveTests: null
    "#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_relaxation_of_existing_exclusions() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  materializations:
    the/materialization:
      bindings:
        - source: the/collection
          resource: { table: bar }
          fields:
            recommended: true
            exclude:
              # Current projection.
              - f_one
              # Pre-existing exclusion which is not an error.
              - existing/not/found
              # Pre-existing exclusion, but its binding is disabled.
              - existing/but/disabled
              # Entirely new exclusion.
              - new/not/found
              # Existing exclusion, but for a different collection.
              - other/not/found
driver:
  liveMaterializations:
    the/materialization:
      bindings:
        - source: the/collection
          resource: ~
          fields:
            recommended: true
            exclude:
              - existing/not/found

        - source: the/collection
          disable: true
          resource: ~
          fields:
            recommended: true
            exclude:
              - existing/but/disabled

        - source: some/other/collection
          resource: ~
          fields:
            recommended: true
            exclude:
              - other/not/found
    "#,
    );

    // Expect `existing/not/found` does NOT produce an error, while others do.
    insta::assert_debug_snapshot!(errors, @r###"
    [
        Error {
            scope: test://example/catalog.yaml#/materializations/the~1materialization/bindings/0/fields/exclude/2,
            error: exclude projection existing/but/disabled does not exist in collection the/collection,
        },
        Error {
            scope: test://example/catalog.yaml#/materializations/the~1materialization/bindings/0/fields/exclude/3,
            error: exclude projection new/not/found does not exist in collection the/collection,
        },
        Error {
            scope: test://example/catalog.yaml#/materializations/the~1materialization/bindings/0/fields/exclude/4,
            error: exclude projection other/not/found does not exist in collection the/collection,
        },
    ]
    "###);
}

#[test]
fn test_cronut_migration_errors() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
driver:
  liveCollections:
    the/collection:
      dataPlaneId: "0e:8e:17:d0:4f:ac:d4:00" # Cronut ID.
    "#,
    );
    insta::assert_debug_snapshot!(errors);
}
