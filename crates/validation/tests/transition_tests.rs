mod common;

const MODEL_YAML: &str = include_str!("transitions.yaml");

#[test]
fn test_updates() {
    let outcome = common::run(MODEL_YAML, "{}");
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_updates_with_clobbered_meta_path() {
    let outcome = common::run(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  captures:
    the/capture:
      bindings:
        - target: the/collection
          # Omit /_meta/path. Expect inactive bindings are still correct.
          resource: { table: foo }

  materializations:
    the/materialization:
      bindings:
        - source: the/collection
          # Omit /_meta/path. Expect inactive bindings are still correct.
          resource: { table: bar }
    "#,
    );
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_updates_with_clobbered_backfill_counter() {
    let common::Outcome {
        built_captures,
        built_collections,
        built_materializations,
        errors,
        ..
    } = common::run(
        MODEL_YAML,
        r#"

test://example/catalog.yaml:
  materializations:
    the/materialization:
      bindings:
        # Simplify to remove spurious additional errors.
        - source: the/collection
          resource: { _meta: { path: [table, path] } }

driver:
  liveCaptures:
    the/capture:
      bindings:
        - target: the/collection
          resource: { _meta: { path: [capture, path] } }
          backfill: 123

  liveCollections:
    the/derivation:
      derive:
        transforms:
          - name: fromCollection
            source: the/collection
            shuffle: any
            backfill: 456

  liveMaterializations:
    the/materialization:
      bindings:
        - source: the/collection
          resource: { _meta: { path: [table, path] } }
          backfill: 789
    "#,
    );

    let captures = built_captures
        .into_iter()
        .map(|row| serde_json::json!([&row.model, row.model_fixes]));

    let collections = built_collections
        .into_iter()
        .map(|row| serde_json::json!([&row.model, row.model_fixes]));

    let materializations = built_materializations
        .into_iter()
        .map(|row| serde_json::json!([&row.model, row.model_fixes]));

    let errors = errors
        .into_iter()
        .map(|err| serde_json::json!(format!("{:?}", err.error)));

    insta::assert_json_snapshot!(captures
        .chain(collections)
        .chain(materializations)
        .chain(errors)
        .collect::<Vec<_>>());
}

#[test]
fn test_change_collection_key_and_partitions() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/collection:
      key: [/f_two, /f_one]
      projections:
        F1:
          location: /f_one
          partition: true
    "#,
    );
    insta::assert_debug_snapshot!(errors);

    // Again, but this time: also reset the collection.
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/collection:
      key: [/f_two, /f_one]
      projections:
        F1:
          location: /f_one
          partition: true
      reset: true
    "#,
    );
    assert!(errors.is_empty());
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
    the/derivation:
      derive: null # Not yet a derivation.
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
      projections:
        FX: null
        FY: null
    the/derivation:
      expectPubId: "00:00:00:00:00:00:00:00"
  captures:
    the/capture:
      expectPubId: "00:00:00:00:00:00:00:00"
  materializations:
    the/materialization:
      expectPubId: "00:00:00:00:00:00:00:00"
      bindings:
        - source: the/collection
          resource: { schema: table, name: path }
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
    let outcome = common::run(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/collection:
      delete: true
  tests: null

driver:
  captures:
    the/capture:
      bindings: []
  derivations:
    the/derivation:
      shuffleKeyTypes: []
      transforms: []
  materializations:
    the/materialization:
      bindings: []
"#,
    );
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_deletion_of_used_collection_when_disabled() {
    let outcome = common::run(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/collection:
      delete: true

    the/derivation:
      derive:
        shards: {disable: true}

  captures:
    the/capture:
      shards: {disable: true}

  materializations:
    the/materialization:
      shards: {disable: true}

  tests: null

driver:
  captures:
    the/capture:
      bindings: []
  derivations:
    the/derivation:
      shuffleKeyTypes: []
      transforms: []
  materializations:
    the/materialization:
      bindings: []
"#,
    );
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_reset_of_used_collection() {
    let outcome = common::run(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/collection:
      reset: true
"#,
    );
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_disable_live_bindings() {
    let outcome = common::run(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/derivation:
      derive:
        transforms:
          - name: fromCollection
            source: { name: the/collection }
            shuffle: any
            lambda: the lambda
            disable: true
  captures:
    the/capture:
      bindings:
        - target: the/collection
          resource: {_meta: { path: [capture, path] }}
          disable: true
  materializations:
    the/materialization:
      bindings:
        - source: the/collection
          resource: {_meta: { path: [table, path] }}
          disable: true

driver:
  captures:
    the/capture:
      bindings: []
  derivations:
    the/derivation:
      shuffleKeyTypes: []
      transforms: []
  materializations:
    the/materialization:
      bindings: []
"#,
    );
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_disable_shards() {
    let outcome = common::run(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/derivation:
      derive:
        shards: {disable: true}
  captures:
    the/capture:
      shards: {disable: true}
  materializations:
    the/materialization:
      shards: {disable: true}

driver:
  captures: null
  derivations: null
  materializations: null
"#,
    );
    insta::assert_debug_snapshot!(outcome);
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

#[test]
fn test_group_by_migration() {
    let outcome = common::run(
        MODEL_YAML,
        r#"
driver:
  liveMaterializations:
    the/materialization:
      lastFields:
        - keys: [F1] # Not f_one, which is canonical.
    "#,
    );
    insta::assert_debug_snapshot!((
        &outcome.built_materializations[0].model,
        &outcome.built_materializations[0].model_fixes
    ));
}

#[test]
fn test_manual_redact_salt_override() {
    // Test that manually specified redact_salt overrides existing salt
    let outcome = common::run(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  captures:
    the/capture:
      # Manually specify a redact salt (base64 encoded)
      redactSalt: bWFudWFsLWNhcHR1cmUtc2FsdA==

  collections:
    the/derivation:
      derive:
        redactSalt: bWFudWFsLWRlcml2YXRpb24tc2FsdA==
    "#,
    );

    // Verify that the manual salts are used in the built specs
    let capture_salt = &outcome.built_captures[0].spec.as_ref().unwrap().redact_salt;
    let derivation_salt = &outcome.built_collections[1]
        .spec
        .as_ref()
        .unwrap()
        .derivation
        .as_ref()
        .unwrap()
        .redact_salt;

    assert_eq!(capture_salt.as_ref(), b"manual-capture-salt");
    assert_eq!(derivation_salt.as_ref(), b"manual-derivation-salt");

    insta::assert_debug_snapshot!(outcome);
}
