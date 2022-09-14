use futures::{future::LocalBoxFuture, FutureExt};
use lazy_static::lazy_static;
use proto_flow::{capture, flow, materialize};
use serde_json::Value;
use std::collections::HashMap;

lazy_static! {
    static ref GOLDEN: Value = serde_yaml::from_slice(include_bytes!("model.yaml")).unwrap();
}

#[test]
fn test_golden_all_visits() {
    let tables = run_test(
        GOLDEN.clone(),
        &flow::build_api::Config {
            build_id: "a-build-id".to_string(),
            ..Default::default()
        },
    );
    insta::assert_debug_snapshot!(tables);
}

#[test]
fn connector_validation_is_skipped_when_shards_are_disabled() {
    let fixture =
        serde_yaml::from_slice(include_bytes!("validation_skipped_when_disabled.yaml")).unwrap();
    let tables = run_test(
        fixture,
        &flow::build_api::Config {
            build_id: "validation-skipped-build-id".to_string(),
            ..Default::default()
        },
    );

    assert!(
        tables.errors.is_empty(),
        "expected no errors, got: {:?}",
        tables.errors
    );
    assert!(tables.built_captures.len() == 1);
    assert_eq!(
        tables.built_captures[0]
            .spec
            .shard_template
            .as_ref()
            .unwrap()
            .disable,
        true
    );
    assert!(tables.built_materializations.len() == 1);
    assert_eq!(
        tables.built_materializations[0]
            .spec
            .shard_template
            .as_ref()
            .unwrap()
            .disable,
        true
    );
}

#[test]
fn test_database_round_trip() {
    let tables = run_test(
        GOLDEN.clone(),
        &flow::build_api::Config {
            build_id: "a-build-id".to_string(),
            ..Default::default()
        },
    );

    // Round-trip source and built tables through the database, verifying equality.
    let db = rusqlite::Connection::open(":memory:").unwrap();
    tables::persist_tables(&db, &tables.as_tables()).unwrap();
    let mut reload_tables = tables::All::default();
    tables::load_tables(&db, reload_tables.as_tables_mut().as_mut_slice()).unwrap();

    let original = format!("{:#?}", tables);
    let recovered = format!("{:#?}", reload_tables);

    if original != recovered {
        std::fs::write("ORIGINAL", original).unwrap();
        std::fs::write("RECOVERED", recovered).unwrap();
        panic!("database round trip: original & restored tables are different! Wrote ORIGINAL & RECOVERED for debugging");
    }
}

#[test]
fn test_golden_error() {
    let errors = run_test_errors(&GOLDEN, "{}");
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_invalid_collection_names_prefixes_and_duplicates() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:
  collections:
    testing/good: &spec
      schema: test://example/int-string.schema
      key: [/int]

    "": *spec
    testing/bad name: *spec
    testing/bad!name: *spec

    # We require a sequence of non-empty tokens, separated by exactly one '/'.
    testing/bad//name: *spec
    testing/bad/name/: *spec
    /testing/bad/name: *spec

    # Invalid prefix of testing/int-string & others.
    testing: *spec

    # Illegal duplicates under naming collation.
    testing/int-sTRinG: *spec
    testing/Int-Halve: *spec
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_invalid_partition_names_and_duplicates() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string:
  collections:
    testing/int-string:
      projections:
        good: &spec
          location: /bit
          partition: true

        "": *spec
        inv alid: *spec
        inv!alid: *spec
        inv/alid: *spec

        # Illegal duplicates under collation.
        INT: /int
        bIt: /bit

        # Attempt to re-map a canonical projection.
        str: /int
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_invalid_remap_of_default_flow_document() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
# Collection int-string uses the default `flow_document` projection of the root.
test://example/int-string.schema:
  type: object
  properties:
    flow_document: { type: boolean }

# Collection int-reverse uses a different `Root` projection of the root.
# We don't expect it to produce an error from an implicit literal
# `flow_document` property.
test://example/int-reverse:
  collections:
    testing/int-reverse:
      schema:
        $ref: test://example/int-string.schema
        properties:
          flow_document: { type: boolean }
      projections:
        Root: ""
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_invalid_transform_names_and_duplicates() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-reverse:
  collections:
    testing/int-reverse:
      derivation:
        transform:
          good: &spec
            source:
              name: testing/int-string
            publish:
              lambda: typescript

          "": *spec
          inv alid: *spec
          inv!alid: *spec
          inv/alid: *spec

          # Illegal duplicate under collation.
          reVeRsEIntString: *spec
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_invalid_capture_names_and_duplicates() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:
  import:
    - test://example/captures
test://example/captures:
  captures:
    good: &spec
      endpoint:
        connector:
          image: an/image
          config:
            bucket: a-bucket
            prefix: and-prefix
      bindings: []

    #"": *spec
    bad name: *spec
    bad!name: *spec

    # We require a sequence of non-empty tokens, separated by exactly one '/'.
    bad//name: *spec
    bad/name/: *spec
    /bad/name: *spec

    # Invalid prefix of testing/some-source.
    testing: *spec

    # Illegal duplicates under naming collation.
    testing/some-source: *spec
    testing/SoMe-source: *spec
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_invalid_materialization_names_and_duplicates() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:
  import:
    - test://example/materializations
test://example/materializations:
  materializations:
    good: &spec
      endpoint:
        connector:
          image: an/image
          config:
            bucket: a-bucket
            prefix: and-prefix
      bindings: []

    #"": *spec
    bad name: *spec
    bad!name: *spec

    # We require a sequence of non-empty tokens, separated by exactly one '/'.
    bad//name: *spec
    bad/name/: *spec
    /bad/name: *spec

    # Invalid prefix of testing/some-source.
    testing: *spec

    # Illegal duplicates under naming collation.
    testing/some-target: *spec
    testing/SoMe-target: *spec
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_invalid_test_names_and_duplicates() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:
  tests:
    testing/good: &spec
      - verify:
          collection: testing/int-string
          documents: []

    "": *spec
    testing/bad name: *spec
    testing/bad!name: *spec

    # We require a sequence of non-empty tokens, separated by exactly one '/'.
    testing/bad//name: *spec
    testing/bad/name/: *spec
    /testing/bad/name: *spec

    # Invalid prefix of testing/test & others.
    testing: *spec

    # Illegal duplicate under naming collation.
    testing/TeSt: *spec
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_cross_entity_name_prefixes_and_duplicates() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:

  collections:
    testing/b/1: &collection_spec
      schema: test://example/int-string.schema
      key: [/int]

    testing/b/3/suffix: *collection_spec
    testing/b/2: *collection_spec

  materializations:
    testing/b/2: &materialization_spec
      endpoint:
        connector:
          image: an/image
          config: { a: config }
      bindings: []

    testing/b/1/suffix: *materialization_spec
    testing/b/3: *materialization_spec
    testing/b/4: *materialization_spec

  captures:
    testing/b/3: &capture_spec
      endpoint:
        connector:
          image: an/image
          config: { a: value }
      bindings: []

    testing/b/1: *capture_spec
    testing/b/2/suffix: *capture_spec
    testing/b/5/suffix: *capture_spec

  tests:
    testing/b/5: &test_spec
      - verify:
          collection: testing/int-string
          documents: []

    testing/b/4/suffix: *test_spec
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_transform_source_not_found() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-halve:
  collections:
    testing/int-halve:
      derivation:
        transform:
          halveIntString:
            source: { name: testinG/Int-String }
          halveSelf:
            source: { name: wildly/off/name }
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_capture_target_not_found() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string-captures:
  captures:
    testing/s3-source:
      bindings:
        - target: testiNg/int-strinK
          resource: { stream: a-stream }
        - target: wildly/off/name
          resource: { stream: v2-stream }
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_capture_target_is_missing_imports() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string-captures:
  import: null
  captures:
    # testing/s3-source is unchanged but is now missing its import.

    testing/db-cdc:
      bindings:
        - target: testing/int-reverse
          resource: { }
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_capture_duplicates() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string-captures:
  captures:
    testing/s3-source:
      bindings:
        - target: testing/int-string
          resource: {}

          # Duplicated resource path (disallowed).
        - target: testing/int-string.v2
          resource: {}

          # Duplicated collection (okay).
        - target: testing/int-string
          resource: {}

driver:
  captures:
    testing/s3-source:
      bindings:
        - resourcePath: [target, one]
        - resourcePath: [target, one]
        - resourcePath: [target, two]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_materialization_duplicates() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/db-views:
  materializations:
    testing/db-views:
      bindings:
        - source: testing/int-string
          resource: {}

        # Duplicated resource path (disallowed).
        - source: testing/int-string.v2
          resource: {}

        # Duplicated collection (okay).
        - source: testing/int-string
          resource: {}

driver:
  materializations:
    testing/db-views:
      bindings:
        - constraints: {}
          resourcePath: [target, one]
        - constraints: {}
          resourcePath: [target, one]
        - constraints: {}
          resourcePath: [target, two]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_use_without_import() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string:
  import: [] # Clear.

test://example/int-reverse:
  import: [] # Clear.

test://example/webhook-deliveries:
  import: [] # Clear.
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_schema_fragment_not_found() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string:
  collections:
    testing/int-string:
      schema: test://example/int-string.schema#/not/found

# Omit downstream errors.
test://example/db-views:
  materializations: null
test://example/webhook-deliveries:
  materializations: null

test://example/int-halve:
  collections:
    testing/int-halve:
      derivation:
        transform:
          halveIntString:
            source:
              schema: test://example/int-string-len.schema#/not/found
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_keyed_location_pointer_is_malformed() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string:
  collections:
    testing/int-string:
      key: [int]
      projections:
        Int: int
        DoubleSlash: /double//slash
        InvalidEscape: /an/esc~ape
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_keyed_location_wrong_type() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string.schema:
  properties:
    int: { type: [number, object] }
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_unknown_locations() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string:
  collections:
    testing/int-string:
      key: [/int, /unknown/key]
      projections:
        Unknown: /unknown/projection

test://example/int-halve:
  collections:
    testing/int-halve:
      derivation:
        transform:
          halveIntString:
            shuffle:
              key: [/len, /int, /unknown/shuffle]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_shuffle_key_length_mismatch() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-halve:
  collections:
    testing/int-halve:
      derivation:
        transform:
          halveIntString:
            shuffle:
              key: [/len]
          halveSelf:
            shuffle:
              key: [/len, /int]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_shuffle_key_types_mismatch() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-halve:
  collections:
    testing/int-halve:
      derivation:
        transform:
          halveIntString:
            shuffle:
              key: [/int, /str]
          halveSelf:
            shuffle:
              key: [/str, /int]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_shuffle_key_relaxed() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-reverse:
  collections:
    testing/int-reverse:
      key: [/bit, /str]
      derivation:
        transform:
          reverseIntString:
            source:
              name: testing/int-string
            publish: { lambda: typescript }
          selfCycle:
            source:
              name: testing/int-reverse
            publish: { lambda: typescript }
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_collection_key_empty() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string:
  collections:
    testing/int-string:
      key: []
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_shuffle_key_empty() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-reverse:
  collections:
    testing/int-reverse:
      derivation:
        transform:
          reverseIntString:
            shuffle: {key: []}
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_partition_selections() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-halve:
  collections:
    testing/int-halve:
      derivation:
        transform:
          halveIntString:
            source:
              partitions:
                include:
                  bit: [true, 42, ""]
                  Int: [15, true]
                  Unknown: ["whoops"]
                exclude:
                  bit: [false, "a string"]
                  Int: [false, "", 16]
                  AlsoUnknown: ["whoops"]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_redundant_source_schema_and_shuffle() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-reverse:
  collections:
    testing/int-reverse:
      derivation:
        transform:
          reverseIntString:
            source:
              name: testing/int-string
              schema: test://example/int-string.schema
            shuffle:
              key: [/int]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_must_have_update_or_publish() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-reverse:
  collections:
    testing/int-reverse:
      derivation:
        typescript: null
        transform:
          reverseIntString:
            publish: null
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_typescript_lambdas_without_module() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-reverse:
  collections:
    testing/int-reverse:
      derivation:
        typescript: null
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_typescript_modules_without_lambdas() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-reverse:
  collections:
    testing/int-reverse:
      derivation:
        transform:
          reverseIntString:
            publish: { lambda: { remote: https://an/api } }
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_typescript_module_used_multiple_times() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-reverse:
  collections:
    testing/int-reverse:
      derivation:
        typescript:
          module: int-halve.ts
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_invalid_initial_register() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-halve:
  collections:
    testing/int-halve:
      derivation:
        register:
          initial: "should be an integer"
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_shape_inspections() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string-len.schema:
  properties:
    int:
      reduce: { strategy: set }
    str:
      reduce: { strategy: sum }
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_schema_reference_verification() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string-len.schema:
  $ref: test://example/int-string.schema#/whoops
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_materialization_source_not_found() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/db-views:
  materializations:
    testing/db-views:
      bindings:
        - source: testiNg/int-strinK
          resource: { table: the_table }
        - source: wildly/off/name
          resource: { table: other_table }
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_materialization_field_errors() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/webhook-deliveries:
  materializations:
    testing/webhook/deliveries:
      bindings:
        # Included only to maintain proper ordering of driver fixture.
        - source: testing/int-string
          resource: { fixture: one }

        - source: testing/int-halve
          resource: { fixture: two }
          fields:
            include:
              int: {} # Include and exclude.
              biT: {} # Unknown.
              Len: {} # OK.
            exclude:
              - BiTT # Unknown.
              - WildlyOffName # Also unknown.
              - int
            recommended: false
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_capture_driver_returns_error() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
driver:
  captures:
    testing/s3-source:
      error: "A driver error!"
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_materialization_driver_returns_error() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
driver:
  materializations:
    testing/webhook/deliveries:
      bindings: []
      error: "A driver error!"
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_materialization_driver_unknown_constraint() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
driver:
  materializations:
    testing/webhook/deliveries:
      bindings:
        - constraints:
            flow_document: { type: 1, reason: "location required" }
            int: { reason: "other whoops" }
          resourcePath: [tar!get, one]
          typeOverride: 98
        - constraints:
            Root: { type: 1, reason: "location required" }
            str: { reason: "whoops" }
          resourcePath: [tar!get, two]
          typeOverride: 99
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_materialization_driver_conflicts() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"

test://example/db-views:
  materializations:
    testing/db-views:
      bindings:
        - source: testing/int-string
          resource: { table: the_table }
          fields:
            include:
              str: {}
            exclude:
              - bit
              - Int
            recommended: true

driver:
  materializations:
    testing/db-views:
      bindings:
        - constraints:
            flow_document: { type: 1, reason: "location required" }
            Int: { type: 1, reason: "location required" }
            int: { type: 5, reason: "field unsatisfiable" }
            str: { type: 4, reason: "field forbidden" }
            bit: { type: 0, reason: "field required" }
            Unknown: { type: 0, reason: "whoops" }
          resourcePath: [tar!get]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_test_step_unknown_collection() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string-tests:
  tests:
    testing/test:
      - ingest:
          collection: testinG/Int-strin
          documents: []
      - verify:
          collection: wildly/Off/Name
          documents: []
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_test_step_ingest_schema_error() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string-tests:
  tests:
    testing/test:
      - ingest:
          collection: testing/int-string
          documents:
            - {int: 42, str_whoops: "string A", bit: true}
            - {int: 52, str_whoops: "string B", bit: true}
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_test_step_verify_key_order() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string-tests:
  tests:
    testing/test:
      - verify:
          collection: testing/int-string
          documents: [{int: 52}, {int: 62}, {int: 42}]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_test_step_verify_selector() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string-tests:
  tests:
    testing/test:
      - verify:
          collection: testing/int-string
          documents: []
          partitions:
            include:
              bit: [true, 42, ""]
              Int: [15, true]
              Unknown: ["whoops"]
            exclude:
              bit: [false, "a string"]
              Int: [false, "", 16]
              AlsoUnknown: ["whoops"]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_materialization_selector() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/db-views:
  materializations:
    testing/db-views:
      bindings:
        - source: testing/int-string
          resource: { table: the_table }
          partitions:
            include:
              bit: [true, 42, ""]
              Int: [15, true]
              Unknown: ["whoops"]
            exclude:
              bit: [false, "a string"]
              Int: [false, "", 16]
              AlsoUnknown: ["whoops"]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_incompatible_npm_packages() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-reverse:
  collections:
    testing/int-reverse:
      derivation:
        typescript:
          npmDependencies:
            package-one: "same"
            pkg-2: "different"
            package-4: "4"

test://example/int-halve:
  collections:
    testing/int-halve:
      derivation:
        typescript:
          npmDependencies:
            package-one: "same"
            pkg-2: "differ ent"
            pkg-three: "3"
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_invalid_and_duplicate_storage_mappings() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string:
  storageMappings:
    testing/:
      # Exact match of a mapping.
      stores: &stores [{provider: S3, bucket: alternate-data-bucket}]
    recoverY/:
      # Prefix of another mapping.
      stores: *stores
    Not-Matched/foobar/:
      # Another mapping is a prefix of this.
      stores: *stores

    "": {stores: *stores}
    "bad space": {stores: *stores}
    "bad!punctuation/": {stores: *stores}
    missingSlash: {stores: *stores}
    double//slash/: {stores: *stores}
    "/leading/Slash/": {stores: *stores}
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_storage_mappings_not_imported() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:
  storageMappings: null

test://example/array-key:
  storageMappings:
    testing/:
      stores: [{provider: S3, bucket: data-bucket}]
    recovery/testing/:
      stores: [{provider: GCS, bucket: recovery-bucket, prefix: some/ }]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_storage_mappings_not_found() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:
  # Clear existing mappings.
  storageMappings: null

test://example/int-string:
  # Define new mappings in a different catalog source.
  storageMappings:
    TestinG/:
      stores: [{provider: S3, bucket: data-bucket}]
    RecoverY/TestinG/:
      stores: [{provider: GCS, bucket: recovery-bucket, prefix: some/ }]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_no_storage_mappings_defined() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:
  storageMappings: null
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_storage_mappings_without_prefix() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:
  storageMappings:
    "":
      # This is allowed, and matches for all journals and tasks.
      stores: [{provider: S3, bucket: a-bucket}]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_collection_schema_string() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:
  import:
    - test://example/string-schema

test://example/string-schema:
  collections:
    testing/string-schema:
      schema: {type: string}
      key: ['']
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_non_canonical_schema_ref() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-halve:
  collections:
    testing/int-halve:
      # Switch from directly naming the (non-canonical) schema
      # to instead $ref'ing it from an inline schema.
      # This is disallowed; we only support $ref's of canonical URIs
      # (as it's not possible to bundle non-canonical $ref's).
      schema:
        $ref: test://example/int-string-len.schema
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MockDriverCalls {
    materializations: HashMap<String, MockMaterializationValidateCall>,
    captures: HashMap<String, MockCaptureValidateCall>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MockCaptureValidateCall {
    endpoint: flow::EndpointType,
    spec: serde_json::Value,
    bindings: Vec<MockDriverBinding>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MockMaterializationValidateCall {
    endpoint: flow::EndpointType,
    spec: serde_json::Value,
    bindings: Vec<MockDriverBinding>,
    #[serde(default)]
    delta_updates: bool,
    #[serde(default)]
    error: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MockDriverBinding {
    resource_path: Vec<String>,
    #[serde(default)]
    constraints: HashMap<String, materialize::Constraint>,
    // type_override overrides the parsed constraints[].type for
    // each constraint. It supports test cases which want to deliberately
    // use type values which are invalid, and can't be parsed as YAML
    // (because of serde deserialization checks by the pbjson crate).
    #[serde(default)]
    type_override: i32,
}

impl validation::Drivers for MockDriverCalls {
    fn validate_materialization<'a>(
        &'a self,
        request: materialize::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<materialize::ValidateResponse, anyhow::Error>> {
        async move {
            let call = match self.materializations.get(&request.materialization) {
                Some(call) => call,
                None => {
                    return Err(anyhow::anyhow!(
                        "driver fixture not found: {}",
                        request.materialization
                    ));
                }
            };

            let endpoint_spec: serde_json::Value =
                serde_json::from_str(&request.endpoint_spec_json)?;

            if call.endpoint as i32 != request.endpoint_type {
                return Err(anyhow::anyhow!(
                    "endpoint type mismatch: {} vs {}",
                    call.endpoint as i32,
                    request.endpoint_type
                ));
            }
            if &call.spec != &endpoint_spec {
                return Err(anyhow::anyhow!(
                    "endpoint spec mismatch: {} vs {}",
                    call.spec.to_string(),
                    &request.endpoint_spec_json,
                ));
            }
            if let Some(err) = &call.error {
                return Err(anyhow::anyhow!("{}", err));
            }

            let bindings = call
                .bindings
                .iter()
                .map(|b| {
                    let mut out = materialize::validate_response::Binding {
                        constraints: b.constraints.clone(),
                        delta_updates: call.delta_updates,
                        resource_path: b.resource_path.clone(),
                    };

                    // NOTE(johnny): clunky support for test_materialization_driver_unknown_constraints,
                    // to work around serde deser not allowing parsing of invalid enum values.
                    for c in out.constraints.iter_mut() {
                        if c.1.r#type == 0 && b.type_override != 0 {
                            c.1.r#type = b.type_override;
                        }
                    }

                    out
                })
                .collect();

            return Ok(materialize::ValidateResponse { bindings });
        }
        .boxed_local()
    }

    fn validate_capture<'a>(
        &'a self,
        request: capture::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<capture::ValidateResponse, anyhow::Error>> {
        async move {
            let call = match self.captures.get(&request.capture) {
                Some(call) => call,
                None => {
                    return Err(anyhow::anyhow!(
                        "driver fixture not found: {}",
                        request.capture
                    ));
                }
            };

            let endpoint_spec: serde_json::Value =
                serde_json::from_str(&request.endpoint_spec_json)?;

            if call.endpoint as i32 != request.endpoint_type {
                return Err(anyhow::anyhow!(
                    "endpoint type mismatch: {} vs {}",
                    call.endpoint as i32,
                    request.endpoint_type
                ));
            }
            if &call.spec != &endpoint_spec {
                return Err(anyhow::anyhow!(
                    "endpoint spec mismatch: {} vs {}",
                    call.spec.to_string(),
                    &request.endpoint_spec_json,
                ));
            }
            if let Some(err) = &call.error {
                return Err(anyhow::anyhow!("{}", err));
            }

            let bindings = call
                .bindings
                .iter()
                .map(|b| capture::validate_response::Binding {
                    resource_path: b.resource_path.clone(),
                })
                .collect();

            return Ok(capture::ValidateResponse { bindings });
        }
        .boxed_local()
    }
}

fn run_test(mut fixture: Value, config: &flow::build_api::Config) -> tables::All {
    // Extract out driver mock call fixtures.
    let mock_calls = fixture
        .get_mut("driver")
        .map(|d| d.take())
        .unwrap_or_default();
    let mock_calls: MockDriverCalls = serde_json::from_value(mock_calls).unwrap();

    let tables::Sources {
        capture_bindings,
        captures,
        collections,
        derivations,
        mut errors,
        fetches,
        imports,
        materialization_bindings,
        materializations,
        npm_dependencies,
        projections,
        resources,
        schema_docs,
        storage_mappings,
        test_steps,
        transforms,
    } = sources::scenarios::evaluate_fixtures(Default::default(), &fixture);

    let tables::Validations {
        built_captures,
        built_collections,
        built_derivations,
        built_materializations,
        built_tests,
        errors: validation_errors,
        inferences,
    } = futures::executor::block_on(validation::validate(
        config,
        &mock_calls,
        &capture_bindings,
        &captures,
        &collections,
        &derivations,
        &fetches,
        &imports,
        &materialization_bindings,
        &materializations,
        &npm_dependencies,
        &projections,
        &resources,
        &storage_mappings,
        &test_steps,
        &transforms,
    ));

    errors.extend(validation_errors.into_iter());

    tables::All {
        built_captures,
        built_collections,
        built_derivations,
        built_materializations,
        built_tests,
        capture_bindings,
        captures,
        collections,
        derivations,
        errors,
        fetches,
        imports,
        inferences,
        materialization_bindings,
        materializations,
        meta: tables::Meta::new(),
        npm_dependencies,
        projections,
        resources,
        schema_docs,
        storage_mappings,
        test_steps,
        transforms,
    }
}

#[must_use]
fn run_test_errors(fixture: &Value, patch: &str) -> tables::Errors {
    let mut fixture = fixture.clone();
    let patch: Value = serde_yaml::from_str(patch).unwrap();
    json_patch::merge(&mut fixture, &patch);

    let tables::All { errors, .. } = run_test(
        fixture,
        &flow::build_api::Config {
            build_id: "a-build-id".to_string(),
            ..Default::default()
        },
    );
    errors
}
