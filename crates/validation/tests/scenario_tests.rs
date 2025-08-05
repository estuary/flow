mod common;

const MODEL_YAML: &str = include_str!("model.yaml");

#[test]
fn test_golden_all_visits() {
    let outcome = common::run(MODEL_YAML, "{}");
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_projection_not_created_for_empty_properties() {
    let fixture = r##"
test://example/catalog.yaml:
  collections:
    testing/schema_with_empty_properties:
      schema:
        type: object
        properties:
          id: { type: string }
          "": { type: string }
          a:
            type: object
            properties:
              "": { type: string }
        required: [id]
      key: [/id]

driver:
  dataPlanes:
    "1d:1d:1d:1d:1d:1d:1d:1d":
      default: true
"##;

    let outcome = common::run(fixture, "{}");
    // Expect not to see any projections for the empty properties
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn connector_validation_is_skipped_when_shards_are_disabled() {
    let outcome = common::run(include_str!("validation_skipped_when_disabled.yaml"), "{}");
    // Expect placeholder validation occurred and built task shards are disabled.
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_collection_schema_contains_flow_document() {
    let fixture = r##"
test://example/catalog.yaml:
  collections:
    testing/collection-with-flow-document:
      key: [/id]
      schema:
        type: object
        properties:
          id: {type: string}
          flow_document: {type: object}
        required: [id]

driver:
  dataPlanes:
    "1d:1d:1d:1d:1d:1d:1d:1d":
      default: true
"##;

    // Expect an implicit projection isn't created for `/flow_document`,
    // while its default projection to the document root is present.
    let outcome = common::run(fixture, "{}");
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn disabled_bindings_are_ignored() {
    let outcome = common::run(include_str!("disabled_bindings_are_ignored.yaml"), "{}");
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_database_round_trip() {
    let mut outcome = common::run(MODEL_YAML, "{}");

    // Collapse `errors_draft` into `errors`, as they use the same DB table.
    outcome.errors.extend(outcome.errors_draft.drain(..));

    // Round-trip source and built tables through the database, verifying equality.
    let db = rusqlite::Connection::open(":memory:").unwrap();
    tables::persist_tables(&db, &outcome.as_tables()).unwrap();

    let mut reloaded = common::Outcome::default();
    tables::load_tables(&db, reloaded.as_tables_mut().as_mut_slice()).unwrap();

    reloaded.errors_draft.clear(); // Loaded twice.

    let original = format!("{outcome:#?}");
    let recovered = format!("{reloaded:#?}");

    if original != recovered {
        std::fs::write("ORIGINAL", original).unwrap();
        std::fs::write("RECOVERED", recovered).unwrap();
        panic!("database round trip: original & restored tables are different! Wrote ORIGINAL & RECOVERED for debugging");
    }
}

#[test]
fn test_invalid_collection_names_prefixes_and_duplicates() {
    let errors = common::run_errors(
        MODEL_YAML,
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

driver:
  storageMappings: null
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_invalid_partition_names_and_duplicates() {
    let errors = common::run_errors(
        MODEL_YAML,
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
fn test_invalid_transform_names_and_duplicates() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/int-reverse:
  collections:
    testing/int-reverse:
      derive:
        transforms:
          - name: reverseIntString
            source: &source
              name: testing/int-string
            shuffle: any
          - name: ""
            source: *source
            shuffle: any
          - name: inv alid
            source: *source
            shuffle: any
          - name: inv!alid
            source: *source
            shuffle: any
          - name: inv/alid
            source: *source
            shuffle: any
          # Illegal duplicate under collation.
          - name: reVeRsEIntString
            source: *source
            shuffle: any
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_invalid_capture_names_and_duplicates() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  import:
    - test://example/captures
test://example/captures:
  captures:
    good: &spec
      endpoint:
        connector: &config
          image: an/image
          config:
            some: thing
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

driver:
  storageMappings: null
  captures:
    good: &connector
      connectorType: IMAGE
      config: *config
      bindings: []
    testing: *connector
    testing/SoMe-source: *connector
    testing/some-source: *connector
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_invalid_materialization_names_and_duplicates() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  import:
    - test://example/materializations
test://example/materializations:
  materializations:
    good: &spec
      endpoint:
        connector: &config
          image: an/image
          config:
            some: thing
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
driver:
  storageMappings: null
  materializations:
    good: &connector
      connectorType: IMAGE
      config: *config
      bindings: []
    testing: *connector
    testing/SoMe-target: *connector
    testing/some-target: *connector
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_invalid_test_names_and_duplicates() {
    let errors = common::run_errors(
        MODEL_YAML,
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
    let errors = common::run_errors(
        MODEL_YAML,
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
          config: { a: config }
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

driver:
  storageMappings: null

  captures:
    testing/b/1: &connector
      connectorType: IMAGE
      config:
        image: an/image
        config: { a: config }
      bindings: []
    testing/b/2/suffix: *connector
    testing/b/3: *connector
    testing/b/5/suffix: *connector

  materializations:
    testing/b/1/suffix: *connector
    testing/b/2: *connector
    testing/b/3: *connector
    testing/b/4: *connector

"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_transform_source_not_found() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/int-halve:
  collections:
    testing/int-halve:
      derive:
        transforms:
          - name: halveIntString
            source: testinG/Int-String
            shuffle:
              key: [/foo, /bar]
          - name: halveSelf
            source: wildly/off/name
            shuffle: any
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_capture_target_not_found() {
    let errors = common::run_errors(
        MODEL_YAML,
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
fn test_capture_missing_resource_path() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/int-string-captures:
  captures:
    testing/s3-source:
      bindings:
        - target: testing/int-string
          resource: {}

        - target: testing/int-string.v2
          resource: {}

driver:
  captures:
    testing/s3-source:
      bindings:
        - resourcePath: []
        - resourcePath: [target, one]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_capture_duplicates() {
    let errors = common::run_errors(
        MODEL_YAML,
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

          # Duplicated disabled resource path (disallowed).
        - target: testing/int-string.v2
          resource: {_meta: {path: [target, one]}}
          disable: true

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
fn test_materialization_missing_resource_paths() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/db-views:
  materializations:
    testing/db-views:
      bindings:
        - source: testing/int-string
          resource: {}
        - source: testing/int-string.v2
          resource: {}

driver:
  materializations:
    testing/db-views:
      bindings:
        - constraints: {}
          resourcePath: [target, one]
        - constraints: {}
          resourcePath: []
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_materialization_duplicates() {
    let errors = common::run_errors(
        MODEL_YAML,
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

        # Duplicated disabled resource path (disallowed).
        - source: testing/int-string.v2
          resource: {_meta: {path: [target, one]}}
          disable: true

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
fn test_materialization_constraints_on_excluded_fields() {
    let outcome = common::run(
        include_str!("materialization_constraints_on_excluded_fields.yaml"),
        "{}",
    );
    // Expect no "naughty_" fields were selected.
    insta::assert_debug_snapshot!(outcome);
}

/// Tests a scenario where a collection spec contains an incomplete
/// `flow://write-schema` definition. This has been observed with users using
/// flowctl, and resulted in a super confusing error message. This now asserts
/// that an inlined write schema definition should always get overwritten by the
/// actual write schema, assuming it's actually referenced.
#[test]
fn test_collection_inlined_write_schema_overwrite() {
    let outcome = common::run(
        r##"
        test://example/catalog.yaml:
          collections:
            testing/with/writeSchema/ref:
              writeSchema:
                type: object
                properties:
                  id: { type: string }
                required: [id]
              readSchema:
                $defs:
                  'flow://write-schema':
                    type: object
                    properties:
                      id:
                        const: "this def should be overwritten prior to validation"
                allOf:
                  - $ref: flow://write-schema
                  - $ref: flow://inferred-schema
              key: [ /id ]
        driver:
          dataPlanes:
            "1d:1d:1d:1d:1d:1d:1d:1d":
              default: true
        "##,
        "{}",
    );

    // If we failed to overwrite the write schema def, then validation would
    // fail because the inlined def does not contain the `$id` property.
    assert!(
        outcome.errors.is_empty(),
        "expected no errors, got: {:?}",
        outcome.errors
    );
    assert!(
        outcome.errors_draft.is_empty(),
        "expected no draft errors, got: {:?}",
        outcome.errors_draft
    );
}

#[test]
fn test_schema_fragment_not_found() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
test://example/int-string:
  collections:
    testing/int-string:
      schema: test://example/int-string.schema#/not/found

    testing/int-string-rw:
      writeSchema: test://example/int-string.schema#/also/not/found
      readSchema:
        type: object
        properties:
          missing:
            $ref: test://example/int-string-len.schema#DoesNotExist
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_keyed_location_pointer_is_malformed() {
    let errors = common::run_errors(
        &MODEL_YAML,
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
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
test://example/int-string.schema:
  properties:
    int: { type: [number, object] }
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_keyed_location_read_write_types_differ() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"

test://example/int-string:
  collections:
    testing/int-string:
      schema: null

      readSchema:
        type: object
        properties:
          int: { type: integer }
          bit: { type: boolean }
          str: { type: string }
        required: [int, bit]

      writeSchema:
        type: object
        properties:
          int: { type: string }
          bit: { type: string }
        required: [int, bit]

test://example/int-string-tests: null # Extra errors we don't care about.
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_unknown_locations() {
    let errors = common::run_errors(
        &MODEL_YAML,
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
      derive:
        transforms:
          - name: halveIntString
            source: testing/int-string-rw
            shuffle:
              key: [/len, /int, /unknown/shuffle]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_shuffle_key_length_mismatch() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
test://example/int-halve:
  collections:
    testing/int-halve:
      derive:
        transforms:
          - name: halveIntString
            source: testing/int-string-rw
            shuffle:
              key: [/len]
          - name: halveSelf
            source: testing/int-halve
            shuffle:
              key: [/len, /int]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_shuffle_key_types_mismatch() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
test://example/int-halve:
  collections:
    testing/int-halve:
      derive:
        transforms:
          - name: halveIntString
            source: testing/int-string-rw
            shuffle:
              key: [/int, /str]
          - name: halveSelf
            source: testing/int-halve
            shuffle:
              key: [/str, /int]
        shuffleKeyTypes: [integer, boolean]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_shuffle_needs_explicit_types() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
test://example/int-halve:
  collections:
    testing/int-halve:
      derive:
        transforms:
          - name: halveIntString
            source: testing/int-string-rw
            shuffle:
              lambda: {the: lambda}
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_collection_key_empty() {
    let errors = common::run_errors(
        &MODEL_YAML,
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
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
test://example/int-reverse:
  collections:
    testing/int-reverse:
      derive:
        transforms:
          - name: reverseIntString
            source: testing/int-string
            shuffle: {key: []}
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_partition_selections() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
test://example/int-halve:
  collections:
    testing/int-halve:
      derive:
        transforms:
          - name: halveIntString
            source:
              name: testing/int-string-rw
              partitions:
                include:
                  bit: [true, 42, ""]
                  Int: [15, true]
                  Unknown: ["whoops"]
                exclude:
                  bit: [false, "a string"]
                  Int: [false, "", 16]
                  AlsoUnknown: ["whoops"]
            shuffle:
              key: [/len, /str]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_partition_not_defined_in_write_schema() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
test://example/int-string:
  collections:
    testing/int-string-rw:
      projections:
        Len:
          location: /len
          partition: true
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_key_not_defined_in_write_schema() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
test://example/int-string:
  collections:
    testing/int-string-rw:
      # /len is present in the read but not write schema.
      key: [/int, /len, /missing-in-read-and-write-schemas]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_shape_inspections() {
    let errors = common::run_errors(
        &MODEL_YAML,
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
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
test://example/int-string-len.schema:
  $ref: test://example/int-string.schema#/whoops
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_materialization_source_not_found() {
    let errors = common::run_errors(
        &MODEL_YAML,
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
    let errors = common::run_errors(
        &MODEL_YAML,
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
            require:
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
    let errors = common::run_errors(
        &MODEL_YAML,
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
fn test_derive_driver_returns_error() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
driver:
  derivations:
    testing/int-halve:
      error: "A driver error!"
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_materialization_driver_returns_error() {
    let errors = common::run_errors(
        &MODEL_YAML,
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
    let errors = common::run_errors(
        &MODEL_YAML,
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
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"

test://example/db-views:
  materializations:
    testing/db-views:
      bindings:
        - source: testing/int-string
          resource: { table: the_table }
          fields:
            require:
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
            flow_document: { type: 2, reason: "location required" }
            Int: { type: 2, reason: "location required" }
            int: { type: 6, reason: "field incompatible" }
            str: { type: 5, reason: "field forbidden" }
            bit: { type: 1, reason: "field required" }
            Unknown: { type: 1, reason: "whoops" }
          resourcePath: [tar!get]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_invalid_group_by_field() {
    // Create a new test with a non-scalar group_by
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
test://example/webhook-deliveries:
  materializations:
    testing/webhook/deliveries:
      bindings:
        - source: testing/int-string
          resource: { fixture: one }
          fields:
            recommended: true
            groupBy:
              - flow_document
        - source: testing/int-halve
          resource: { fixture: two }
          fields:
            recommended: false
            groupBy:
              - Unknown
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_test_step_unknown_collection() {
    let errors = common::run_errors(
        &MODEL_YAML,
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
    let errors = common::run_errors(
        &MODEL_YAML,
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
    let errors = common::run_errors(
        &MODEL_YAML,
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
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
test://example/int-string-tests:
  tests:
    testing/test:
      - verify:
          collection:
            name: testing/int-string
            partitions:
              include:
                bit: [true, 42, ""]
                Int: [15, true]
                Unknown: ["whoops"]
              exclude:
                bit: [false, "a string"]
                Int: [false, "", 16]
                AlsoUnknown: ["whoops"]
          documents: []
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_materialization_selector() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
test://example/db-views:
  materializations:
    testing/db-views:
      bindings:
        - source:
            name: testing/int-string
            partitions:
              include:
                bit: [true, 42, ""]
                Int: [15, true]
                Unknown: ["whoops"]
              exclude:
                bit: [false, "a string"]
                Int: [false, "", 16]
                AlsoUnknown: ["whoops"]
          resource: { table: the_table }
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_invalid_and_duplicate_storage_mappings() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
driver:
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
fn test_storage_mappings_not_found() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
driver:
  storageMappings:
    testing/: null
    recovery/testing/: null
    TestinG/:
      stores: [{provider: S3, bucket: data-bucket}]
    RecoverY/TestinG/:
      stores: [{provider: GCS, bucket: recovery-bucket, prefix: some/ }]
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_storage_mappings_without_prefix() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
driver:
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
    let errors = common::run_errors(
        &MODEL_YAML,
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
    let errors = common::run_errors(
        &MODEL_YAML,
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

#[test]
fn test_derivation_not_before_after_ordering() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
test://example/int-halve:
  collections:
    testing/int-halve:
      derive:
        transforms:
          - name: halveIntString
            shuffle: { key: [/len, /str] }
            source:
              name: testing/int-string-rw
              notBefore: 2020-03-06T03:02:01Z
              notAfter:  2019-03-06T03:02:01Z
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_materialization_not_before_after_ordering() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
test://example/webhook-deliveries:
  materializations:
    testing/webhook/deliveries:
      bindings:
        - source:
            name: testing/int-string
            notBefore: 2017-03-03T03:02:01Z
            notAfter:  2016-03-03T03:02:01Z
          resource: { fixture: one }
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_test_not_before_after() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
test://example/int-string-tests:
  tests:
    testing/test:
      - verify:
          collection:
            name: testing/int-string
            notBefore: 2017-03-03T03:02:01Z
            notAfter: 2019-03-06T09:30:02Z
          documents: []
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_invalid_generated_file_url() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
driver:
  derivations:
    testing/from-array-key:
      generatedFiles:
        "this is not a URL! ": generated content
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_data_plane_not_found() {
    let errors = common::run_errors(
        &MODEL_YAML,
        r#"
driver:
  dataPlanes:
    "1d:1d:1d:1d:1d:1d:1d:1d": null
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_collection_inferred_schema_is_unchanged() {
    let outcome = common::run(include_str!("schema_inference.yaml"), "{}");
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_collection_inferred_schema_is_updated() {
    let outcome = common::run(
        include_str!("schema_inference.yaml"),
        r#"
test://example/catalog.yaml:
  collections:
    testing/foobar:
      readSchema:
        $defs: null
    "#,
    );
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_collection_inferred_schema_wrong_generation() {
    let outcome = common::run(
        include_str!("schema_inference.yaml"),
        r#"
driver:
  liveCollections:
    testing/foobar: null
"#,
    );
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_collection_inferred_schema_not_available() {
    let outcome = common::run(
        include_str!("schema_inference.yaml"),
        r#"
driver:
  liveInferredSchemas: null
"#,
    );
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_collection_inferred_schema_generation_id_fallback() {
    let outcome = common::run(
        include_str!("schema_inference.yaml"),
        r#"
test://example/catalog.yaml:
  collections:
    testing/foobar:
      readSchema:
        $defs: null

driver:
  liveInferredSchemas:
    testing/foobar:
      x-collection-generation-id: null
"#,
    );
    insta::assert_debug_snapshot!(outcome);
}
