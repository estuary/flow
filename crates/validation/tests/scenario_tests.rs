use futures::{future::BoxFuture, FutureExt};
use lazy_static::lazy_static;
use proto_flow::{capture, derive, flow, materialize, runtime::Container};
use serde_json::Value;
use std::collections::BTreeMap;

lazy_static! {
    static ref GOLDEN: Value = serde_yaml::from_slice(include_bytes!("model.yaml")).unwrap();
}

#[test]
fn test_golden_all_visits() {
    let (
        (
            tables::Sources {
                captures,
                collections,
                errors: _,
                fetches,
                imports,
                materializations,
                resources,
                storage_mappings,
                tests,
            },
            tables::Validations {
                built_captures,
                built_collections,
                built_materializations,
                built_tests,
                errors: _,
            },
        ),
        errors,
    ) = run_test(GOLDEN.clone(), "a-build-id");

    // NOTE(johnny): There used to be a tables::All which was removed.
    // We re-constitute it here only to avoid churning this existing snapshot.
    // We can remove this and separately update the snapshot of `sources` & `validations`.
    #[derive(Debug)]
    #[allow(dead_code)]
    struct All {
        built_captures: tables::BuiltCaptures,
        built_collections: tables::BuiltCollections,
        built_materializations: tables::BuiltMaterializations,
        built_tests: tables::BuiltTests,
        captures: tables::Captures,
        collections: tables::Collections,
        errors: tables::Errors,
        fetches: tables::Fetches,
        imports: tables::Imports,
        materializations: tables::Materializations,
        meta: tables::Meta,
        resources: tables::Resources,
        storage_mappings: tables::StorageMappings,
        tests: tables::Tests,
    }
    let tables = All {
        built_captures,
        built_collections,
        built_materializations,
        built_tests,
        captures,
        collections,
        errors,
        fetches,
        imports,
        materializations,
        meta: tables::Meta::default(),
        resources,
        storage_mappings,
        tests,
    };

    insta::assert_debug_snapshot!(tables);
}

#[test]
fn test_projection_not_created_for_empty_properties() {
    let fixture = serde_yaml::from_str(
        r##"
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
  storageMappings:
    testing/:
      stores: [{provider: S3, bucket: a-bucket}]
    recovery/:
      stores: [{provider: S3, bucket: a-bucket}]
driver:
  captures: {}
  derivations: {}
  materializations: {}
    "##,
    )
    .unwrap();

    let ((_, validations), errors) = run_test(fixture, "remapping_flow_truncated");

    assert!(errors.is_empty(), "got errors: {errors:?}");
    assert_eq!(1, validations.built_collections.len());
    // Expect not to see any projections for the empty properties
    insta::assert_debug_snapshot!(validations.built_collections[0].spec.projections);
}

#[test]
fn connector_validation_is_skipped_when_shards_are_disabled() {
    let fixture =
        serde_yaml::from_slice(include_bytes!("validation_skipped_when_disabled.yaml")).unwrap();
    let ((_, validations), errors) = run_test(fixture, "validation-skipped-build-id");
    assert!(errors.is_empty(), "expected no errors, got: {errors:?}");

    let tables::Validations {
        built_captures,
        built_materializations,
        ..
    } = validations;

    assert_eq!(built_captures.len(), 1);
    assert!(
        built_captures[0]
            .spec
            .shard_template
            .as_ref()
            .unwrap()
            .disable,
    );
    assert_eq!(built_materializations.len(), 1);
    assert!(
        built_materializations[0]
            .spec
            .shard_template
            .as_ref()
            .unwrap()
            .disable,
    );
}

#[test]
fn test_collection_schema_contains_flow_document() {
    let fixture = serde_yaml::from_str(
        r##"
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

  storageMappings:
    testing/:
      stores: [{provider: S3, bucket: a-bucket}]
    recovery/:
      stores: [{provider: S3, bucket: a-bucket}]
driver: {}
"##,
    )
    .unwrap();
    let ((_, validations), errors) =
        run_test(fixture, "collection-contains-flow-document-build-id");
    assert!(errors.is_empty(), "expected no errors, got: {errors:?}");

    let tables::Validations {
        built_collections, ..
    } = validations;

    let collection = &built_collections[0];
    assert!(!collection
        .spec
        .projections
        .iter()
        .any(|p| p.ptr == "/flow_document"));

    let root_projection = collection
        .spec
        .projections
        .iter()
        .find(|p| p.field == "flow_document")
        .expect("missing flow_document projection");
    assert!(root_projection.ptr.is_empty());
}

#[test]
fn disabled_bindings_are_ignored() {
    let models = r##"
test://example/catalog.yaml:
  collections:
    testing/collection:
      key: [/id]
      schema:
        type: object
        properties:
          id: {type: string}
        required: [id]
    testing/partly-disabled-derivation:
      key: [/id]
      schema:
        type: object
        properties:
          id: {type: string}
        required: [id]
      derive:
        using:
          sqlite: {}
        transforms:
          - name: enabledTransform
            shuffle: any
            source: { name: testing/collection }
            lambda: 'select $id, 1 as count;'
          - name: disabledTransform
            shuffle: any
            source: { name: testing/collection }
            lambda: 'select $id, 2 as count;'
            disable: true
    testing/fully-disabled-derivation:
      key: [/id]
      schema:
        type: object
        properties:
          id: {type: string}
        required: [id]
      derive:
        using:
          sqlite: {}
        transforms:
          - name: disabledTransformA
            source: { name: testing/collection }
            lambda: select $id, 1 as count;
            disable: true
          - name: disabledTransformB
            source: { name: testing/collection }
            lambda: select $id, 2 as count;
            disable: true


  captures:
    testing/partially-disabled-capture:
      endpoint: { connector: { image: s3, config: {} }}
      bindings:
        - target: disabled/test/one
          disable: true
          resource: { stream: disabled-stream }
        - target: testing/collection
          resource: { stream: enabled-stream }
    testing/fully-disabled-capture:
      endpoint: { connector: { image: s3, config: {} }}
      bindings:
        - target: disabled/test/two
          disable: true
          resource: { stream: disabled-stream }
        - target: disabled/test/three
          disable: true
          resource: { stream: another-disabled-stream }
  materializations:
    testing/partially-disabled-materialization:
      endpoint: { connector: { image: s3, config: {} }}
      bindings:
        - source: testing/collection
          disable: true
          resource: { stream: disabled-stream }
        - source: testing/collection
          resource: { stream: enabled-stream }

    testing/fully-disabled-materialization:
      endpoint: { connector: { image: s3, config: {} }}
      bindings:
        - source: testing/collection
          disable: true
          resource: { stream: disabled-stream }

  storageMappings:
    testing/:
      stores: [{provider: S3, bucket: a-bucket}]
    recovery/:
      stores: [{provider: S3, bucket: a-bucket}]

driver:
  derivations:
    testing/partly-disabled-derivation:
      connectorType: SQLITE
      config: {}
      transforms:
        - readOnly: true
      shuffleKeyTypes: []
      generatedFiles: {}

    testing/fully-disabled-derivation:
      connectorType: SQLITE
      config: {}
      transforms: []
      shuffleKeyTypes: []
      generatedFiles: {}

  captures:
    testing/partially-disabled-capture:
      connectorType: IMAGE
      config:
        image: s3
        config: {}
      bindings:
        -  resourcePath: [ enabled-stream ]

    testing/fully-disabled-capture:
      connectorType: IMAGE
      config:
        image: s3
        config: {}
      bindings: []

  materializations:
    testing/partially-disabled-materialization:
      connectorType: IMAGE
      config:
        image: s3
        config: {}
      bindings:
        - resourcePath: [ enabled-stream ]
          constraints:
            flow_document: { type: 2, reason: "location required" }

    testing/fully-disabled-materialization:
      connectorType: IMAGE
      config:
        image: s3
        config: {}
      bindings: []

  "##;

    let ((_, validations), errors) =
        run_test(serde_yaml::from_str(models).unwrap(), "disabled-bindings");
    assert!(errors.is_empty(), "expected no errors, got: {errors:?}");

    let tables::Validations {
        built_captures,
        built_materializations,
        ..
    } = validations;

    assert_eq!(built_captures.len(), 2);
    let partly_disabled_cap = built_captures
        .iter()
        .find(|c| c.capture == "testing/partially-disabled-capture")
        .unwrap();
    assert_eq!(1, partly_disabled_cap.spec.bindings.len());

    let fully_disabled_cap = built_captures
        .iter()
        .find(|c| c.capture == "testing/fully-disabled-capture")
        .unwrap();
    assert_eq!(0, fully_disabled_cap.spec.bindings.len());

    let partly_disabled_mat = built_materializations
        .iter()
        .find(|m| m.materialization == "testing/partially-disabled-materialization")
        .unwrap();
    assert_eq!(1, partly_disabled_mat.spec.bindings.len());
    let fully_disabled_mat = built_materializations
        .iter()
        .find(|m| m.materialization == "testing/fully-disabled-materialization")
        .unwrap();
    assert_eq!(0, fully_disabled_mat.spec.bindings.len());
}

#[test]
fn test_database_round_trip() {
    let ((sources, validations), _) = run_test(GOLDEN.clone(), "a-build-id");

    // Round-trip source and built tables through the database, verifying equality.
    let db = rusqlite::Connection::open(":memory:").unwrap();

    tables::persist_tables(&db, &sources.as_tables()).unwrap();
    tables::persist_tables(&db, &validations.as_tables()).unwrap();

    let mut reload_sources = tables::Sources::default();
    let mut reload_validations = tables::Validations::default();

    tables::load_tables(&db, reload_sources.as_tables_mut().as_mut_slice()).unwrap();
    tables::load_tables(&db, reload_validations.as_tables_mut().as_mut_slice()).unwrap();

    let original = format!("{sources:#?} {validations:#?}");
    let recovered = format!("{reload_sources:#?} {reload_validations:#?}");

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
fn test_invalid_transform_names_and_duplicates() {
    let errors = run_test_errors(
        &GOLDEN,
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
driver: {}
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
driver: {}
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
driver: {}
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
fn test_materialization_constraints_on_excluded_fields() {
    let ((_, validations), errors) = run_test(
        serde_yaml::from_str(
            r#"
test://example/catalog.yaml:
  collections:
    testing/constraints:
      schema:
        type: object
        properties:
          id: { type: string }
          naughty_u: { type: string }
          naughty_f: { type: string }
        required: [id]
      key: [/id]
  materializations:
    testing/db-views:
      endpoint:
        connector:
          image: an/image:test
          config: {}
      bindings:
        - source: testing/constraints
          resource: {table: anything}
          fields:
            recommended: true
            exclude:
              - naughty_u
              - naughty_f
  storageMappings:
    testing/:
        stores: [{ provider: S3, bucket: data-bucket }]
    recovery/testing/:
        stores: [{ provider: GCS, bucket: recovery-bucket, prefix: some/ }]
driver:
  materializations:
    testing/db-views:
      connectorType: IMAGE
      config:
        image: an/image:test
        config: {}
      bindings:
        - constraints:
            flow_document: { type: 1, reason: "location required" }
            id: { type: 1, reason: "location required" }
            naughty_u: { type: 6, reason: "field unsatisfiable" }
            naughty_f: { type: 5, reason: "field forbidden" }
          resourcePath: [anything]
"#,
        )
        .unwrap(),
        "constraints-excluded-fields",
    );
    assert!(errors.is_empty(), "expected no errors, got: {errors:?}");

    let fields = validations.built_materializations[0].spec.bindings[0]
        .field_selection
        .as_ref()
        .unwrap();
    assert!(!fields.values.iter().any(|f| f.starts_with("naughty_")));
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
    let errors = run_test_errors(
        &GOLDEN,
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
    let errors = run_test_errors(
        &GOLDEN,
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
    let errors = run_test_errors(
        &GOLDEN,
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
fn test_shuffle_is_missing() {
    let errors = run_test_errors(
        &GOLDEN,
        r#"
test://example/int-halve:
  collections:
    testing/int-halve:
      derive:
        transforms:
          - name: halveIntString
            source: testing/int-string-rw
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
    let errors = run_test_errors(
        &GOLDEN,
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
    let errors = run_test_errors(
        &GOLDEN,
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
    let errors = run_test_errors(
        &GOLDEN,
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
fn test_derive_driver_returns_error() {
    let errors = run_test_errors(
        &GOLDEN,
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
            flow_document: { type: 2, reason: "location required" }
            Int: { type: 2, reason: "location required" }
            int: { type: 6, reason: "field unsatisfiable" }
            str: { type: 5, reason: "field forbidden" }
            bit: { type: 1, reason: "field required" }
            Unknown: { type: 1, reason: "whoops" }
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
    let errors = run_test_errors(
        &GOLDEN,
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

#[test]
fn test_derivation_not_before_after_ordering() {
    let errors = run_test_errors(
        &GOLDEN,
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
    let errors = run_test_errors(
        &GOLDEN,
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
    let errors = run_test_errors(
        &GOLDEN,
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
    let errors = run_test_errors(
        &GOLDEN,
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

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MockDriverCalls {
    #[serde(default)]
    captures: BTreeMap<String, MockCaptureValidateCall>,
    #[serde(default)]
    derivations: BTreeMap<String, MockDeriveValidateCall>,
    #[serde(default)]
    inferred_schemas: BTreeMap<models::Collection, models::Schema>,
    #[serde(default)]
    materializations: BTreeMap<String, MockMaterializationValidateCall>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MockCaptureValidateCall {
    connector_type: flow::capture_spec::ConnectorType,
    config: serde_json::Value,
    bindings: Vec<MockDriverBinding>,
    #[serde(default)]
    network_ports: Vec<flow::NetworkPort>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MockDeriveValidateCall {
    connector_type: flow::collection_spec::derivation::ConnectorType,
    config: serde_json::Value,
    shuffle_key_types: Vec<flow::collection_spec::derivation::ShuffleType>,
    transforms: Vec<MockDeriveTransform>,
    generated_files: BTreeMap<String, String>,
    #[serde(default)]
    network_ports: Vec<flow::NetworkPort>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MockDeriveTransform {
    read_only: bool,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MockMaterializationValidateCall {
    connector_type: flow::materialization_spec::ConnectorType,
    config: serde_json::Value,
    bindings: Vec<MockDriverBinding>,
    #[serde(default)]
    delta_updates: bool,
    #[serde(default)]
    network_ports: Vec<flow::NetworkPort>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MockDriverBinding {
    resource_path: Vec<String>,
    #[serde(default)]
    constraints: BTreeMap<String, materialize::response::validated::Constraint>,
    // type_override overrides the parsed constraints[].type for
    // each constraint. It supports test cases which want to deliberately
    // use type values which are invalid, and can't be parsed as YAML
    // (because of serde deserialization checks by the pbjson crate).
    #[serde(default)]
    type_override: i32,
}

impl validation::Connectors for MockDriverCalls {
    fn validate_capture<'a>(
        &'a self,
        request: capture::Request,
    ) -> BoxFuture<'a, anyhow::Result<capture::Response>> {
        let capture::Request {
            validate: Some(request),
            ..
        } = request
        else {
            unreachable!()
        };

        async move {
            let call = match self.captures.get(&request.name) {
                Some(call) => call,
                None => {
                    return Err(anyhow::anyhow!(
                        "driver fixture not found: {}",
                        request.name
                    ));
                }
            };

            let config: serde_json::Value = serde_json::from_str(&request.config_json)?;

            if call.connector_type as i32 != request.connector_type {
                return Err(anyhow::anyhow!(
                    "connector type mismatch: {} vs {}",
                    call.connector_type as i32,
                    request.connector_type
                ));
            }
            if &call.config != &config {
                return Err(anyhow::anyhow!(
                    "connector config mismatch: {} vs {}",
                    call.config.to_string(),
                    &request.config_json,
                ));
            }
            if let Some(err) = &call.error {
                return Err(anyhow::anyhow!("{}", err));
            }

            let bindings = call
                .bindings
                .iter()
                .map(|b| capture::response::validated::Binding {
                    resource_path: b.resource_path.clone(),
                })
                .collect();

            Ok(capture::Response {
                validated: Some(capture::response::Validated { bindings }),
                ..Default::default()
            }
            .with_internal(|internal| {
                internal.container = Some(Container {
                    ip_addr: "1.2.3.4".to_string(),
                    network_ports: call.network_ports.clone(),
                    mapped_host_ports: Default::default(),
                });
            }))
        }
        .boxed()
    }

    fn validate_derivation<'a>(
        &'a self,
        request: derive::Request,
    ) -> BoxFuture<'a, anyhow::Result<derive::Response>> {
        let derive::Request {
            validate: Some(request),
            ..
        } = request
        else {
            unreachable!()
        };

        async move {
            let name = &request.collection.as_ref().unwrap().name;

            let call = match self.derivations.get(name) {
                Some(call) => call,
                None => {
                    return Err(anyhow::anyhow!("driver fixture not found: {}", name));
                }
            };

            let config: serde_json::Value = serde_json::from_str(&request.config_json)?;

            if call.connector_type as i32 != request.connector_type {
                return Err(anyhow::anyhow!(
                    "connector type mismatch: {} vs {}",
                    call.connector_type as i32,
                    request.connector_type
                ));
            }
            if &call.config != &config {
                return Err(anyhow::anyhow!(
                    "connector config mismatch: {} vs {}",
                    call.config.to_string(),
                    &request.config_json,
                ));
            }
            if call
                .shuffle_key_types
                .iter()
                .map(|t| *t as i32)
                .collect::<Vec<_>>()
                != request.shuffle_key_types
            {
                return Err(anyhow::anyhow!(
                    "shuffle types mismatch: {:?} vs {:?}",
                    call.shuffle_key_types,
                    request.shuffle_key_types,
                ));
            }

            if let Some(err) = &call.error {
                return Err(anyhow::anyhow!("{}", err));
            }

            let transforms = call
                .transforms
                .iter()
                .map(|b| derive::response::validated::Transform {
                    read_only: b.read_only,
                })
                .collect();

            Ok(derive::Response {
                validated: Some(derive::response::Validated {
                    transforms,
                    generated_files: call.generated_files.clone(),
                }),
                ..Default::default()
            }
            .with_internal(|internal| {
                internal.container = Some(Container {
                    ip_addr: "1.2.3.4".to_string(),
                    network_ports: call.network_ports.clone(),
                    mapped_host_ports: Default::default(),
                });
            }))
        }
        .boxed()
    }

    fn validate_materialization<'a>(
        &'a self,
        request: materialize::Request,
    ) -> BoxFuture<'a, anyhow::Result<materialize::Response>> {
        let materialize::Request {
            validate: Some(request),
            ..
        } = request
        else {
            unreachable!()
        };

        async move {
            let call = match self.materializations.get(&request.name) {
                Some(call) => call,
                None => {
                    return Err(anyhow::anyhow!(
                        "driver fixture not found: {}",
                        request.name
                    ));
                }
            };

            let config: serde_json::Value = serde_json::from_str(&request.config_json)?;

            if call.connector_type as i32 != request.connector_type {
                return Err(anyhow::anyhow!(
                    "connector type mismatch: {} vs {}",
                    call.connector_type as i32,
                    request.connector_type
                ));
            }
            if &call.config != &config {
                return Err(anyhow::anyhow!(
                    "connector config mismatch: {} vs {}",
                    call.config.to_string(),
                    &request.config_json,
                ));
            }
            if let Some(err) = &call.error {
                return Err(anyhow::anyhow!("{}", err));
            }

            let bindings = call
                .bindings
                .iter()
                .map(|b| {
                    let mut out = materialize::response::validated::Binding {
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

            Ok(materialize::Response {
                validated: Some(materialize::response::Validated { bindings }),
                ..Default::default()
            }
            .with_internal(|internal| {
                internal.container = Some(Container {
                    ip_addr: "1.2.3.4".to_string(),
                    network_ports: call.network_ports.clone(),
                    mapped_host_ports: Default::default(),
                });
            }))
        }
        .boxed()
    }
}

impl validation::ControlPlane for MockDriverCalls {
    fn resolve_collections<'a>(
        &'a self,
        _collections: Vec<models::Collection>,
    ) -> BoxFuture<'a, anyhow::Result<Vec<proto_flow::flow::CollectionSpec>>> {
        async move { Ok(Vec::new()) }.boxed()
    }

    fn get_inferred_schemas<'a>(
        &'a self,
        collections: Vec<models::Collection>,
    ) -> BoxFuture<'a, anyhow::Result<BTreeMap<models::Collection, validation::InferredSchema>>>
    {
        let out = collections
            .iter()
            .filter_map(|collection| {
                self.inferred_schemas.get(collection).map(|schema| {
                    (
                        collection.clone(),
                        validation::InferredSchema {
                            schema: schema.clone(),
                            md5: String::from("mock md5"),
                        },
                    )
                })
            })
            .collect();

        async move { Ok(out) }.boxed()
    }
}

fn run_test(
    mut fixture: Value,
    build_id: &str,
) -> ((tables::Sources, tables::Validations), tables::Errors) {
    // Extract out driver mock call fixtures.
    let mock_calls = fixture
        .get_mut("driver")
        .map(|d| d.take())
        .unwrap_or_default();
    let mock_calls: MockDriverCalls = serde_json::from_value(mock_calls).unwrap();

    let mut sources = sources::scenarios::evaluate_fixtures(Default::default(), &fixture);
    sources::inline_sources(&mut sources);

    let tables::Sources {
        captures,
        collections,
        errors: _,
        fetches,
        imports,
        materializations,
        resources: _,
        storage_mappings,
        tests,
    } = &sources;

    let mut validations = futures::executor::block_on(validation::validate(
        build_id,
        &url::Url::parse("file:///project/root").unwrap(),
        &mock_calls,
        &mock_calls,
        captures,
        collections,
        fetches,
        imports,
        materializations,
        storage_mappings,
        tests,
    ));

    let mut errors = std::mem::take(&mut sources.errors);
    errors.extend(std::mem::take(&mut validations.errors).into_iter());

    ((sources, validations), errors)
}

#[must_use]
fn run_test_errors(fixture: &Value, patch: &str) -> tables::Errors {
    let mut fixture = fixture.clone();
    let patch: Value = serde_yaml::from_str(patch).unwrap();
    json_patch::merge(&mut fixture, &patch);

    let (_, mut errors) = run_test(fixture, "a-build-id");

    // Squelch expected fixture error.
    if matches!(errors.first(), Some(err) if err.scope.as_str() == "test://example/from-array-key#/collections/testing~1from-array-key/derive/using/sqlite/migrations/1")
    {
        errors = errors.into_iter().skip(1).collect();
    }
    errors
}
