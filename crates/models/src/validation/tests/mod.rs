use crate::source;
use crate::tables;
use crate::validation;

use futures::{future::LocalBoxFuture, FutureExt};
use lazy_static::lazy_static;
use protocol::materialize;
use serde_json::Value;
use std::collections::HashMap;

lazy_static! {
    static ref GOLDEN: Value = serde_yaml::from_slice(include_bytes!("model.yaml")).unwrap();
}

#[test]
fn test_golden_all_visits() {
    let (source_tables, built_tables) = run_test(GOLDEN.clone());
    insta::assert_debug_snapshot!(built_tables);
    insta::assert_debug_snapshot!(source_tables);
}

#[test]
fn test_database_round_trip() {
    let (source_tables, built_tables) = run_test(GOLDEN.clone());

    // Round-trip source and built tables through the database, verifying equality.
    let db = rusqlite::Connection::open(":memory:").unwrap();
    tables::persist_tables(&db, &source_tables.as_tables()).unwrap();
    let mut restored_tables = source::Tables::default();
    tables::load_tables(&db, restored_tables.as_tables_mut().as_mut_slice()).unwrap();

    assert_eq!(
        format!("{:#?}", source_tables),
        format!("{:#?}", restored_tables)
    );

    let db = rusqlite::Connection::open(":memory:").unwrap();
    tables::persist_tables(&db, &built_tables.as_tables()).unwrap();
    let mut restored_tables = validation::Tables::default();
    tables::load_tables(&db, restored_tables.as_tables_mut().as_mut_slice()).unwrap();

    assert_eq!(
        format!("{:#?}", built_tables),
        format!("{:#?}", restored_tables)
    );
}

#[test]
fn test_golden_error() {
    run_test_errors(&GOLDEN, "{}");
}

#[test]
fn test_invalid_collection_names_prefixes_and_duplicates() {
    run_test_errors(
        &GOLDEN,
        r#"
test://root:
  collections:
    good: &spec
      schema: test://int-string.schema
      key: [/int]
      store: { name: storeEndpoint }

    "": *spec
    bad name: *spec
    bad!name: *spec

    # We require a sequence of non-empty tokens, separated by exactly one '/'.
    bad//name: *spec
    bad/name/: *spec
    /bad/name: *spec

    # Invalid prefix of testing/int-string & others.
    testing: *spec

    # Illegal duplicates under naming collation.
    testing/int-sTRinG: *spec
    testing/Int-Halve: *spec
"#,
    );
}

#[test]
fn test_invalid_partition_names_and_duplicates() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string:
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
}

#[test]
fn test_invalid_transform_names_and_duplicates() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-reverse:
  collections:
    testing/int-reverse:
      derivation:
        transform:
          good: &spec
            source:
              name: testing/int-string
            publish:
              nodeJS: "something something"

          "": *spec
          inv alid: *spec
          inv!alid: *spec
          inv/alid: *spec

          # Illegal duplicate under collation.
          reVeRsEIntString: *spec
"#,
    );
}

#[test]
fn test_invalid_endpoint_names_and_duplicates() {
    run_test_errors(
        &GOLDEN,
        r#"
test://root:
  import:
    - test://more-endpoints
test://more-endpoints:
  endpoints:
    good: &spec
      s3:
        bucket: a-bucket
        prefix: and-prefix

    "": *spec
    inv alid: *spec
    inv!alid: *spec
    inv/alid: *spec

    # Illegal duplicates under collation.
    StoreEndpoinT: *spec
    CAPtUReEndpoINT: *spec
"#,
    );
}

#[test]
fn test_invalid_capture_names_prefixes_and_duplicates() {
    run_test_errors(
        &GOLDEN,
        r#"
test://root:
  captures:
    good: &spec
      target:
        name: testing/int-string
      pushAPI: {}

    "": *spec
    bad name: *spec
    bad!name: *spec
    bad//name: *spec
    bad/name/: *spec
    /bad/name: *spec

    # Invalid prefix of testing/int-string/pull & push.
    testing: *spec

    # Illegal duplicates under naming collation.
    testing/int-sTRinG/pUll: *spec
    testing/Int-strINg/PuSH: *spec
"#,
    );
}

#[test]
fn test_invalid_materialization_names_prefixes_and_duplicates() {
    run_test_errors(
        &GOLDEN,
        r#"
test://root:
  materializations:
    good: &spec
      source:
        name: testing/int-string
      endpoint:
        name: materializeEndpoint
        config: { fixture: one }

    "": *spec
    bad name: *spec
    bad!name: *spec
    bad//name: *spec
    bad/name/: *spec
    /bad/name: *spec

    # Invalid prefix of testing/int-string.
    testing: *spec

    # Illegal duplicate under naming collation.
    testing/int-sTRinG: *spec
"#,
    );
}

#[test]
fn test_transform_source_not_found() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-halve:
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
}

#[test]
fn test_capture_target_not_found() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string-capture:
  captures:
    testing/int-string/pull:
      target: { name: testiNg/int-strinK }
    testing/int-string/push:
      target: { name: wildly/off/name }
"#,
    );
}

#[test]
fn test_capture_endpoint_not_found() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string-capture:
  captures:
    testing/int-string/pull:
      endpoint: { name: CaptureEndpoit }
    testing/int-string/push:
      endpoint: { name: wildlyOffName }
      pushAPI: null
"#,
    );
}

#[test]
fn test_capture_endpoint_wrong_type() {
    run_test_errors(
        &GOLDEN,
        r#"
test://root:
  endpoints:
    captureEndpoint:
      s3: null
      postgres:
        host: a-host
        user: a-user
        password: a-password
"#,
    );
}

#[test]
fn test_capture_target_is_derivation_and_missing_imports() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string-capture:
  import: null
  captures:
    testing/int-string/pull:
      target: { name: testing/int-reverse }
"#,
    );
}

#[test]
fn test_capture_duplicates() {
    run_test_errors(
        &GOLDEN,
        r#"
test://root:
  captures:
    testing/int-string/another/pull:
      target:
        name: testing/int-string
      endpoint: { name: captureEndpoint }
"#,
    );
}

#[test]
fn test_collection_store_not_found() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string:
  collections:
    testing/int-string:
      store: { name: sToreEndpoin }

test://int-reverse:
  collections:
    testing/int-reverse:
      store: { name: WildlyOffName }
"#,
    );
}

#[test]
fn test_use_without_import() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string:
  import: [] # Clear.
  collections:
    testing/int-string:
      store: { name: s3WithoutImport }

test://int-reverse:
  import: [] # Clear.
  endpoints:
    s3WithoutImport:
      s3:
        bucket: a-bucket
        prefix: and-prefix
"#,
    );
}

#[test]
fn test_schema_fragment_not_found() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string:
  collections:
    testing/int-string:
      schema: test://int-string.schema#/not/found

test://int-string-materialization:
  materializations:
    testing/int-string: null # Omit downstream errors.

test://int-halve:
  collections:
    testing/int-halve:
      derivation:
        transform:
          halveIntString:
            source:
              schema: test://int-string-len.schema#/not/found
"#,
    );
}

#[test]
fn test_keyed_location_wrong_type() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string.schema:
  properties:
    int: { type: [number, object] }
"#,
    );
}

#[test]
fn test_unknown_locations() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string:
  collections:
    testing/int-string:
      key: [/int, /unknown/key]
      projections:
        Unknown: /unknown/projection

test://int-halve:
  collections:
    testing/int-halve:
      derivation:
        transform:
          halveIntString:
            shuffle:
              key: [/len, /int, /unknown/shuffle]
"#,
    );
}

#[test]
fn test_shuffle_key_length_mismatch() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-halve:
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
}

#[test]
fn test_shuffle_key_types_mismatch() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-halve:
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
}

#[test]
fn test_shuffle_key_empty() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-reverse:
  collections:
    testing/int-reverse:
      derivation:
        transform:
          reverseIntString:
            shuffle: {key: []}
"#,
    );
}

#[test]
fn test_partition_selections() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-halve:
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
}

#[test]
fn test_redundant_source_schema_and_shuffle() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-reverse:
  collections:
    testing/int-reverse:
      derivation:
        transform:
          reverseIntString:
            source:
              name: testing/int-string
              schema: test://int-string.schema
            shuffle:
              key: [/int]
"#,
    );
}

#[test]
fn test_incompatible_collection_store() {
    run_test_errors(
        &GOLDEN,
        r#"
test://root:
  endpoints:
    storeEndpoint:
      s3: null
      postgres:
        host: a-host
        user: a-user
        password: a-password
"#,
    );
}

#[test]
fn test_must_have_update_or_publish() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-reverse:
  collections:
    testing/int-reverse:
      derivation:
        transform:
          reverseIntString:
            publish: null
            update: null
"#,
    );
}

#[test]
fn test_invalid_initial_register() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-halve:
  collections:
    testing/int-halve:
      derivation:
        register:
          initial: "should be an integer"
"#,
    );
}

#[test]
fn test_shape_inspections() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string-len.schema:
  properties:
    int:
      reduce: { strategy: set }
    str:
      reduce: { strategy: sum }
"#,
    );
}

#[test]
fn test_schema_reference_verification() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string-len.schema:
  $ref: test://int-string.schema#/whoops
"#,
    );
}

#[test]
fn test_materialization_source_not_found() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string-materialization:
  materializations:
    testing/int-string:
      source: { name: testiNg/int-strinK }

test://int-halve-materialization:
  materializations:
    testing/int-halve:
      source: { name: wildly/off/name }
"#,
    );
}

#[test]
fn test_materialization_endpoint_not_found() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string-materialization:
  materializations:
    testing/int-string:
      endpoint: { name: MaterializeEndpoit }

test://int-halve-materialization:
  materializations:
    testing/int-halve:
      endpoint: { name: wildlyOffName }
"#,
    );
}

#[test]
fn test_materialization_field_errors() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-halve-materialization:
  materializations:
    testing/int-halve:
      fields:
        include:
          int: {} # Include and exclude.
          biT: {} # Unknown.
        exclude:
          - BiTT # Unknown.
          - WildlyOffName # Also unknown.
          - int
"#,
    );
}

#[test]
fn test_materialization_driver_returns_error() {
    run_test_errors(
        &GOLDEN,
        r#"
driver:
  materializations:
    one:
      constraints: null
      error: "A driver error!"
"#,
    );
}

#[test]
fn test_materialization_driver_unknown_constraint() {
    run_test_errors(
        &GOLDEN,
        r#"
driver:
  materializations:
    one:
      constraints:
        str: { type: 99, reason: "whoops" }
"#,
    );
}

#[test]
fn test_materialization_driver_conflicts() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string-materialization:
  materializations:
    testing/int-string:
      fields:
        include:
          str: {}
        exclude:
          - bit
          - Int
driver:
  materializations:
    one:
      constraints:
        flow_document: { type: 1, reason: "location required" }
        Int: { type: 1, reason: "location required" }

        int: { type: 5, reason: "field unsatisfiable" }
        str: { type: 4, reason: "field forbidden" }
        bit: { type: 0, reason: "field required" }
        Unknown: { type: 0, reason: "whoops" }
"#,
    );
}

#[test]
fn test_test_step_unknown_collection() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string-tests:
  tests:
    "A Test":
      - ingest:
          collection: testinG/Int-strin
          documents: []
      - verify:
          collection: wildly/Off/Name
          documents: []
"#,
    );
}

#[test]
fn test_test_step_ingest_schema_error() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string-tests:
  tests:
    "A Test":
      - ingest:
          collection: testing/int-string
          documents:
            - {int: 42, str_whoops: "string A", bit: true}
            - {int: 52, str_whoops: "string B", bit: true}
"#,
    );
}

#[test]
fn test_test_step_verify_key_order() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string-tests:
  tests:
    "A Test":
      - verify:
          collection: testing/int-string
          documents: [{int: 52}, {int: 62}, {int: 42}]
"#,
    );
}

#[test]
fn test_test_step_verify_selector() {
    run_test_errors(
        &GOLDEN,
        r#"
test://int-string-tests:
  tests:
    "A Test":
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
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MockDriverCalls {
    materializations: HashMap<String, MockMaterializationValidateCall>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MockMaterializationValidateCall {
    endpoint: source::EndpointType,
    config: serde_json::Value,
    #[serde(default)]
    constraints: HashMap<String, materialize::Constraint>,
    #[serde(default)]
    error: Option<String>,
}

impl validation::Drivers for MockDriverCalls {
    fn validate_materialization(
        &self,
        endpoint_type: source::EndpointType,
        endpoint_config: serde_json::Value,
        _request: materialize::ValidateRequest,
    ) -> LocalBoxFuture<Result<materialize::ValidateResponse, validation::BoxError>> {
        async move {
            for (_key, call) in &self.materializations {
                if (call.endpoint, &call.config) != (endpoint_type, &endpoint_config) {
                    continue;
                }

                if let Some(err) = &call.error {
                    return Err(err.as_str().into());
                } else {
                    return Ok(materialize::ValidateResponse {
                        constraints: call.constraints.clone(),
                    });
                }
            }
            return Err("driver fixture not found".into());
        }
        .boxed_local()
    }
}

fn run_test(mut fixture: Value) -> (source::Tables, validation::Tables) {
    // Extract out driver mock call fixtures.
    let mock_calls = fixture
        .get_mut("driver")
        .map(|d| d.take())
        .unwrap_or_default();
    let mock_calls: MockDriverCalls = serde_json::from_value(mock_calls).unwrap();

    let source_tables = source::scenarios::evaluate_fixtures(Default::default(), &fixture);
    if !source_tables.errors.is_empty() {
        eprint!("{:?}", &source_tables);
        panic!("unexpected fixture load error");
    }
    let built_tables = validation::validate(&mock_calls, &source_tables);

    (source_tables, built_tables)
}

fn run_test_errors(fixture: &Value, patch: &str) {
    let mut fixture = fixture.clone();
    let patch: Value = serde_yaml::from_str(patch).unwrap();
    json_patch::merge(&mut fixture, &patch);

    let (_, built_tables) = run_test(fixture);
    insta::assert_debug_snapshot!(built_tables.errors);
}
