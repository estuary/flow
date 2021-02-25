use futures::{future::LocalBoxFuture, FutureExt};
use lazy_static::lazy_static;
use models::tables;
use protocol::{flow, materialize};
use serde_json::Value;
use std::collections::HashMap;

lazy_static! {
    static ref GOLDEN: Value = serde_yaml::from_slice(include_bytes!("model.yaml")).unwrap();
}

#[test]
fn test_golden_all_visits() {
    let tables = run_test(GOLDEN.clone());
    insta::assert_debug_snapshot!(tables);
}

#[test]
fn test_database_round_trip() {
    let tables = run_test(GOLDEN.clone());

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
    run_test_errors(&GOLDEN, "{}");
}

#[test]
fn test_invalid_collection_names_prefixes_and_duplicates() {
    run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:
  collections:
    good: &spec
      schema: test://example/int-string.schema
      key: [/int]

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
}

#[test]
fn test_invalid_transform_names_and_duplicates() {
    run_test_errors(
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
}

#[test]
fn test_invalid_endpoint_names_and_duplicates() {
    run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:
  import:
    - test://example/more-endpoints
test://example/more-endpoints:
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
    CAPtUReEndpoINT: *spec
    Materializeendpoint: *spec
"#,
    );
}

#[test]
fn test_invalid_capture_names_prefixes_and_duplicates() {
    run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:
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
test://example/catalog.yaml:
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
}

#[test]
fn test_capture_target_not_found() {
    run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string-capture:
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
test://example/int-string-capture:
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
test://example/catalog.yaml:
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
test://example/int-string-capture:
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
test://example/catalog.yaml:
  captures:
    testing/int-string/another/pull:
      target:
        name: testing/int-string
      endpoint: { name: captureEndpoint }
"#,
    );
}

#[test]
fn test_materialization_duplicates() {
    run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:
  materializations:
    testing/int-halve-duplicate:
      source:
        name: testing/int-halve
      endpoint:
        name: materializeEndpoint
        config: { fixture: two }
"#,
    );
}

#[test]
fn test_use_without_import() {
    run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string:
  import: [] # Clear.

test://example/int-reverse:
  import: [] # Clear.
  endpoints:
    s3WithoutImport:
      s3:
        bucket: a-bucket
        prefix: and-prefix

test://example/int-string-materialization:
  import: [] # Clear.
  materializations:
    testing/int-string:
      endpoint:
        name: s3WithoutImport
"#,
    );
}

#[test]
fn test_schema_fragment_not_found() {
    run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string:
  collections:
    testing/int-string:
      schema: test://example/int-string.schema#/not/found

test://example/int-string-materialization:
  materializations:
    testing/int-string: null # Omit downstream errors.

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
}

#[test]
fn test_keyed_location_wrong_type() {
    run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string.schema:
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
}

#[test]
fn test_shuffle_key_length_mismatch() {
    run_test_errors(
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
}

#[test]
fn test_shuffle_key_types_mismatch() {
    run_test_errors(
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
}

#[test]
fn test_shuffle_key_empty() {
    run_test_errors(
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
}

#[test]
fn test_partition_selections() {
    run_test_errors(
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
}

#[test]
fn test_redundant_source_schema_and_shuffle() {
    run_test_errors(
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
}

#[test]
fn test_must_have_update_or_publish() {
    run_test_errors(
        &GOLDEN,
        r#"
test://example/int-reverse:
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
test://example/int-halve:
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
test://example/int-string-len.schema:
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
test://example/int-string-len.schema:
  $ref: test://example/int-string.schema#/whoops
"#,
    );
}

#[test]
fn test_materialization_source_not_found() {
    run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string-materialization:
  materializations:
    testing/int-string:
      source: { name: testiNg/int-strinK }

test://example/int-halve-materialization:
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
test://example/int-string-materialization:
  materializations:
    testing/int-string:
      endpoint: { name: MaterializeEndpoit }

test://example/int-halve-materialization:
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
test://example/int-halve-materialization:
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
test://example/int-string-materialization:
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
test://example/int-string-tests:
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
test://example/int-string-tests:
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
test://example/int-string-tests:
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
test://example/int-string-tests:
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

#[test]
fn test_duplicate_named_schema() {
    run_test_errors(
        &GOLDEN,
        r#"
# Repeat a named anchor, in a different schema.
test://example/int-string-len.schema:
  $defs:
    anAnchor:
      $anchor: AnAnchor
      type: string
"#,
    );
}

#[test]
fn test_incompatible_npm_packages() {
    run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:
  npmDependencies:
    package-one: "same"
    pkg-2: "different"
    package-4: "4"

test://example/int-string:
  npmDependencies:
    package-one: "same"
    pkg-2: "differ ent"
    pkg-three: "3"
"#,
    );
}

#[test]
fn test_duplicate_journal_rule() {
    run_test_errors(
        &GOLDEN,
        r#"
test://example/int-string:
  journalRules:
    123 A Rule:
      template: {}
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
    endpoint: flow::EndpointType,
    config: serde_json::Value,
    #[serde(default)]
    constraints: HashMap<String, materialize::Constraint>,
    #[serde(default)]
    error: Option<String>,
}

impl validation::Drivers for MockDriverCalls {
    fn validate_materialization<'a>(
        &'a self,
        request: materialize::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<materialize::ValidateResponse, anyhow::Error>> {
        async move {
            let endpoint_config: serde_json::Value =
                serde_json::from_str(&request.endpoint_config_json)?;

            for (_key, call) in &self.materializations {
                if (call.endpoint as i32, &call.config) != (request.endpoint_type, &endpoint_config)
                {
                    continue;
                }

                if let Some(err) = &call.error {
                    return Err(anyhow::anyhow!("{}", err));
                } else {
                    return Ok(materialize::ValidateResponse {
                        constraints: call.constraints.clone(),
                    });
                }
            }
            return Err(anyhow::anyhow!("driver fixture not found"));
        }
        .boxed_local()
    }
}

fn run_test(mut fixture: Value) -> tables::All {
    // Extract out driver mock call fixtures.
    let mock_calls = fixture
        .get_mut("driver")
        .map(|d| d.take())
        .unwrap_or_default();
    let mock_calls: MockDriverCalls = serde_json::from_value(mock_calls).unwrap();

    let sources::Tables {
        captures,
        collections,
        derivations,
        endpoints,
        mut errors,
        fetches,
        imports,
        journal_rules,
        materializations,
        named_schemas,
        npm_dependencies,
        mut projections,
        resources,
        schema_docs,
        test_steps,
        transforms,
    } = sources::scenarios::evaluate_fixtures(Default::default(), &fixture);

    let validation::Tables {
        built_collections,
        built_derivations,
        built_materializations,
        built_tests,
        built_transforms,
        errors: validation_errors,
        implicit_projections,
        inferences,
    } = futures::executor::block_on(validation::validate(
        &mock_calls,
        &captures,
        &collections,
        &derivations,
        &endpoints,
        &imports,
        &journal_rules,
        &materializations,
        &named_schemas,
        &npm_dependencies,
        &projections,
        &resources,
        &schema_docs,
        &test_steps,
        &transforms,
    ));

    errors.extend(validation_errors.into_iter());
    projections.extend(implicit_projections.into_iter());

    tables::All {
        built_collections,
        built_derivations,
        built_materializations,
        built_tests,
        built_transforms,
        captures,
        collections,
        derivations,
        endpoints,
        errors,
        fetches,
        imports,
        inferences,
        journal_rules,
        materializations,
        named_schemas,
        npm_dependencies,
        projections,
        resources,
        schema_docs,
        test_steps,
        transforms,
    }
}

fn run_test_errors(fixture: &Value, patch: &str) {
    let mut fixture = fixture.clone();
    let patch: Value = serde_yaml::from_str(patch).unwrap();
    json_patch::merge(&mut fixture, &patch);

    let tables::All { errors, .. } = run_test(fixture);
    insta::assert_debug_snapshot!(errors);
}
