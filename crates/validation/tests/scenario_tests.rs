use futures::{future::LocalBoxFuture, FutureExt};
use lazy_static::lazy_static;
use models::tables;
use protocol::{capture, flow, materialize};
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
fn test_invalid_capture_names_and_duplicates() {
    run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:
  import:
    - test://example/captures
test://example/captures:
  captures:
    good: &spec
      endpoint:
        airbyteSource:
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
}

#[test]
fn test_invalid_materialization_names_and_duplicates() {
    run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:
  import:
    - test://example/materializations
test://example/materializations:
  materializations:
    good: &spec
      endpoint:
        flowSink:
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
}

#[test]
fn test_cross_entity_name_prefixes_and_duplicates() {
    run_test_errors(
        &GOLDEN,
        r#"
test://example/catalog.yaml:

  collections:
    a/b/1: &collection_spec
      schema: test://example/int-string.schema
      key: [/int]

    a/b/3/suffix: *collection_spec
    a/b/2: *collection_spec

  materializations:
    a/b/2: &materialization_spec
      endpoint:
        flowSink:
          image: an/image
          config: { a: config }
      bindings: []

    a/b/1/suffix: *materialization_spec
    a/b/3: *materialization_spec

  captures:
    a/b/3: &capture_spec
      endpoint:
        airbyteSource:
          image: an/image
          config: { a: value }
      bindings: []

    a/b/1: *capture_spec
    a/b/2/suffix: *capture_spec
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
}

#[test]
fn test_capture_target_is_derivation_and_missing_imports() {
    run_test_errors(
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
}

#[test]
fn test_capture_duplicates() {
    run_test_errors(
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
}

#[test]
fn test_materialization_duplicates() {
    run_test_errors(
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

test://example/webhook-deliveries:
  import: [] # Clear.
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
}

#[test]
fn test_materialization_field_errors() {
    run_test_errors(
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
}

#[test]
fn test_capture_driver_returns_error() {
    run_test_errors(
        &GOLDEN,
        r#"
driver:
  captures:
    testing/s3-source:
      error: "A driver error!"
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
    testing/webhook/deliveries:
      bindings: []
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
    testing/webhook/deliveries:
      bindings:
        - constraints:
            flow_document: { type: 1, reason: "location required" }
            int: { type: 98, reason: "other whoops" }
          resourcePath: [tar!get, one]
        - constraints:
            flow_document: { type: 1, reason: "location required" }
            str: { type: 99, reason: "whoops" }
          resourcePath: [tar!get, two]
"#,
    );
}

#[test]
fn test_materialization_driver_conflicts() {
    run_test_errors(
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
fn test_materialization_selector() {
    run_test_errors(
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
                .map(|b| materialize::validate_response::Binding {
                    constraints: b.constraints.clone(),
                    delta_updates: call.delta_updates,
                    resource_path: b.resource_path.clone(),
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

fn run_test(mut fixture: Value) -> tables::All {
    // Extract out driver mock call fixtures.
    let mock_calls = fixture
        .get_mut("driver")
        .map(|d| d.take())
        .unwrap_or_default();
    let mock_calls: MockDriverCalls = serde_json::from_value(mock_calls).unwrap();

    let sources::Tables {
        capture_bindings,
        captures,
        collections,
        derivations,
        mut errors,
        fetches,
        imports,
        journal_rules,
        materialization_bindings,
        materializations,
        named_schemas,
        npm_dependencies,
        mut projections,
        resources,
        schema_docs,
        shard_rules,
        test_steps,
        transforms,
    } = sources::scenarios::evaluate_fixtures(Default::default(), &fixture);

    let validation::Tables {
        built_captures,
        built_collections,
        built_derivations,
        built_materializations,
        built_tests,
        errors: validation_errors,
        implicit_projections,
        inferences,
    } = futures::executor::block_on(validation::validate(
        &mock_calls,
        &capture_bindings,
        &captures,
        &collections,
        &derivations,
        &imports,
        &journal_rules,
        &materialization_bindings,
        &materializations,
        &named_schemas,
        &npm_dependencies,
        &projections,
        &resources,
        &schema_docs,
        &shard_rules,
        &test_steps,
        &transforms,
    ));

    errors.extend(validation_errors.into_iter());
    projections.extend(implicit_projections.into_iter());

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
        journal_rules,
        materialization_bindings,
        materializations,
        named_schemas,
        npm_dependencies,
        projections,
        resources,
        schema_docs,
        shard_rules,
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
