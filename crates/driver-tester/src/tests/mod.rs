mod functional;
//mod transactional;

use crate::request::{
    new_transaction, FieldSelectionPointers, LoadReceiver, LoadSender, StoreResponseReceiver,
    StoreSender,
};
use crate::test_doc::{self, TestDoc};
use crate::{DriverClientImpl, Fixture, TestResult};
use itertools::Itertools;
use protocol::{
    flow::CollectionSpec,
    materialize::{
        constraint::Type, ApplyRequest, Constraint, FieldSelection, SessionRequest, ValidateRequest,
    },
};
use rand::Rng;
use serde_json::{json, Value};
use tracing::{debug, trace};

use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Display};

/// Enum representing each of the test cases that may be run.
#[derive(Debug, Copy, Clone, PartialEq, Hash)]
pub enum TestCase {
    Functional,
    Transactional,
}

/// The list of all test cases, which represents the default tests to run when no `--test`
/// arguments are given.
pub const ALL_TESTS: &[TestCase] = &[TestCase::Functional, TestCase::Transactional];

impl std::str::FromStr for TestCase {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "functional" => Ok(Self::Functional),
            "transactional" => Ok(Self::Transactional),
            _ => Err(anyhow::anyhow!("no such test case: '{}'", s)),
        }
    }
}

impl Display for TestCase {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let name = match *self {
            TestCase::Functional => "functional",
            TestCase::Transactional => "transactional",
        };
        f.write_str(name)
    }
}

#[tracing::instrument(level = "info")]
pub async fn run(test: TestCase, fixture: &mut Fixture) -> TestResult {
    let start = std::time::Instant::now();
    let result = match test {
        TestCase::Functional => functional::test(fixture).await,
        TestCase::Transactional => unimplemented!(), // transactional::test(fixture).await,
    };
    TestResult {
        name: test.to_string(),
        duration: start.elapsed(),
        error: result.err(),
    }
}

/// A CollectionFixture represents a fixture that is tied to a specific materialization.
pub struct MaterializationFixture {
    /// The spec of the collection being materialized.
    pub spec: CollectionSpec,
    /// The specific Fields being materialized.
    pub fields: FieldSelection,
    /// The parsed json pointers corresponding to the `fields` selected. This is used to exactact
    /// keys and values from documents, and to randomly generate new json documents.
    pub field_pointers: FieldSelectionPointers,
}

impl MaterializationFixture {
    // Runs through the steps for applying the materialization. This is pretty close to what
    // flowctl will do during the build and apply steps, but combined into a single function.
    // This will automatically select all recommended fields for materialization, and will return
    // an error if the constraints returned by the driver prohibit building a valid FieldSelection.
    pub async fn exec_setup(
        fixture: &mut Fixture,
        spec: CollectionSpec,
        apply_dry_run: bool,
    ) -> anyhow::Result<MaterializationFixture> {
        let start_session = SessionRequest {
            endpoint_url: fixture.endpoint.clone(),
            target: fixture.target.clone(),
            shard_id: String::new(),
        };
        // We intentionally don't return this handle in order to emulate the expected behavior in
        // real world usage, where flowctl will execute validation and apply calls using an empty
        // shard_id, and the materialization consumer will use a separate session.
        let initial_handle = fixture
            .client
            .start_session(start_session)
            .await?
            .into_inner()
            .handle;

        let validate_req = ValidateRequest {
            handle: initial_handle.clone(),
            collection: Some(spec.clone()),
        };
        let constraints = fixture
            .client
            .validate(validate_req)
            .await?
            .into_inner()
            .constraints;
        debug!("validate constraints: {:?}", constraints);

        let selected_fields = build_field_selection(&constraints)?;
        debug!("resolved selected fields: {:?}", selected_fields);
        let field_selection_pointers = FieldSelectionPointers::new(&selected_fields, &spec)?;

        let collection_fixture = MaterializationFixture {
            spec,
            fields: selected_fields,
            field_pointers: field_selection_pointers,
        };

        let apply_req = collection_fixture.new_apply_req(initial_handle, apply_dry_run);
        let action_description = fixture
            .client
            .apply(apply_req)
            .await?
            .into_inner()
            .action_description;
        debug!(
            "successfully applied materialization with dry_run: {}, action_description: \n{}",
            apply_dry_run, action_description
        );

        Ok(collection_fixture)
    }

    /// Generates and returns a number of TestDocs. The returned documents will all have their
    /// `exists` field set to `false`. This assumption is overwhelmingly likely to be correct for
    /// collections that define non-null keys with significant enough length. But it's likely to
    /// be incorrect if used with a collection with nullable keys, so use cautiously.
    pub fn rand_test_docs(&self, count: usize, rng: &mut impl Rng) -> Vec<TestDoc> {
        test_doc::rand_test_docs(rng, &self.spec.projections, count)
    }

    /// Starts a new transaction rpc and returns the sender and receiver for the bidirectional
    /// stream.
    pub async fn start_transaction(
        &self,
        client: &mut DriverClientImpl,
        handle: Vec<u8>,
        flow_checkpoint: Vec<u8>,
    ) -> anyhow::Result<(LoadSender, LoadReceiver)> {
        let (tx, rx) = new_transaction(client.clone()).await;
        let tx = tx
            .send_start(handle, self.fields.clone(), flow_checkpoint)
            .await?;
        Ok((tx, rx))
    }

    /// Returns an ApplyRequest for applying this materialization.
    pub fn new_apply_req(&self, handle: Vec<u8>, dry_run: bool) -> ApplyRequest {
        ApplyRequest {
            handle,
            collection: Some(self.spec.clone()),
            fields: Some(self.fields.clone()),
            dry_run,
        }
    }

    /// Attempts to load the `expected` documents and asserts that the returned documents match the
    /// expected. The LoadRequests are generated by extracting the keys from the `expected`
    /// documents, and verifying the LoadResponses is done by extracting the keys from the returned
    /// documents to match them up.
    ///
    /// This function accounts for drivers that do not support loads. If a prior value of the
    /// `always_empty_hint` is given, then this function will assert that the received
    /// `always_empty_hint` is consistent with the previous value. If a prior value is not known,
    /// then the `always_empty_hint` from the LoadEOF is simply trusted. If `always_empty_hint` is
    /// true, then it ensures that no documents were returned. If false, then it ensures that all
    /// expected documents are returned and match the expected.
    ///
    /// The `assert_json_matches` function is used to compare json documents. See the docs there
    /// for details.
    pub async fn verify_load(
        &self,
        mut tx: LoadSender,
        rx: LoadReceiver,
        prev_always_empty: Option<bool>,
        expected: &[TestDoc],
    ) -> anyhow::Result<(StoreSender, StoreResponseReceiver, bool)> {
        if prev_always_empty != Some(true) {
            tx.send_load(&self.spec, expected).await?;
        }
        let tx = tx.finish_loads().await?;
        debug!("sent loadEOF");
        let (rx, received_docs, always_empty) = rx.recv_all().await?;

        if always_empty {
            debug!("always_empty_hint was true, so assering that no documents were loaded");
            anyhow::ensure!(
                prev_always_empty != Some(false),
                "driver returned true always_empty_hint after previously returning false"
            );
            anyhow::ensure!(
                received_docs.is_empty(),
                "expected no documents returned since always_empty_hint was true, but got {} documents",
                received_docs.len()
            );
        } else {
            debug!(
                "load response(s) returned {} documents",
                received_docs.len()
            );
            trace!(
                "load response documents: [{}]",
                received_docs.iter().join(", ")
            );
            anyhow::ensure!(
                prev_always_empty != Some(true),
                "driver returned false always_empty_hint after previously returning true"
            );
            verify_loaded_documents(&self.field_pointers, expected, received_docs)?;
        }
        Ok((tx, rx, always_empty))
    }
}

/// Asserts that the loaded documents matche the expected documents. All expected documents must be
/// present in `loaded_docs`, and no extra documents are tolerated. Expected and actual documents
/// are matched by extracting their keys.
pub fn verify_loaded_documents(
    fields: &FieldSelectionPointers,
    expected: &[TestDoc],
    loaded_docs: impl IntoIterator<Item = impl Borrow<Value>>,
) -> Result<(), anyhow::Error> {
    let mut expected_by_key: HashMap<Vec<u8>, &'_ Value> = expected
        .into_iter()
        .map(|doc| {
            let k = fields.get_packed_key(doc.borrow());
            (k, doc.borrow())
        })
        .collect::<HashMap<_, _>>();

    // Track the keys we've observed in order to ensure that the response contains at most one
    // document per distinct key. This condition seems pretty unlikely, but for now I'm erring on
    // the side of caution.
    let mut observed_keys = HashSet::new();

    for actual in loaded_docs {
        let actual_doc = actual.borrow();
        let key = fields.get_packed_key(actual_doc);
        let display_key = tuple::Bytes::from(key.as_slice());

        if let Some((exp_key, expected)) = expected_by_key.remove_entry(&key) {
            if !observed_keys.insert(exp_key) {
                anyhow::bail!("got more than 1 document for the same key in a single load transaction. Key: {}", display_key);
            }
            assert_json_matches(&expected, &actual_doc)?;
        } else {
            anyhow::bail!("Got unexpected document: {}", actual_doc);
        }
    }
    // If there are any expected documents left, then try to format a reasonable error message.
    if !expected_by_key.is_empty() {
        anyhow::bail!(
            "Load Response missing expected documents: [{}]",
            expected_by_key.values().join(", ")
        );
    }

    Ok(())
}

/// Asserts that the actual json document matches the expected, and returns an error if there are
/// any significant differences. This function allows for a few minor discrepencies between the
/// actual and expected documents. First, it allows extra fields that appear in the actual
/// document, but not the expected. We might want to reconsider that in the future, but for now,
/// it's considered permissable for a Driver to make additive changes to a document. I'm
/// speculating that this may be needed to support transactional semantics in key-value stores that
/// don't support multi-document transactions. Secondly, `assert_json_matches` also applies an
/// epsilon when comparing floating point values, to allow for insignificant differences that may
/// be caused by different json parsers.
pub fn assert_json_matches(expected: &Value, actual: &Value) -> anyhow::Result<()> {
    let mut diff = Vec::new();
    doc::Diff::diff(
        Some(actual),
        Some(expected),
        &json::Location::Root,
        &mut diff,
    );
    if !diff.is_empty() {
        anyhow::bail!("expected document: {}, actual: {}", expected, actual);
    }
    Ok(())
}

pub fn assert_checkpoint_eq(
    expected: impl AsRef<[u8]>,
    actual: impl AsRef<[u8]>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        expected.as_ref() == actual.as_ref(),
        "invalid checkpoint, expected checkpoint: [{}], actual: [{}]",
        String::from_utf8_lossy(expected.as_ref()),
        String::from_utf8_lossy(actual.as_ref()),
    );
    Ok(())
}

// Builds a FieldSelection for the materialization, or returns an error if the constraints don't
// allow for a workable selection.
fn build_field_selection(
    constraints: &HashMap<String, Constraint>,
) -> Result<FieldSelection, anyhow::Error> {
    let mut fields = FieldSelection {
        keys: vec![String::from("id_one"), String::from("id_two")],
        values: Vec::new(),
        document: String::from("flow_document"),
    };
    for field in fields.keys.iter() {
        let constraint = constraints.get(field).ok_or_else(|| {
            anyhow::anyhow!(
                "validate response is missing constraint for key field '{}'",
                field
            )
        })?;
        if is_forbidden(constraint) {
            anyhow::bail!(
                "key field '{}' is forbidden by the validation constraint: {:?}",
                field,
                constraint
            );
        }
    }

    let constraint = constraints.get(&fields.document).ok_or_else(|| {
        anyhow::anyhow!(
            "validate response is missing constraint for root document field 'flow_document'"
        )
    })?;
    if is_forbidden(constraint) {
        anyhow::bail!(
            "root document field '{}' is forbidden by the validation constraint: {:?}",
            &fields.document,
            constraint
        );
    }

    // add any optional values fields
    for field in &["numValue", "intValue", "boolValue"] {
        // is the field allowed by the constraints?
        let constraint = constraints.get(*field);
        if !constraint.map(is_forbidden).unwrap_or(true) {
            fields.values.push(field.to_string());
        }
    }

    Ok(fields)
}

fn is_forbidden(constraint: &Constraint) -> bool {
    match Type::from_i32(constraint.r#type) {
        None => true, // unrecognized enum is something protobuf forces you to handle
        Some(Type::FieldForbidden) => true,
        Some(Type::Unsatisfiable) => true,
        Some(_) => false,
    }
}

fn test_collection() -> CollectionSpec {
    let spec_json = json!({
        "name": "functional-test-materialize",
        "schema_uri": "test://functional.test/materialize.json",
        "key_ptrs": ["/id1", "/id2"],
        "projections": [
            {
                "ptr": "/id1",
                "field": "id_one",
                "user_provided": true,
                "is_partition_key": false,
                "is_primary_key": true,
                "inference": {
                    "types": ["integer"],
                    "must_exist": true,
                    "title": "title of id_one",
                    "description": "description of id_one"
                }
            },
            {
                "ptr": "/id2",
                "field": "id_two",
                "user_provided": true,
                "is_partition_key": false,
                "is_primary_key": true,
                "inference": {
                    "types": ["string"],
                    "must_exist": true
                }
            },
            {
                "ptr": "/intValue",
                "field": "intValue",
                "user_provided": false,
                "is_partition_key": false,
                "is_primary_key": false,
                "inference": {
                    "types": ["integer", "null"],
                    "must_exist": true
                }
            },
            {
                "ptr": "/numValue",
                "field": "numValue",
                "user_provided": false,
                "is_partition_key": false,
                "is_primary_key": false,
                "inference": {
                    "types": ["number", "null"],
                    "must_exist": true
                }
            },
            {
                "ptr": "/boolValue",
                "field": "boolValue",
                "user_provided": false,
                "is_partition_key": false,
                "is_primary_key": false,
                "inference": {
                    "types": ["boolean"],
                    "must_exist": false
                }
            },
            {
                "ptr": "",
                "field": "flow_document",
                "user_provided": false,
                "is_partition_key": false,
                "is_primary_key": false,
                "inference": {
                    "types": ["object"],
                    "must_exist": true
                }
            }
        ]
    });

    serde_json::from_value(spec_json).unwrap()
}
