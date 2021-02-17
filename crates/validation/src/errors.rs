use json::schema::types;
use models::tables;
use url::Url;

#[must_use]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{entity} name cannot be empty")]
    NameEmpty { entity: &'static str },
    #[error("{name} cannot be used as name for {entity} ({unmatched:?} is invalid)")]
    NameRegex {
        entity: &'static str,
        name: String,
        unmatched: String,
    },
    #[error("{entity} {lhs} has a duplicated definition at {rhs_scope}")]
    Duplicate {
        entity: &'static str,
        lhs: String,
        rhs_scope: Url,
    },
    #[error("{entity} {lhs} is a prohibited prefix of {rhs}, defined at {rhs_scope}")]
    Prefix {
        entity: &'static str,
        lhs: String,
        rhs: String,
        rhs_scope: Url,
    },
    #[error("{ref_entity} {ref_name}, referenced by {this_entity} {this_name}, is not defined")]
    NoSuchEntity {
        this_entity: &'static str,
        this_name: String,
        ref_entity: &'static str,
        ref_name: String,
    },
    #[error("{ref_entity} {ref_name}, referenced by {this_entity} {this_name}, is not defined; did you mean {suggest_name} defined at {suggest_scope}?")]
    NoSuchEntitySuggest {
        this_entity: &'static str,
        this_name: String,
        ref_entity: &'static str,
        ref_name: String,
        suggest_name: String,
        suggest_scope: Url,
    },
    #[error("{this_entity} {this_name} references {ref_entity} {ref_name}, defined at {ref_scope}, without importing it or being imported by it")]
    MissingImport {
        this_entity: &'static str,
        this_name: String,
        ref_entity: &'static str,
        ref_name: String,
        ref_scope: Url,
    },
    #[error("referenced schema fragment location {schema} does not exist")]
    NoSuchSchema { schema: Url },
    #[error(
        "keyed location {ptr} (having type {type_:?}) must be required to exist by schema {schema}"
    )]
    KeyMayNotExist {
        ptr: String,
        type_: types::Set,
        schema: Url,
    },
    #[error("location {ptr} accepts {type_:?} in schema {schema}, but {disallowed:?} is disallowed in locations used as keys")]
    KeyWrongType {
        ptr: String,
        type_: types::Set,
        disallowed: types::Set,
        schema: Url,
    },
    #[error("location {ptr} is unknown in schema {schema}")]
    NoSuchPointer { ptr: String, schema: Url },
    #[error("transform {lhs_name} shuffled key types {lhs_types:?} don't align with transform {rhs_name} types {rhs_types:?}")]
    ShuffleKeyMismatch {
        lhs_name: String,
        lhs_types: Vec<types::Set>,
        rhs_name: String,
        rhs_types: Vec<types::Set>,
    },
    #[error("{category} projection {field} does not exist in collection {collection}")]
    NoSuchProjection {
        category: String,
        field: String,
        collection: String,
    },
    #[error("{category} projection {field} of collection {collection} is not a partition")]
    ProjectionNotPartitioned {
        category: String,
        field: String,
        collection: String,
    },
    #[error("projection {field} is the canonical field name of location {canonical_ptr}, and cannot re-map it to {wrong_ptr}")]
    ProjectionRemapsCanonicalField {
        field: String,
        canonical_ptr: String,
        wrong_ptr: String,
    },
    #[error("{category} partition selector field {field} value {value} is incompatible with the projections type, {type_:?}")]
    SelectorTypeMismatch {
        category: String,
        field: String,
        value: String,
        type_: types::Set,
    },
    #[error("{category} partition selector field {field} cannot be an empty string")]
    SelectorEmptyString { category: String, field: String },
    #[error(
        "source schema {schema} is already the schema of {collection} and should be omitted here"
    )]
    SourceSchemaNotDifferent { schema: Url, collection: String },
    #[error("transform {transform} shuffle key is already the key of {collection} and should be omitted here")]
    ShuffleKeyNotDifferent {
        transform: String,
        collection: String,
    },
    #[error("transform {transform} shuffle key cannot be empty")]
    ShuffleKeyEmpty { transform: String },
    #[error("{type_:?} is not a supported endpoint type for a capture")]
    CaptureEndpointType { type_: protocol::flow::EndpointType },
    #[error("must set at least one of 'update' or 'publish' lambdas")]
    NoUpdateOrPublish { transform: String },
    #[error("capture {capture} cannot capture into derived collection {derivation}")]
    CaptureOfDerivation { capture: String, derivation: String },
    #[error("captures {lhs_name} and {rhs_name} (at {rhs_scope}) have the same target {target}, endpoint, and configuration")]
    CaptureMultiplePulls {
        lhs_name: String,
        rhs_name: String,
        rhs_scope: Url,
        target: String,
    },
    #[error("materializations {lhs_name} and {rhs_name} (at {rhs_scope}) have the same target {target}, endpoint, and configuration")]
    MaterializationMultiplePushes {
        lhs_name: String,
        rhs_name: String,
        rhs_scope: Url,
        target: String,
    },
    #[error("driver error while validating materialization {name}")]
    MaterializationDriver {
        name: String,
        #[source]
        detail: anyhow::Error,
    },
    #[error("materialization {materialization} field {field} is not satisfiable ({reason})")]
    FieldUnsatisfiable {
        materialization: String,
        field: String,
        reason: String,
    },
    #[error(
        "materialization {materialization} has no acceptable field that satisfies required location {location}"
    )]
    LocationUnsatisfiable {
        materialization: String,
        location: String,
    },
    #[error("documents to verify are not in collection key order")]
    TestVerifyOrder,
    #[error("package {package} is repeated with incompatible versions {lhs_version:?} here, vs {rhs_version:?} at {rhs_scope}")]
    NPMVersionsIncompatible {
        package: String,
        lhs_version: String,
        rhs_version: String,
        rhs_scope: Url,
    },

    #[error("derivation's initial register is invalid against its schema: {}", serde_json::to_string_pretty(.0).unwrap())]
    RegisterInitialInvalid(doc::FailedValidation),
    #[error("test ingest document is invalid against the collection schema: {}", serde_json::to_string_pretty(.0).unwrap())]
    IngestDocInvalid(doc::FailedValidation),

    #[error(transparent)]
    SchemaIndex(#[from] json::schema::index::Error),
    #[error(transparent)]
    SchemaShape(#[from] doc::inference::Error),
}

impl Error {
    pub fn push(self, scope: &url::Url, errors: &mut tables::Errors) {
        errors.push_row(scope, anyhow::anyhow!(self));
    }
}
