use json::schema::types;
use proto_flow::flow::collection_spec::derivation::ShuffleType;
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
    #[error(
        "{lhs_entity} {lhs_name} {error_class} {rhs_entity} {rhs_name}, defined at {rhs_scope}"
    )]
    NameCollision {
        error_class: &'static str,
        lhs_entity: &'static str,
        lhs_name: String,
        rhs_entity: &'static str,
        rhs_name: String,
        rhs_scope: Url,
    },
    #[error("{ref_entity} {ref_name}, referenced by {this_entity}, is not defined")]
    NoSuchEntity {
        this_entity: String,
        ref_entity: &'static str,
        ref_name: String,
    },
    #[error("{ref_entity} {ref_name}, referenced by {this_entity}, is not defined; did you mean {suggest_name} defined at {suggest_scope} ?")]
    NoSuchEntitySuggest {
        this_entity: String,
        ref_entity: &'static str,
        ref_name: String,
        suggest_name: String,
        suggest_scope: Url,
    },
    #[error("at least one storage mapping must be defined")]
    NoStorageMappings {},
    /// This comes from a validation that ensures users cannot specify the `endpoint` property of a Store that pertains
    /// to the 'default/' prefix. This is because the prefix is used to look up the AWS credentials for each store that
    /// uses a custom endpoint, but the 'default' profile is always used for Flow's own credentials. Therefore, allowing
    /// a user to specify a custom endpoint for that profile could result in Flow's own credentials being sent to a
    /// malicious endpoint. This also pertains to the empty storage prefix, which is also disallowed for custom storage endpoints.
    #[error("'custom' storage mapping '{prefix}' is not allowed under the {disallowed} prefix")]
    InvalidCustomStoragePrefix {
        prefix: String,
        disallowed: &'static str, // will either be "empty" or "'default/'"
    },
    #[error("could not map {this_entity} {this_thing} into a storage mapping; did you mean {suggest_name} defined at {suggest_scope}?")]
    NoStorageMappingSuggest {
        this_thing: String,
        this_entity: &'static str,
        suggest_name: String,
        suggest_scope: Url,
    },
    #[error("{this_thing} references {ref_entity} {ref_name}, defined at {ref_scope}, without importing it or being imported by it")]
    MissingImport {
        this_thing: String,
        ref_entity: &'static str,
        ref_name: String,
        ref_scope: Url,
    },
    #[error("collection {collection} must define either `schema` or both of `writeSchema` and `readSchema`")]
    InvalidSchemaCombination { collection: String },
    #[error("referenced schema fragment location {schema} does not exist")]
    NoSuchSchema { schema: Url },
    #[error("collection {collection} key cannot be empty (https://go.estuary.dev/Zq6zVB)")]
    CollectionKeyEmpty { collection: String },
    #[error("collection schema {schema} must have type 'object'")]
    CollectionSchemaNotObject { schema: Url },
    #[error("{ptr} is not a valid JSON pointer (missing leading '/' slash)")]
    KeyMissingLeadingSlash { ptr: String },
    #[error("{ptr} is not a valid JSON pointer ({unmatched:?} is invalid)")]
    KeyRegex { ptr: String, unmatched: String },
    #[error("keyed location {ptr} must be required to exist, but is not required within schema {schema}")]
    KeyMayNotExist { ptr: String, schema: Url },
    #[error("location {ptr} is prohibited from ever existing by the schema {schema}")]
    KeyCannotExist { ptr: String, schema: Url },
    #[error("location {ptr} accepts {type_:?} in schema {schema}, but locations used as keys may only be null-able integers, strings, or booleans")]
    KeyWrongType {
        ptr: String,
        type_: types::Set,
        schema: Url,
    },
    #[error("location {ptr} is unknown in schema {schema}")]
    KeyIsImplicit { ptr: String, schema: Url },
    #[error("location {ptr} has a reduction strategy {strategy:?}, which is disallowed because the location is used as a key")]
    KeyHasReduction {
        ptr: String,
        schema: Url,
        strategy: doc::inference::Reduction,
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
    #[error("projection {field} is the canonical field name of location {canonical_ptr:?}, and cannot re-map it to {wrong_ptr:?}")]
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
    #[error("transform {transform} shuffle key is already the collection key of {collection} and should be omitted here")]
    ShuffleKeyNotDifferent {
        transform: String,
        collection: String,
    },
    #[error(
        "cannot infer shuffle key types because all transforms use computed shuffle lambdas. Please add an explicit `shuffleKeyTypes` to this derivation."
    )]
    ShuffleKeyCannotInfer {},
    #[error("transform {transform} shuffle key cannot be empty")]
    ShuffleKeyEmpty { transform: String },
    #[error("transform {lhs_name} shuffled key types {lhs_types:?} don't align with transform {rhs_name} types {rhs_types:?}")]
    ShuffleKeyImplicitMismatch {
        lhs_name: String,
        lhs_types: Vec<ShuffleType>,
        rhs_name: String,
        rhs_types: Vec<ShuffleType>,
    },
    #[error("transform {name} shuffled key types {types:?} don't align with declared shuffle key types {given_types:?}")]
    ShuffleKeyExplicitMismatch {
        name: String,
        types: Vec<ShuffleType>,
        given_types: Vec<ShuffleType>,
    },
    #[error("error while extracting metadata from the connector image '{image}'")]
    ImageInspectFailed {
        image: String,
        #[source]
        detail: anyhow::Error,
    },
    #[error("connector error while validating capture {name}")]
    CaptureConnector {
        name: String,
        #[source]
        detail: anyhow::Error,
    },
    #[error("connector error while validating derivation {name}")]
    DeriveConnector {
        name: String,
        #[source]
        detail: anyhow::Error,
    },
    #[error("connector error while validating materialization {name}")]
    MaterializationConnector {
        name: String,
        #[source]
        detail: anyhow::Error,
    },
    #[error("error while resolving referenced collections from the control plane")]
    ResolveCollections {
        #[source]
        detail: anyhow::Error,
    },
    #[error("materialization {name} field {field} is not satisfiable ({reason})")]
    FieldUnsatisfiable {
        name: String,
        field: String,
        reason: String,
    },
    #[error(
        "materialization {name} has no acceptable field that satisfies required location {location}"
    )]
    LocationUnsatisfiable { name: String, location: String },
    #[error("documents to verify are not in collection key order")]
    TestVerifyOrder,
    #[error("test ingest document is invalid against the collection schema: {}", serde_json::to_string_pretty(.0).unwrap())]
    IngestDocInvalid(doc::FailedValidation),
    #[error("{entity} {name} bindings duplicate the endpoint resource {resource} at {rhs_scope}")]
    BindingDuplicatesResource {
        entity: &'static str,
        name: String,
        resource: String,
        rhs_scope: Url,
    },
    #[error(transparent)]
    SchemaBuild(#[from] json::schema::build::Error),
    #[error(transparent)]
    SchemaIndex(#[from] json::schema::index::Error),
    #[error(transparent)]
    SchemaShape(#[from] doc::inference::Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

impl Error {
    pub fn push(self, scope: sources::Scope, errors: &mut tables::Errors) {
        errors.insert_row(scope.flatten(), anyhow::anyhow!(self));
    }
}
