use json::schema::types;
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
    #[error("{ref_entity} {ref_name}, referenced by {this_thing}, is not defined")]
    NoSuchEntity {
        this_thing: String,
        ref_entity: &'static str,
        ref_name: String,
    },
    #[error("{ref_entity} {ref_name}, referenced by {this_thing}, is not defined; did you mean {suggest_name} defined at {suggest_scope}?")]
    NoSuchEntitySuggest {
        this_thing: String,
        ref_entity: &'static str,
        ref_name: String,
        suggest_name: String,
        suggest_scope: Url,
    },
    #[error("at least one storage mapping must be defined")]
    NoStorageMappings {},
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
    #[error("referenced schema fragment location {schema} does not exist")]
    NoSuchSchema { schema: Url },
    #[error("collection {collection} key cannot be empty (https://go.estuary.dev/Zq6zVB)")]
    CollectionKeyEmpty { collection: String },
    #[error("collection {collection} schema must be an object")]
    CollectionSchemaNotObject { collection: String },
    #[error("{ptr} is not a valid JSON pointer (missing leading '/' slash)")]
    KeyMissingLeadingSlash { ptr: String },
    #[error("{ptr} is not a valid JSON pointer ({unmatched:?} is invalid)")]
    KeyRegex { ptr: String, unmatched: String },
    #[error("keyed location {ptr} must be required to exist by schema {schema} (https://go.estuary.dev/KUYbal)")]
    KeyMayNotExist { ptr: String, schema: Url },
    #[error(
        "location {ptr} can never exist within schema {schema} (https://go.estuary.dev/L3m1y9)"
    )]
    KeyCannotExist { ptr: String, schema: Url },
    #[error("location {ptr} accepts {type_:?} in schema {schema}, but {disallowed:?} is disallowed in locations used as keys (https://go.estuary.dev/CigSvN)")]
    KeyWrongType {
        ptr: String,
        type_: types::Set,
        disallowed: types::Set,
        schema: Url,
    },
    #[error("location {ptr} is unknown in schema {schema} (https://go.estuary.dev/rdCMNB)")]
    KeyIsImplicit { ptr: String, schema: Url },
    #[error("keyed location {ptr} has a disallowed {strategy:?} reduction strategy (https://go.estuary.dev/V5RRHc)")]
    KeyHasReduction {
        ptr: String,
        schema: Url,
        strategy: doc::inference::Reduction,
    },
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
    #[error("transform {transform} shuffle key is already the key of {collection} and should be omitted here")]
    ShuffleKeyNotDifferent {
        transform: String,
        collection: String,
    },
    #[error("transform {transform} shuffle key cannot be empty")]
    ShuffleKeyEmpty { transform: String },
    #[error("transform {transform} must set at least one of 'update' or 'publish' lambdas")]
    NoUpdateOrPublish { transform: String },
    #[error("derivation defines a TypeScript module but uses no TypeScript lambdas")]
    TypescriptModuleWithoutLambdas,
    #[error("derivation uses TypeScript lambdas but defines no TypeScript module")]
    TypescriptLambdasWithoutModule,
    #[error("TypeScript module {module} must be unique to one derivation, but is used here by {lhs_derivation} and also by {rhs_derivation} at {rhs_scope}")]
    TypescriptModuleNotUnique {
        module: Url,
        lhs_derivation: String,
        rhs_derivation: String,
        rhs_scope: Url,
    },
    #[error("driver error while validating capture {name}")]
    CaptureDriver {
        name: String,
        #[source]
        detail: anyhow::Error,
    },
    #[error("driver error while validating materialization {name}")]
    MaterializationDriver {
        name: String,
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
    #[error("{entity} {name} bindings duplicate the endpoint resource {resource} at {rhs_scope}")]
    BindingDuplicatesResource {
        entity: &'static str,
        name: String,
        resource: String,
        rhs_scope: Url,
    },

    #[error("one or more JSON schemas has errors which prevent further validation checks")]
    SchemaBuild,
    #[error(transparent)]
    SchemaIndex(#[from] json::schema::index::Error),
    #[error(transparent)]
    SchemaShape(#[from] doc::inference::Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

impl Error {
    pub fn push(self, scope: &url::Url, errors: &mut tables::Errors) {
        errors.insert_row(scope, anyhow::anyhow!(self));
    }
}
