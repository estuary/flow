use super::ContentType;
use crate::catalog::extraction::KeyError;
use crate::doc;
use estuary_json::schema;
use itertools::Itertools;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("at {loc}")]
    At {
        loc: String,
        #[source]
        detail: Box<Error>,
    },
    #[error("failed to fetch resource {url}")]
    Fetch {
        url: url::Url,
        #[source]
        detail: Box<Error>,
    },
    #[error("don't know how to fetch this resource URI")]
    FetchNotSupported,

    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("joining '{relative}' with base URL '{base}': {detail}")]
    URLJoinErr {
        base: url::Url,
        relative: String,
        detail: url::ParseError,
    },
    #[error("failed to parse URL")]
    URLParseErr(#[from] url::ParseError),
    #[error("HTTP error (reqwest)")]
    ReqwestErr(#[from] reqwest::Error),
    #[error("failed to parse YAML")]
    YAMLErr(#[from] serde_yaml::Error),
    #[error("failed to merge YAML alias nodes")]
    YAMLMergeErr(#[from] yaml_merge_keys::MergeKeyError),

    #[error("failed to parse JSON")]
    JSONErr(#[from] serde_json::Error),
    #[error("catalog database error")]
    SQLiteErr(#[from] rusqlite::Error),
    #[error(
        "{source_uri:?} references {import_uri:?} without directly or indirectly importing it"
    )]
    MissingImport {
        source_uri: String,
        import_uri: String,
    },
    #[error("{source_uri:?} imports {import_uri:?}, but {import_uri:?} already transitively imports {source_uri:?}")]
    CyclicImport {
        source_uri: String,
        import_uri: String,
    },
    #[error("resource has content-type {next}, but is already registered with type {prev}")]
    ContentTypeMismatch {
        next: ContentType,
        prev: ContentType,
    },
    #[error("failed to build schema")]
    SchemaBuildErr(#[from] schema::build::Error),
    #[error("schema indexing error")]
    SchemaIndexErr(#[from] schema::index::Error),
    #[error("subprocess {process:?} failed with status {status}")]
    SubprocessFailed {
        process: std::path::PathBuf,
        status: std::process::ExitStatus,
    },
    #[error("Invalid collection keys: \n{}", .0.iter().join("\n"))]
    InvalidCollectionKeys(Vec<KeyError>),
    #[error("schema validation error: {}", serde_json::to_string_pretty(.0).unwrap())]
    FailedValidation(doc::FailedValidation),

    #[error(transparent)]
    InvalidProjection(#[from] crate::catalog::projections::NoSuchLocationError),

    #[error(
        "Materialization references the collection: '{collection_name}', which does not exist"
    )]
    MaterializationCollectionMissing { collection_name: String },
}
