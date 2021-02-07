use super::ContentType;
use crate::extraction::KeyError;
use crate::test_case::TestVerifyOutOfOrder;
use doc::{self, inference};
use itertools::Itertools;
use json::schema;
use std::fmt;

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
    #[error("inferred error in schema {schema_uri}")]
    SchemaInferenceErr {
        schema_uri: url::Url,
        #[source]
        detail: inference::Error,
    },
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
    InvalidProjection(#[from] crate::projections::NoSuchLocationError),

    #[error(
        "Materialization references the collection: '{collection_name}', which does not exist"
    )]
    MaterializationCollectionMissing { collection_name: String },

    #[error("the derived collection '{collection_name}' cannot name itself as a source in transform '{transform_name}'")]
    DerivationReadsItself {
        collection_name: String,
        transform_name: String,
    },
    #[error("Transforms have mismatched shuffles: {}", serde_json::to_string_pretty(.0).unwrap())]
    TransformShuffleMismatch(serde_json::Value),

    #[error(transparent)]
    NoSuchEntity(#[from] NoSuchEntity),

    #[error("The --source-catalog cannot be used because it does not contain a source resource")]
    MissingSourceResource,

    // TODO: think of a better error message
    #[error("Invalid --catalog was not built successfully")]
    CatalogNotBuilt,

    #[error("The --catalog was not built by this version of flowctl. Catalog version: '{0}'")]
    CatalogVersionMismatch(String),

    #[error(transparent)]
    TestInvalid(#[from] TestVerifyOutOfOrder),
}

impl Error {
    /// Removes all wrapping `At` variants and returns the inner error. If the error is already not
    /// located, then this just returns self.
    #[cfg(test)]
    pub fn unlocate(self) -> Error {
        match self {
            Error::At { detail, .. } => detail.unlocate(),
            other => other,
        }
    }

    pub fn missing_collection(given_name: String, closest_match: Option<(String, i64)>) -> Error {
        Error::NoSuchEntity(NoSuchEntity::collection(given_name, closest_match))
    }
}

#[derive(Debug)]
pub struct NoSuchEntity {
    entity_type: &'static str,
    given_name: String,
    closest_match: Option<String>,
}

impl NoSuchEntity {
    const COLLECTION: &'static str = "collection";

    pub fn collection(given_name: String, closest_match: Option<(String, i64)>) -> NoSuchEntity {
        let closest_match = closest_match.and_then(|(name, edit_dist)| {
            // Only suggest the closest match if it's closer than this threshold. This is to
            // prevent us showing silly suggestions in cases where there's nothing even remotely
            // close. The value chosen for this threshold is arbitrary, with the goal that it more
            // or less matches a subjective assessment of whether or not the suggestion would be
            // useful.
            if edit_dist <= name.len().min(4) as i64 {
                Some(name)
            } else {
                None
            }
        });
        NoSuchEntity {
            entity_type: NoSuchEntity::COLLECTION,
            given_name,
            closest_match,
        }
    }
}

impl fmt::Display for NoSuchEntity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "no {} found with name: '{}'.",
            self.entity_type, self.given_name
        )?;
        if let Some(suggestion) = self.closest_match.as_deref() {
            write!(f, " Closest match is: '{}'.", suggestion)?;
        }
        Ok(())
    }
}

impl std::error::Error for NoSuchEntity {}
