use crate::catalog;
use estuary_json::schema::index;
use http;
use hyper;
use thiserror;
use tokio::task::JoinError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Catalog error: {0}")]
    CatalogError(#[from] catalog::Error),
    #[error("Schema index error: {0}")]
    SchemaIndexErr(#[from] index::Error),
    #[error("catalog database error: {0}")]
    SQLiteErr(#[from] rusqlite::Error),
    #[error("failed to parse JSON: {0}")]
    JSONErr(#[from] serde_json::Error),
    #[error("Failed to 'npm install' the catalog NodeJS pack")]
    NpmInstallFailed,
    #[error("HTTP error: {0}")]
    HyperError(#[from] hyper::Error),
    #[error("async task failure: {0}")]
    JoinErr(#[from] JoinError),
    #[error("HTTP error (warp): {0}")]
    WarpError(#[from] warp::Error),
    #[error("HTTP error: {0}")]
    HttpError(#[from] http::Error),
    #[error("Unknown source collection: {0}")]
    UnknownSourceCollection(String),
    #[error("Invocation returned non-OK status {status}: {body}")]
    RemoteHTTPError {
        status: http::StatusCode,
        body: String,
    },
    #[error("invalid 'application/json-seq' encoding")]
    InvalidJsonSeq,
    #[error("source document validation failed")]
    SourceValidationFailed,
    #[error("derived document validation failed")]
    DerivedValidationFailed,
    #[error("missing success trailer")]
    NoSuccessTrailerRenameMe,
    #[error("channel send error: {0}")]
    ChannelSendErr(#[from] futures::channel::mpsc::SendError),
}
