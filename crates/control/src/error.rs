use std::error::Error;
use std::string::FromUtf8Error;

use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use tracing::error;

// TODO: Can't use the Payload wrapper as is, as inside `into_response`, we
// don't have a type for `Data` in `Payload<Data>`. I'm sure there's a way to
// model this differently that would work, but this is expedient.
#[derive(Debug, Serialize)]
pub struct ErrorWrapper {
    error: ProblemDetails,
}

impl ErrorWrapper {
    pub fn new(error: ProblemDetails) -> Self {
        Self { error }
    }
}

// TODO: Remove/rework how error details are used/constructed after initial
// development phase. This is currently the full error message details. This
// level of detail is not appropriate for end users, but is probably helpful for
// developers in the short term.
#[derive(Debug, Serialize)]
pub struct ProblemDetails {
    title: String,
    detail: Option<String>,
}

/// Application errors that can be automatically turned into an appropriate HTTP
/// response.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("database error")]
    Sqlx(#[from] sqlx::Error),

    #[error("subprocess error")]
    Subprocess(#[from] SubprocessError),

    #[error("json serialization error")]
    Serde(#[from] serde_json::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            AppError::Sqlx(sqlx::Error::RowNotFound) => StatusCode::NOT_FOUND,
            AppError::Sqlx(sqlx::Error::Database(_e)) => StatusCode::BAD_REQUEST,
            AppError::Sqlx(_e) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Subprocess(_e) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Serde(_e) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Other(_e) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        if status.is_server_error() {
            error!(status = ?status, message = ?self, details = ?self.source());
        }

        let body = Json(ErrorWrapper::new(ProblemDetails {
            title: self.to_string(),
            detail: self.source().map(ToString::to_string),
        }));

        (status, body).into_response()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SubprocessError {
    #[error("subprocess failed with status {status}")]
    Failure {
        status: std::process::ExitStatus,
        stdout: String,
        stderr: String,
    },
    #[error("subprocess encountered io error")]
    IO(#[from] std::io::Error),
    #[error("subprocess output was not UTF8")]
    Utf8(#[from] FromUtf8Error),
}
