use axum::http::StatusCode;
use serde::Serialize;

use super::Rejection;

pub trait ApiErrorExt {
    /// Sets the given http response status to use when responding with this error.
    fn with_status(self, status: axum::http::StatusCode) -> ApiError;
}

impl<E: Into<ApiError> + Sized> ApiErrorExt for E {
    fn with_status(self, status: axum::http::StatusCode) -> ApiError {
        let mut err: ApiError = self.into();
        err.status = status;
        err
    }
}

/// An error that can be returned from an API handler, which specifies an HTTP
/// status code and wraps an `anyhow::Error`. It implements `IntoResponse`,
/// allowing handlers to return a `Result<Json<T>, ApiError>`.
#[derive(Debug, thiserror::Error, serde::Serialize)]
#[error("status: {status}, error: {error}")]
pub struct ApiError {
    #[serde(serialize_with = "ser_status")]
    status: axum::http::StatusCode,
    #[serde(serialize_with = "ser_anyhow_error")]
    #[source]
    error: anyhow::Error,
}

fn ser_status<S: serde::ser::Serializer>(
    status: &axum::http::StatusCode,
    s: S,
) -> Result<S::Ok, S::Error> {
    status.as_u16().serialize(s)
}

fn ser_anyhow_error<S: serde::ser::Serializer>(e: &anyhow::Error, s: S) -> Result<S::Ok, S::Error> {
    let str_val = format!("{e:#}"); // alternate renders nested causes
    str_val.serialize(s)
}

impl ApiError {
    pub fn not_found(catalog_name: &str) -> ApiError {
        ApiError {
            status: StatusCode::NOT_FOUND,
            error: anyhow::anyhow!(
                "requested entity '{catalog_name}' does not exist or you are not authorized"
            ),
        }
    }

    fn status_for(err: &anyhow::Error) -> StatusCode {
        // Ensure that we set the proper status code if the anyhow error itself
        // wraps a Rejection. This might not be necessary since we generally
        // convert Rejections into ApiErrors directly, using `From` impl, which
        // always sets the proper status. But this check is cheap, and it
        // ensures that we'll set the proper status in case a `?` operator
        // somewhere converts the Rejection into an `anyhow::Error` before
        // converting that error into an `ApiError`.
        if let Some(_rejection) = err.downcast_ref::<Rejection>() {
            return StatusCode::BAD_REQUEST;
        }
        if let Some(api_error) = err.downcast_ref::<ApiError>() {
            return api_error.status;
        }
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(error: sqlx::Error) -> ApiError {
        tracing::error!(?error, "API responding with database error");
        ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error: anyhow::anyhow!("database error, please retry the request"),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        let status = Self::status_for(&error);
        ApiError { status, error }
    }
}

impl From<Rejection> for ApiError {
    fn from(value: Rejection) -> Self {
        ApiError {
            status: StatusCode::BAD_REQUEST,
            error: anyhow::Error::from(value).context("Input validation error"),
        }
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let status = self.status;
        (status, axum::Json(self)).into_response()
    }
}
