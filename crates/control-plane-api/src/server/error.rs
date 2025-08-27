//! Defines the `ApiError` type that can be returned from an API handler, which
//! specifies an HTTP status code and wraps an `anyhow::Error`. It implements
//! `IntoResponse`, allowing handlers to return a `Result<Json<T>, ApiError>`.
//! `From` impls exist for `anyhow::Error`, `Rejection`, and `sqlx::Error` with
//! reasonable default status codes. The http status code can be customized
//! using `ApiErrorExt::with_status` if you need to return a specific response
//! status for a given error.
//!
//! These types are written with the aim of making them easy to use in the
//! server. It's unclear whether we need an error struct defined in the `models`
//! crate, but in this case it's probably easiest to just use separate structs
//! for the client and server.
use axum::http::StatusCode;
use schemars::JsonSchema;

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

/// An error response
#[derive(
    Debug, thiserror::Error, serde::Serialize, serde::Deserialize, JsonSchema, aide::OperationIo,
)]
#[aide(output)]
#[error("status: {status}, error: {error}")]
pub struct ApiError {
    /// The HTTP status code
    #[serde(with = "status_serde")]
    #[schemars(schema_with = "status_serde::schema")]
    pub status: axum::http::StatusCode,

    /// The error message
    #[serde(with = "error_serde")]
    #[schemars(schema_with = "error_serde::schema")]
    #[source]
    pub error: anyhow::Error,
}

mod status_serde {
    use serde::{
        de::{self, Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    pub fn schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        serde_json::from_value(serde_json::json!({
            "type": "integer",
            "minimum": 100,
            "maximum": 599,
        }))
        .unwrap()
    }
    pub fn serialize<S: Serializer>(
        status: &axum::http::StatusCode,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        status.as_u16().serialize(s)
    }
    pub fn deserialize<'a, D: Deserializer<'a>>(
        deserializer: D,
    ) -> Result<axum::http::StatusCode, D::Error> {
        let int_val = <u16 as Deserialize>::deserialize(deserializer)?;
        axum::http::StatusCode::from_u16(int_val).map_err(|e| de::Error::custom(e))
    }
}

mod error_serde {
    use serde::{
        de::{Deserialize, Deserializer},
        ser::Serializer,
    };

    pub fn schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        serde_json::from_value(serde_json::json!({
            "type": "string",
        }))
        .unwrap()
    }
    pub fn serialize<S: Serializer>(error: &anyhow::Error, s: S) -> Result<S::Ok, S::Error> {
        let err_str = format!("{error:#}"); // alternate renders nested causes
        s.serialize_str(&err_str)
    }
    pub fn deserialize<'a, D: Deserializer<'a>>(
        deserializer: D,
    ) -> Result<anyhow::Error, D::Error> {
        let str_val = <String as Deserialize>::deserialize(deserializer)?;
        Ok(anyhow::anyhow!(str_val))
    }
}

impl ApiError {
    pub fn unauthorized(prefix: &str) -> ApiError {
        ApiError::new(
            StatusCode::UNAUTHORIZED,
            anyhow::anyhow!("user is not authorized to {prefix}"),
        )
    }

    pub fn new(status: StatusCode, error: anyhow::Error) -> ApiError {
        ApiError { status, error }
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
