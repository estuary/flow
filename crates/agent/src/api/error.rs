use axum::http::StatusCode;
use schemars::JsonSchema;
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
#[derive(
    Debug, thiserror::Error, serde::Serialize, serde::Deserialize, JsonSchema, aide::OperationIo,
)]
#[aide(output)]
#[error("status: {status}, error: {error}")]
pub struct ApiError {
    #[serde(with = "status_serde")]
    #[schemars(schema_with = "status_serde::schema")]
    status: axum::http::StatusCode,

    #[serde(with = "error_serde")]
    #[schemars(schema_with = "error_serde::schema")]
    #[source]
    error: anyhow::Error,
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
        de::{self, Deserialize, Deserializer},
        ser::{Serialize, Serializer},
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
