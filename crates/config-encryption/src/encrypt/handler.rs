use crate::{encrypt, SopsArgs};
use axum::{
    self,
    extract::multipart,
    http::{self, header},
    response::{IntoResponse, IntoResponseParts, Response, ResponseParts},
    Extension, Json,
};
use serde::Serialize;
use serde_json::Value;
use std::fmt::{self, Debug, Display};
use std::sync::Arc;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Format {
    Json,
    Yaml,
}

impl Format {
    /// Return a Format corresponding to the given mime type from an Accept header. Returns Json if
    /// there's a *, and None if the mime doesn't seem compatible with either json or yaml.
    pub fn for_mime(m: &mime::Mime) -> Option<Format> {
        match (m.type_().as_str(), m.subtype().as_str()) {
            ("*", "*") => Some(Format::Json),
            ("application", "*") => Some(Format::Json),
            ("application", "json") => Some(Format::Json),
            // application/yaml isn't technically a standard, but it seems to be the most
            // reasonable thing we could use.
            ("application", "yaml") => Some(Format::Yaml),
            _ => None,
        }
    }

    pub fn sops_type(&self) -> &'static str {
        match self {
            Format::Json => "json",
            Format::Yaml => "yaml",
        }
    }

    pub fn content_type(&self) -> axum::headers::HeaderValue {
        let val = match self {
            Format::Json => "application/json",
            Format::Yaml => "application/yaml",
        };
        axum::headers::HeaderValue::from_static(val)
    }
}

pub fn router(sops_args: SopsArgs) -> axum::Router {
    axum::Router::new()
        .route(
            "/v1/encrypt-config",
            axum::routing::post(handle_encrypt_req),
        )
        .layer(Extension(Arc::new(sops_args)))
}

/// Type of a successful encrypt-config response
pub struct EncryptedConfig {
    pub document: Vec<u8>,
    pub format: Format,
}

impl IntoResponse for EncryptedConfig {
    fn into_response(self) -> Response {
        let EncryptedConfig { document, format } = self;
        (format, document).into_response()
    }
}

impl IntoResponseParts for Format {
    type Error = core::convert::Infallible;

    fn into_response_parts(self, mut res: ResponseParts) -> Result<ResponseParts, Self::Error> {
        res.headers_mut()
            .insert(header::CONTENT_TYPE, self.content_type());
        Ok(res)
    }
}

pub async fn handle_encrypt_req(
    accept: Option<axum::TypedHeader<Accept>>,
    req: EncryptReq,
    Extension(sops_args): Extension<Arc<SopsArgs>>,
) -> Result<EncryptedConfig, Error> {
    let validated = encrypt::validate(req)?;
    let prepared = encrypt::add_encrypted_suffixes(validated, &sops_args.encrypted_suffix)?;
    let output_format = accept.map(|header| header.0 .0).unwrap_or(Format::Json);
    let result = encrypt::encrypt(prepared, sops_args, output_format).await;
    if let Err(err) = result.as_ref() {
        tracing::error!(error = ?err, "request failed");
    }
    result
}

/// Error that describes a failed request. This could be due to a bad request, io error, or
/// anything else. This will be serialized as JSON and returned in the response body if anything
/// goes wrong.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    BadHeader(#[from] axum::extract::rejection::TypedHeaderRejection),

    #[error("invalid multipart request: {0}")]
    BadMultipartReq(#[from] multipart::MultipartRejection),
    // different from MultipartRejection because this catches errors reading the body
    #[error("multipart error: {0}")]
    MultipartError(#[from] multipart::MultipartError),
    // Used only for multipart requests. Json requests will use axum's JsonRejection
    #[error("failed to deserialize '{0}': {1}")]
    YamlError(&'static str, RedactDebug<serde_yaml::Error>),
    #[error("unexpected multipart field with name: '{0:?}'")]
    UnexpectedMultipartField(Option<String>),
    #[error("duplicate request part: '{0}'")]
    DuplicateMultipartField(&'static str),

    #[error("missing request part(s): {0:?}")]
    MissingFields(&'static [&'static str]),
    #[error("failed to build json schema: {0}")]
    SchemaBuild(#[from] ::json::schema::BuildError),
    #[error("failed to index json schema: {0}")]
    SchemaIndex(#[from] ::json::schema::index::Error),
    #[error("config failed schema validation")]
    FailedValidation(RedactDebug<::doc::FailedValidation>),
    #[error("the location '{0}' is cannot be encrypted because {1}")]
    InvalidSecretLocation(String, &'static str),
    #[error("missing Content-Type header")]
    MissingContentType,
    #[error("invalid Content-Type header")]
    InvalidContentType,
    #[error(transparent)]
    InvalidBody(#[from] axum::extract::rejection::JsonRejection),

    // Keep the wrapped errors for internal errors out of the Display message to avoid leaking details about the server.
    #[error("internal server error")]
    IoError(#[from] std::io::Error),
    #[error("internal server error")]
    Serialization(#[from] serde_json::Error),
    #[error("internal server error")]
    SopsFailed(std::process::ExitStatus, String),
    #[error("internal server error")]
    JoinError(#[from] tokio::task::JoinError),
}

impl Serialize for Error {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        let id = self.id();
        let mut ser = serializer.serialize_map(None)?;

        ser.serialize_entry("error", id)?;
        let desc = self.to_string();
        ser.serialize_entry("description", &desc)?;

        // If this is a validation error, then return the validation output as JSON to allow
        // clients to see exactly what went wrong. Ideally we'd be able to use this to highlight
        // the specific locations that failed validation.
        if let Error::FailedValidation(val_err) = self {
            ser.serialize_entry("details", &val_err.0)?;
        }
        ser.end()
    }
}

impl Error {
    pub fn status(&self) -> http::StatusCode {
        match self {
            Error::IoError(_) => http::StatusCode::INTERNAL_SERVER_ERROR,
            Error::Serialization(_) => http::StatusCode::INTERNAL_SERVER_ERROR,
            Error::SopsFailed(_, _) => http::StatusCode::INTERNAL_SERVER_ERROR,
            Error::JoinError(_) => http::StatusCode::INTERNAL_SERVER_ERROR,

            _ => http::StatusCode::BAD_REQUEST,
        }
    }

    pub fn id(&self) -> &'static str {
        match self {
            Error::BadHeader(_) => "BadHeader",
            Error::BadMultipartReq(_) => "BadMultipartReq",
            Error::MultipartError(_) => "MultipartError",
            Error::YamlError(_, _) => "YamlError",
            Error::UnexpectedMultipartField(_) => "UnexpectedMultipartField",
            Error::DuplicateMultipartField(_) => "DuplicateMultipartField",
            Error::MissingFields(_) => "MissingFields",
            Error::SchemaBuild(_) => "SchemaBuild",
            Error::SchemaIndex(_) => "SchemaIndex",
            Error::FailedValidation(_) => "FailedValidation",
            Error::InvalidSecretLocation(_, _) => "InvalidSecretLocation",
            Error::MissingContentType => "MissingContentType",
            Error::InvalidContentType => "InvalidContentType",
            Error::InvalidBody(_) => "InvalidBody",
            Error::IoError(_) => "IoError",
            Error::Serialization(_) => "Serialization",
            Error::SopsFailed(_, _) => "SopsFailed",
            Error::JoinError(_) => "JoinError",
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        handle_error(self).into_response()
    }
}

fn handle_error(
    error: encrypt::handler::Error,
) -> (http::StatusCode, axum::Json<encrypt::handler::Error>) {
    // Allows metrics to track each type of error separately.
    let labels = [("error", error.id())];
    metrics::increment_counter!("encrypt_config_errors", &labels);

    let status = error.status();
    if status.is_server_error() {
        tracing::error!(?error, "internal server error");
    } else {
        tracing::debug!(?error, "bad request error");
    }

    (status, axum::Json(error))
}

/// Wraps a type in order to override its Debug impl with a "redacted" string, while allowing Display
/// to function as normal. This is a bit of extra caution to prevent validation errors from being
/// logged on the server side.
pub struct RedactDebug<T>(pub T);
impl<T> Debug for RedactDebug<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}
impl<T> Display for RedactDebug<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A request to encrypt a subset of values in `config`. The values to be encrypted are derived from
/// the provided `schema` using the `secret` annotation. Any field with `"secret": true` in the
/// schema will be encrypted by extending its property name with `--encrypted-suffix`.
/// The request can be provided in multiple ways:
/// - `Content-Type: application/json` where the JSON body includes both the schema and the config
///   as properties.
/// - `Content-Type: multipart/form-data` with fields named `schema` and `config`, which may each
///   have a content type of either application/json or application/yaml. This representation exists
///   because it might make things easier for the flowctl CLI.
#[derive(serde::Deserialize, serde::Serialize)]
pub struct EncryptReq {
    /// A JSON schema that is used both for validation of the config, and to identify the specific
    /// properties that should be encrypted. Any property that has a `"secret": true` annotation
    /// will be encrypted, and all others will be left as plain text. Encrypted properties will
    /// have a suffix added to its name (e.g. "api_key" -> "api_key_sops").
    pub schema: Value,
    /// The plain text configuration to encrypt. This must validate against the provided JSON
    /// schema. If provided in YAML format, then all comments will be stripped.
    pub config: Value,
}

#[async_trait::async_trait]
impl<B> axum::extract::FromRequest<B> for EncryptReq
where
    B: axum::body::HttpBody<Data = bytes::Bytes> + Default + Unpin + Send + Sync + 'static,
    B::Error: Into<axum::BoxError>,
{
    type Rejection = Error;

    async fn from_request(
        req: &mut axum::extract::RequestParts<B>,
    ) -> Result<Self, Self::Rejection> {
        let content_type = req
            .headers()
            .get("Content-Type")
            .ok_or(Error::MissingContentType)?
            .to_str()
            .map_err(|_| Error::InvalidContentType)?;
        let mime = content_type
            .parse::<mime::Mime>()
            .map_err(|_| Error::InvalidContentType)?;

        match (mime.type_().as_str(), mime.subtype().as_str()) {
            ("multipart", "form-data") => {
                let multipart = axum::extract::Multipart::from_request(req).await?;
                EncryptReq::from_multipart(multipart).await
            }
            ("application", "json") => {
                let Json(encrypt_req) = req.extract::<Json<EncryptReq>>().await?;
                Ok(encrypt_req)
            }
            (_, _) => Err(Error::InvalidContentType),
        }
    }
}

impl EncryptReq {
    async fn from_multipart(mut multipart: axum::extract::Multipart) -> Result<Self, Error> {
        const SCHEMA_FIELD: &str = "schema";
        const CONFIG_FIELD: &str = "config";

        let mut schema: Option<Value> = None;
        let mut config: Option<Value> = None;

        while let Some(field) = multipart.next_field().await? {
            match field.name() {
                Some(SCHEMA_FIELD) => parse_field(&mut schema, SCHEMA_FIELD, field).await?,
                Some(CONFIG_FIELD) => {
                    parse_field(&mut config, CONFIG_FIELD, field).await?;
                }
                _ => {
                    return Err(Error::UnexpectedMultipartField(
                        field.name().map(|n| n.to_owned()),
                    ));
                }
            }
        }

        match (schema, config) {
            (Some(s), Some(c)) => Ok(EncryptReq {
                schema: s,
                config: c,
            }),
            (Some(_), None) => Err(Error::MissingFields(&[CONFIG_FIELD])),
            (None, Some(_)) => Err(Error::MissingFields(&[SCHEMA_FIELD])),
            (None, None) => Err(Error::MissingFields(&[SCHEMA_FIELD, CONFIG_FIELD])),
        }
    }
}

async fn parse_field<T: serde::de::DeserializeOwned>(
    dest: &mut Option<T>,
    name: &'static str,
    field: multipart::Field<'_>,
) -> Result<(), Error> {
    let bytes = field.bytes().await?;
    let t = serde_yaml::from_slice(bytes.as_ref())
        .map_err(|e| Error::YamlError(name, RedactDebug(e)))?;
    if dest.is_some() {
        return Err(Error::DuplicateMultipartField(name));
    }
    *dest = Some(t);
    Ok(())
}

/// For some silly reason, axum doesn't provide any Accept header implementation.
/// This impl is specialized to interpret content types as a `Format`, but it could easily be made
/// generic if there's need to use it elsewhere.
pub struct Accept(pub Format);
impl axum::headers::Header for Accept {
    fn name() -> &'static axum::headers::HeaderName {
        &header::ACCEPT
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, axum::headers::Error>
    where
        Self: Sized,
        I: Iterator<Item = &'i http::HeaderValue>,
    {
        values
            // A single Accept header value may contain multipe mime types separated by commas,
            // so we divide it up here.
            .flat_map(|value| value.to_str().ok().unwrap_or("").split(','))
            .filter(|s| !s.trim().is_empty())
            // Try to convert each content-type to a tuple of Format and q value, skipping any that don't
            // parse correctly. If they all fail, then we'll return an error at the end.
            .filter_map(|ct| {
                ct.parse::<mime::Mime>().ok().and_then(|mime| {
                    let q = mime
                        .get_param("q")
                        .and_then(|n| n.as_str().parse::<f64>().ok())
                        .unwrap_or(0.0);
                    Format::for_mime(&mime).map(|format| (format, q))
                })
            })
            // Find the tuple with the highest q value. This is done as integers because max_by_key
            // requires it to be Ord, not just PartialOrd.
            .max_by_key(|elem| (elem.1 * 1000.0).round() as i64)
            .map(|elem| Accept(elem.0))
            // Error if we failed to find an acceptable content type.
            .ok_or_else(axum::headers::Error::invalid)
    }

    fn encode<E: Extend<http::HeaderValue>>(&self, _values: &mut E) {
        unimplemented!("accept header is never encoded");
    }
}
