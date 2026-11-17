//! The connector protocols which `connector.Request` and `connector.Response`
//! carry.
//!
//! [`Protocol`] states each protocol's facts once: its message types, its
//! variant of their `kind`, and the shape of its Spec exchange. [`Capture`],
//! [`Derive`], and [`Materialize`] implement it.

use crate::{capture, derive, materialize};

/// One connector protocol carried by `connector.Connector`.
pub trait Protocol: Sized + Send + Sync + 'static {
    type Request: prost::Message
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + Default
        + Send
        + Sync
        + Unpin
        + 'static;

    type Response: prost::Message
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + Default
        + Send
        + Sync
        + Unpin
        + 'static;

    /// Names this protocol in errors.
    const NAME: &'static str;
    /// Task type which this protocol implies.
    const TASK_TYPE: crate::ops::TaskType;

    /// Wrap a request in this protocol's `connector.Request` kind.
    fn wrap_request(request: Self::Request) -> super::request::Kind;

    /// Unwrap a request, or None if `kind` is another protocol's.
    fn unwrap_request(kind: super::request::Kind) -> Option<Self::Request>;

    /// Wrap a response in this protocol's `connector.Response` kind.
    fn wrap_response(response: Self::Response) -> super::response::Kind;

    /// Spec request of `connector_type` and its endpoint configuration.
    fn spec_request(connector_type: i32, config_json: bytes::Bytes) -> Self::Request;

    /// Is `request` a Spec?
    fn is_spec(request: &Self::Request) -> bool;

    /// Unwrap the Spec of `response`, or return a response which isn't one.
    fn unwrap_spec(
        response: Self::Response,
    ) -> Result<super::response::started::Spec, Self::Response>;
}

macro_rules! protocol {
    ($name:ident, $module:ident, $task_type:ident) => {
        pub enum $name {}

        impl Protocol for $name {
            type Request = $module::Request;
            type Response = $module::Response;

            const NAME: &'static str = stringify!($name);
            const TASK_TYPE: crate::ops::TaskType = crate::ops::TaskType::$task_type;

            fn wrap_request(request: Self::Request) -> super::request::Kind {
                super::request::Kind::$name(request)
            }

            fn unwrap_request(kind: super::request::Kind) -> Option<Self::Request> {
                match kind {
                    super::request::Kind::$name(request) => Some(request),
                    _ => None,
                }
            }

            fn wrap_response(response: Self::Response) -> super::response::Kind {
                super::response::Kind::$name(response)
            }

            fn spec_request(connector_type: i32, config_json: bytes::Bytes) -> Self::Request {
                $module::Request {
                    kind: Some($module::request::Kind::Spec($module::request::Spec {
                        connector_type,
                        config_json,
                    })),
                    ..Default::default()
                }
            }

            fn is_spec(request: &Self::Request) -> bool {
                matches!(request.kind, Some($module::request::Kind::Spec(_)))
            }

            fn unwrap_spec(
                response: Self::Response,
            ) -> Result<super::response::started::Spec, Self::Response> {
                match response {
                    $module::Response {
                        kind: Some($module::response::Kind::Spec(spec)),
                        ..
                    } => Ok(super::response::started::Spec::$name(spec)),
                    response => Err(response),
                }
            }
        }
    };
}

protocol!(Capture, capture, Capture);
protocol!(Derive, derive, Derivation);
protocol!(Materialize, materialize, Materialization);

impl super::response::started::Spec {
    /// Task type which this Spec's protocol implies.
    pub fn task_type(&self) -> crate::ops::TaskType {
        match self {
            Self::Capture(_) => Capture::TASK_TYPE,
            Self::Derive(_) => Derive::TASK_TYPE,
            Self::Materialize(_) => Materialize::TASK_TYPE,
        }
    }

    /// Protocol version which the connector reports.
    pub fn protocol(&self) -> u32 {
        match self {
            Self::Capture(spec) => spec.protocol,
            Self::Derive(spec) => spec.protocol,
            Self::Materialize(spec) => spec.protocol,
        }
    }

    /// JSON schema of the connector's endpoint configuration.
    pub fn config_schema_json(&self) -> &bytes::Bytes {
        match self {
            Self::Capture(spec) => &spec.config_schema_json,
            Self::Derive(spec) => &spec.config_schema_json,
            Self::Materialize(spec) => &spec.config_schema_json,
        }
    }
}
