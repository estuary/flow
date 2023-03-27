#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Request {
    #[prost(message, optional, tag = "1")]
    pub spec: ::core::option::Option<request::Spec>,
    #[prost(message, optional, tag = "2")]
    pub discover: ::core::option::Option<request::Discover>,
    #[prost(message, optional, tag = "3")]
    pub validate: ::core::option::Option<request::Validate>,
    #[prost(message, optional, tag = "4")]
    pub apply: ::core::option::Option<request::Apply>,
    #[prost(message, optional, tag = "5")]
    pub open: ::core::option::Option<request::Open>,
    #[prost(message, optional, tag = "6")]
    pub acknowledge: ::core::option::Option<request::Acknowledge>,
    /// Reserved for internal use.
    #[prost(message, optional, tag = "100")]
    pub internal: ::core::option::Option<::pbjson_types::Any>,
}
/// Nested message and enum types in `Request`.
pub mod request {
    /// Spec requests the specification definition of this connector.
    /// Notably this includes its configuration JSON schemas.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Spec {
        /// Connector type addressed by this request.
        #[prost(
            enumeration = "super::super::flow::capture_spec::ConnectorType",
            tag = "1"
        )]
        pub connector_type: i32,
        /// Connector configuration, as an encoded JSON object.
        /// This may be a partial specification (for example, a Docker image),
        /// providing only enough information to fetch the remainder of the
        /// specification schema.
        #[prost(string, tag = "2")]
        pub config_json: ::prost::alloc::string::String,
    }
    /// Discover returns the set of resources available from this connector.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Discover {
        /// Connector type addressed by this request.
        #[prost(
            enumeration = "super::super::flow::capture_spec::ConnectorType",
            tag = "1"
        )]
        pub connector_type: i32,
        /// Connector configuration, as an encoded JSON object.
        #[prost(string, tag = "2")]
        pub config_json: ::prost::alloc::string::String,
    }
    /// Validate a capture configuration and proposed bindings.
    /// Validate is run out-of-band with ongoing capture invocations.
    /// It's purpose is to confirm that the proposed configuration
    /// is likely to succeed if applied and run, or to report any
    /// potential issues for the user to address.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Validate {
        /// Name of the capture being validated.
        #[prost(string, tag = "1")]
        pub name: ::prost::alloc::string::String,
        /// Connector type addressed by this request.
        #[prost(
            enumeration = "super::super::flow::capture_spec::ConnectorType",
            tag = "2"
        )]
        pub connector_type: i32,
        /// Connector configuration, as an encoded JSON object.
        #[prost(string, tag = "3")]
        pub config_json: ::prost::alloc::string::String,
        #[prost(message, repeated, tag = "4")]
        pub bindings: ::prost::alloc::vec::Vec<validate::Binding>,
        /// Network ports of this proposed capture.
        #[prost(message, repeated, tag = "5")]
        pub network_ports: ::prost::alloc::vec::Vec<super::super::flow::NetworkPort>,
    }
    /// Nested message and enum types in `Validate`.
    pub mod validate {
        /// Bindings of endpoint resources and collections to which they would be
        /// captured. Bindings are ordered and unique on the bound collection name.
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Binding {
            /// JSON-encoded object which specifies the endpoint resource to be captured.
            #[prost(string, tag = "1")]
            pub resource_config_json: ::prost::alloc::string::String,
            /// Collection to be captured.
            #[prost(message, optional, tag = "2")]
            pub collection: ::core::option::Option<
                super::super::super::flow::CollectionSpec,
            >,
        }
    }
    /// Apply a capture configuration and bindings to its endpoint.
    /// Apply is run out-of-band with ongoing connector invocations,
    /// and may be run many times for a single capture name,
    /// where each invocation has varying bindings, or even no bindings.
    /// The connector performs any required setup or cleanup.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Apply {
        /// Capture to be applied.
        #[prost(message, optional, tag = "1")]
        pub capture: ::core::option::Option<super::super::flow::CaptureSpec>,
        /// Version of the CaptureSpec being applied.
        #[prost(string, tag = "2")]
        pub version: ::prost::alloc::string::String,
        /// Is this Apply a dry-run? If so, no action is undertaken and Apply will
        /// report only what would have happened.
        #[prost(bool, tag = "3")]
        pub dry_run: bool,
    }
    /// Open a capture for reading documents from the endpoint.
    /// Unless the connector requests explicit acknowledgements,
    /// Open is the last message which will be sent to the connector.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Open {
        /// CaptureSpec to be pulled.
        #[prost(message, optional, tag = "1")]
        pub capture: ::core::option::Option<super::super::flow::CaptureSpec>,
        /// Version of the opened CaptureSpec.
        /// The connector may want to require that this match the version last
        /// provided to a successful Apply RPC. It's possible that it won't,
        /// due to expected propagation races in Flow's distributed runtime.
        #[prost(string, tag = "2")]
        pub version: ::prost::alloc::string::String,
        /// Range of documents to be processed by this invocation.
        #[prost(message, optional, tag = "3")]
        pub range: ::core::option::Option<super::super::flow::RangeSpec>,
        /// Last-persisted connector checkpoint state from a previous invocation.
        #[prost(string, tag = "4")]
        pub state_json: ::prost::alloc::string::String,
    }
    /// Tell the connector that some number of its preceding Checkpoints have
    /// committed to the Flow recovery log.
    ///
    /// Acknowledge is sent only if the connector set
    /// Response.Opened.explicit_acknowledgements.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Acknowledge {
        /// Number of preceeding Response.Checkpoint messages which have
        /// committed and are being acknowledged. Always one or more.
        #[prost(uint32, tag = "1")]
        pub checkpoints: u32,
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Response {
    #[prost(message, optional, tag = "1")]
    pub spec: ::core::option::Option<response::Spec>,
    #[prost(message, optional, tag = "2")]
    pub discovered: ::core::option::Option<response::Discovered>,
    #[prost(message, optional, tag = "3")]
    pub validated: ::core::option::Option<response::Validated>,
    #[prost(message, optional, tag = "4")]
    pub applied: ::core::option::Option<response::Applied>,
    #[prost(message, optional, tag = "5")]
    pub opened: ::core::option::Option<response::Opened>,
    #[prost(message, optional, tag = "6")]
    pub captured: ::core::option::Option<response::Captured>,
    #[prost(message, optional, tag = "7")]
    pub checkpoint: ::core::option::Option<response::Checkpoint>,
    /// Reserved for internal use.
    #[prost(message, optional, tag = "100")]
    pub internal: ::core::option::Option<::pbjson_types::Any>,
}
/// Nested message and enum types in `Response`.
pub mod response {
    /// Spec responds to Request.Spec.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Spec {
        /// Protocol version must be 3032023.
        #[prost(uint32, tag = "1")]
        pub protocol: u32,
        /// JSON schema of the connector's configuration.
        #[prost(string, tag = "2")]
        pub config_schema_json: ::prost::alloc::string::String,
        /// JSON schema of the connecor's resource configuration.
        #[prost(string, tag = "3")]
        pub resource_config_schema_json: ::prost::alloc::string::String,
        /// URL for connector's documention.
        #[prost(string, tag = "4")]
        pub documentation_url: ::prost::alloc::string::String,
        /// Optional OAuth2 configuration.
        #[prost(message, optional, tag = "5")]
        pub oauth2: ::core::option::Option<super::super::flow::OAuth2>,
    }
    /// Discovered responds to Request.Discover.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Discovered {
        #[prost(message, repeated, tag = "1")]
        pub bindings: ::prost::alloc::vec::Vec<discovered::Binding>,
    }
    /// Nested message and enum types in `Discovered`.
    pub mod discovered {
        /// Potential bindings which the capture could provide.
        /// Bindings may be returned in any order.
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Binding {
            /// The recommended name for this discovered binding,
            /// which is appended to a catalog prefix of the proposed capture
            /// to form the name of its recommended collection.
            #[prost(string, tag = "1")]
            pub recommended_name: ::prost::alloc::string::String,
            /// JSON-encoded object which specifies the captured resource configuration.
            #[prost(string, tag = "2")]
            pub resource_config_json: ::prost::alloc::string::String,
            /// JSON schema of documents produced by this binding.
            #[prost(string, tag = "3")]
            pub document_schema_json: ::prost::alloc::string::String,
            /// Composite key of documents (if known), as JSON-Pointers.
            #[prost(string, repeated, tag = "4")]
            pub key: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        }
    }
    /// Validated responds to Request.Validate.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Validated {
        #[prost(message, repeated, tag = "1")]
        pub bindings: ::prost::alloc::vec::Vec<validated::Binding>,
    }
    /// Nested message and enum types in `Validated`.
    pub mod validated {
        /// Validation responses for each binding of the request, and matching the
        /// request ordering. Each Binding must have a unique resource_path.
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Binding {
            /// Components of the resource path which fully qualify the resource
            /// identified by this binding.
            /// - For an RDBMS, this might be []{dbname, schema, table}.
            /// - For Kafka, this might be []{topic}.
            /// - For Redis, this might be []{key_prefix}.
            #[prost(string, repeated, tag = "1")]
            pub resource_path: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        }
    }
    /// Applied responds to Request.Apply.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Applied {
        /// Human-readable description of the action that the Driver took (or, if
        /// dry_run, would have taken). If empty, this Apply is to be considered a
        /// "no-op".
        #[prost(string, tag = "1")]
        pub action_description: ::prost::alloc::string::String,
    }
    /// Opened responds to Request.Open.
    /// After Opened, the connector beings sending Captured and Checkpoint.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Opened {
        /// If true then the runtime will send one Request.Acknowledge
        /// for each Response.Checkpoint sent by the connector,
        /// upon that Checkpoint having fully committed.
        #[prost(bool, tag = "1")]
        pub explicit_acknowledgements: bool,
    }
    /// Document captured by this connector invocation.
    /// Emitted documents are pending, and are not committed to their bound collection
    /// until a following Checkpoint is emitted.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Captured {
        /// Index of the Open binding for which this document is captured.
        #[prost(uint32, tag = "1")]
        pub binding: u32,
        /// Published JSON document.
        #[prost(string, tag = "2")]
        pub doc_json: ::prost::alloc::string::String,
    }
    /// Checkpoint all preceding documents of this invocation since the last checkpoint.
    /// The Flow runtime may begin to commit documents in a transaction.
    /// Note that the runtime may include more than one checkpoint in a single transaction.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Checkpoint {
        #[prost(message, optional, tag = "1")]
        pub state: ::core::option::Option<super::super::flow::ConnectorState>,
    }
}
