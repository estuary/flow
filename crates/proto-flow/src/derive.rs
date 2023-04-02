#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Request {
    #[prost(message, optional, tag = "1")]
    pub spec: ::core::option::Option<request::Spec>,
    #[prost(message, optional, tag = "2")]
    pub validate: ::core::option::Option<request::Validate>,
    #[prost(message, optional, tag = "3")]
    pub open: ::core::option::Option<request::Open>,
    #[prost(message, optional, tag = "4")]
    pub read: ::core::option::Option<request::Read>,
    #[prost(message, optional, tag = "5")]
    pub flush: ::core::option::Option<request::Flush>,
    #[prost(message, optional, tag = "6")]
    pub start_commit: ::core::option::Option<request::StartCommit>,
    #[prost(message, optional, tag = "7")]
    pub reset: ::core::option::Option<request::Reset>,
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
            enumeration = "super::super::flow::collection_spec::derivation::ConnectorType",
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
    /// Validate a derivation configuration and proposed transforms.
    /// Validate is run out-of-band with ongoing connector invocations.
    /// It's purpose is to confirm that the proposed configuration
    /// is likely to succeed if applied and run, or to report any
    /// potential issues for the user to address.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Validate {
        /// Connector type addressed by this request.
        #[prost(
            enumeration = "super::super::flow::collection_spec::derivation::ConnectorType",
            tag = "1"
        )]
        pub connector_type: i32,
        /// Connector configuration, as an encoded JSON object.
        #[prost(string, tag = "2")]
        pub config_json: ::prost::alloc::string::String,
        /// Collection to be derived.
        #[prost(message, optional, tag = "3")]
        pub collection: ::core::option::Option<super::super::flow::CollectionSpec>,
        #[prost(message, repeated, tag = "4")]
        pub transforms: ::prost::alloc::vec::Vec<validate::Transform>,
        /// JSON types of shuffle key components extracted by the transforms of this derivation.
        #[prost(
            enumeration = "super::super::flow::collection_spec::derivation::ShuffleType",
            repeated,
            tag = "5"
        )]
        pub shuffle_key_types: ::prost::alloc::vec::Vec<i32>,
        /// URL which roots the current Flow project.
        ///
        /// Derivation connectors should use this URL to generate any project-level
        /// files which are returned with Response.Validated.generated_files.
        #[prost(string, tag = "6")]
        pub project_root: ::prost::alloc::string::String,
        /// Map of relative JSON pointers to the derivation specification,
        /// and the absolute URL from which the location's content was resolved.
        /// Connectors may use this for generating more helpful errors which are
        /// framed to the user's filesystem, rather than the filesystem within
        /// the connector.
        #[prost(btree_map = "string, string", tag = "7")]
        pub import_map: ::prost::alloc::collections::BTreeMap<
            ::prost::alloc::string::String,
            ::prost::alloc::string::String,
        >,
        /// Network ports of this proposed derivation.
        #[prost(message, repeated, tag = "8")]
        pub network_ports: ::prost::alloc::vec::Vec<super::super::flow::NetworkPort>,
    }
    /// Nested message and enum types in `Validate`.
    pub mod validate {
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Transform {
            /// Stable name of this transform.
            #[prost(string, tag = "1")]
            pub name: ::prost::alloc::string::String,
            /// Sourced collection of this transform.
            #[prost(message, optional, tag = "2")]
            pub collection: ::core::option::Option<
                super::super::super::flow::CollectionSpec,
            >,
            /// JSON-encoded object which specifies the shuffle lambda configuration.
            /// If this transform has no shuffle lambda, this is empty.
            #[prost(string, tag = "3")]
            pub shuffle_lambda_config_json: ::prost::alloc::string::String,
            /// JSON-encoded object which specifies the lambda configuration.
            #[prost(string, tag = "4")]
            pub lambda_config_json: ::prost::alloc::string::String,
        }
    }
    /// Open a derivation stream.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Open {
        /// Collection to be derived.
        #[prost(message, optional, tag = "1")]
        pub collection: ::core::option::Option<super::super::flow::CollectionSpec>,
        /// Version of the opened MaterializationSpec.
        /// The driver may want to require that this match the version last
        /// provided to a successful Apply RPC. It's possible that it won't,
        /// due to expected propagation races in Flow's distributed runtime.
        #[prost(string, tag = "2")]
        pub version: ::prost::alloc::string::String,
        /// Range of documents to be processed by this invocation.
        #[prost(message, optional, tag = "3")]
        pub range: ::core::option::Option<super::super::flow::RangeSpec>,
        /// Last-persisted connector checkpoint state from a previous invocation.
        #[prost(string, tag = "5")]
        pub state_json: ::prost::alloc::string::String,
    }
    /// Read a document for one of the Opened transforms.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Read {
        /// Index of the Open transform for which this document was read.
        #[prost(uint32, tag = "1")]
        pub transform: u32,
        /// Deconstructed document UUID.
        #[prost(message, optional, tag = "2")]
        pub uuid: ::core::option::Option<super::super::flow::UuidParts>,
        #[prost(message, optional, tag = "3")]
        pub shuffle: ::core::option::Option<read::Shuffle>,
        /// JSON document which was read.
        #[prost(string, tag = "4")]
        pub doc_json: ::prost::alloc::string::String,
    }
    /// Nested message and enum types in `Read`.
    pub mod read {
        /// Shuffle under which this document was mapped.
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Shuffle {
            /// Shuffle key, as an array of key components.
            /// Ordering matches `keys` of the materialization's field selection.
            #[prost(string, tag = "1")]
            pub key_json: ::prost::alloc::string::String,
            /// Packed tuple of the document's shuffled key.
            #[prost(bytes = "vec", tag = "2")]
            pub packed: ::prost::alloc::vec::Vec<u8>,
            /// Stable hash of this document's shuffle key, which falls within
            /// `key_begin` and `key_end` of the Request.Open.
            #[prost(uint32, tag = "3")]
            pub hash: u32,
        }
    }
    /// Flush tells the connector it should immediately complete any deferred
    /// work and respond with Published documents for all previously Read
    /// documents, and then respond with Flushed.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Flush {}
    /// StartCommit indicates that the Flow runtime is beginning to commit.
    /// The checkpoint is purely advisory and the connector is not required to touch it.
    /// The connector responds with StartedCommit.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StartCommit {
        /// Flow runtime checkpoint associated with this transaction.
        #[prost(message, optional, tag = "1")]
        pub runtime_checkpoint: ::core::option::Option<
            ::proto_gazette::consumer::Checkpoint,
        >,
    }
    /// Reset any internal state, as if the derivation were just initialized.
    /// This is used only when running Flow tests, and clears the effects of
    /// one test before running the next.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Reset {}
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Response {
    #[prost(message, optional, tag = "1")]
    pub spec: ::core::option::Option<response::Spec>,
    #[prost(message, optional, tag = "2")]
    pub validated: ::core::option::Option<response::Validated>,
    #[prost(message, optional, tag = "3")]
    pub opened: ::core::option::Option<response::Opened>,
    #[prost(message, optional, tag = "4")]
    pub published: ::core::option::Option<response::Published>,
    #[prost(message, optional, tag = "5")]
    pub flushed: ::core::option::Option<response::Flushed>,
    #[prost(message, optional, tag = "6")]
    pub started_commit: ::core::option::Option<response::StartedCommit>,
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
        /// JSON schema of the connecor's lambda configuration.
        #[prost(string, tag = "3")]
        pub lambda_config_schema_json: ::prost::alloc::string::String,
        /// URL for connector's documention.
        #[prost(string, tag = "4")]
        pub documentation_url: ::prost::alloc::string::String,
    }
    /// Validated responds to Request.Validate.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Validated {
        #[prost(message, repeated, tag = "1")]
        pub transforms: ::prost::alloc::vec::Vec<validated::Transform>,
        /// Generated files returned by the connector.
        /// Keys are absolute URLs of the generated resource, and values are its
        /// generated file content.
        ///
        /// This can include project-level files, which should be underneath
        /// Request.Validate.project_root.
        ///
        /// When run in certain contexts within a user's local development environment,
        /// reads of Flow specifications use a relaxed handling for missing configuration
        /// files: rather than failing due to a missing file, the config file is instead
        /// resolved into an absolute URL of where the file is *expected* to live.
        /// The connector should handle these missing configs by generating and returning
        /// stub implementations of these files at those URLs.
        #[prost(btree_map = "string, string", tag = "2")]
        pub generated_files: ::prost::alloc::collections::BTreeMap<
            ::prost::alloc::string::String,
            ::prost::alloc::string::String,
        >,
    }
    /// Nested message and enum types in `Validated`.
    pub mod validated {
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Transform {
            /// Is this transform known to be read-only?
            #[prost(bool, tag = "1")]
            pub read_only: bool,
        }
    }
    /// Opened responds to Request.Open.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Opened {}
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Published {
        /// Published JSON document.
        #[prost(string, tag = "1")]
        pub doc_json: ::prost::alloc::string::String,
    }
    /// Flushed responds to Request.Flush, and indicates that all documents
    /// have been published.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Flushed {}
    /// StartedCommit responds to a Request.StartCommit, and includes an optional
    /// connector state update.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StartedCommit {
        #[prost(message, optional, tag = "1")]
        pub state: ::core::option::Option<super::super::flow::ConnectorState>,
    }
}
