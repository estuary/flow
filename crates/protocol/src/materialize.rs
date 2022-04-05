/// Constraint constrains the use of a flow.Projection within a materialization.
#[derive(serde::Deserialize, serde::Serialize)] #[serde(deny_unknown_fields)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Constraint {
    #[prost(enumeration="constraint::Type", tag="2")]
    pub r#type: i32,
    /// Optional human readable reason for the given constraint.
    /// Implementations are strongly encouraged to supply a descriptive message.
    #[prost(string, tag="3")]
    pub reason: ::prost::alloc::string::String,
}
/// Nested message and enum types in `Constraint`.
pub mod constraint {
    /// Type encodes a constraint type for this flow.Projection.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Type {
        /// This specific projection must be present.
        FieldRequired = 0,
        /// At least one projection with this location pointer must be present.
        LocationRequired = 1,
        /// A projection with this location is recommended, and should be included by
        /// default.
        LocationRecommended = 2,
        /// This projection may be included, but should be omitted by default.
        FieldOptional = 3,
        /// This projection must not be present in the materialization.
        FieldForbidden = 4,
        /// This specific projection is required but is also unacceptable (e.x.,
        /// because it uses an incompatible type with a previous applied version).
        Unsatisfiable = 5,
    }
}
/// SpecRequest is the request type of the Spec RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SpecRequest {
    /// Endpoint type addressed by this request.
    #[prost(enumeration="super::flow::EndpointType", tag="1")]
    pub endpoint_type: i32,
    /// Driver specification, as an encoded JSON object.
    /// This may be a partial specification (for example, a Docker image),
    /// providing only enough information to fetch the remainder of the
    /// specification schema.
    #[prost(string, tag="2")]
    pub endpoint_spec_json: ::prost::alloc::string::String,
}
/// SpecResponse is the response type of the Spec RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SpecResponse {
    /// JSON schema of an endpoint specification.
    #[prost(string, tag="1")]
    pub endpoint_spec_schema_json: ::prost::alloc::string::String,
    /// JSON schema of a resource specification.
    #[prost(string, tag="2")]
    pub resource_spec_schema_json: ::prost::alloc::string::String,
    /// URL for connector's documention.
    #[prost(string, tag="3")]
    pub documentation_url: ::prost::alloc::string::String,
}
/// ValidateRequest is the request type of the Validate RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidateRequest {
    /// Name of the materialization being validated.
    #[prost(string, tag="1")]
    pub materialization: ::prost::alloc::string::String,
    /// Endpoint type addressed by this request.
    #[prost(enumeration="super::flow::EndpointType", tag="2")]
    pub endpoint_type: i32,
    /// Driver specification, as an encoded JSON object.
    #[prost(string, tag="3")]
    pub endpoint_spec_json: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="4")]
    pub bindings: ::prost::alloc::vec::Vec<validate_request::Binding>,
}
/// Nested message and enum types in `ValidateRequest`.
pub mod validate_request {
    /// Bindings of endpoint resources and collections from which they would be
    /// materialized. Bindings are ordered and unique on the bound collection name.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Binding {
        /// JSON-encoded object which specifies the endpoint resource to be
        /// materialized.
        #[prost(string, tag="1")]
        pub resource_spec_json: ::prost::alloc::string::String,
        /// Collection to be materialized.
        #[prost(message, optional, tag="2")]
        pub collection: ::core::option::Option<super::super::flow::CollectionSpec>,
        /// Projection configuration, keyed by the projection field name,
        /// with JSON-encoded and driver-defined configuration objects.
        #[prost(map="string, string", tag="3")]
        pub field_config_json: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    }
}
/// ValidateResponse is the response type of the Validate RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidateResponse {
    #[prost(message, repeated, tag="1")]
    pub bindings: ::prost::alloc::vec::Vec<validate_response::Binding>,
}
/// Nested message and enum types in `ValidateResponse`.
pub mod validate_response {
    /// Validation responses for each binding of the request, and matching the
    /// request ordering. Each Binding must have a unique resource_path.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Binding {
        /// Constraints over collection projections imposed by the Driver,
        /// keyed by the projection field name. Projections of the CollectionSpec
        /// which are missing from constraints are implicitly forbidden.
        #[prost(map="string, message", tag="1")]
        pub constraints: ::std::collections::HashMap<::prost::alloc::string::String, super::Constraint>,
        /// Components of the resource path which fully qualify the resource
        /// identified by this binding.
        /// - For an RDBMS, this might be []{dbname, schema, table}.
        /// - For Kafka, this might be []{topic}.
        /// - For Redis, this might be []{key_prefix}.
        #[prost(string, repeated, tag="2")]
        pub resource_path: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// Materialize combined delta updates of documents rather than full
        /// reductions.
        ///
        /// When set, the Flow runtime will not attempt to load documents via
        /// TransactionRequest.Load, and also disables re-use of cached documents
        /// stored in prior transactions. Each stored document is exclusively
        /// combined from updates processed by the runtime within the current
        /// transaction only.
        ///
        /// This is appropriate for drivers over streams, WebHooks, and append-only
        /// files.
        ///
        /// For example, given a collection which reduces a sum count for each key,
        /// its materialization will produce a stream of delta updates to the count,
        /// such that a reader of the stream will arrive at the correct total count.
        #[prost(bool, tag="3")]
        pub delta_updates: bool,
    }
}
/// ApplyRequest is the request type of the ApplyUpsert and ApplyDelete RPCs.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyRequest {
    /// Materialization to be applied.
    #[prost(message, optional, tag="1")]
    pub materialization: ::core::option::Option<super::flow::MaterializationSpec>,
    /// Version of the MaterializationSpec being applied.
    #[prost(string, tag="2")]
    pub version: ::prost::alloc::string::String,
    /// Is this Apply a dry-run? If so, no action is undertaken and Apply will
    /// report only what would have happened.
    #[prost(bool, tag="3")]
    pub dry_run: bool,
}
/// ApplyResponse is the response type of the ApplyUpsert and ApplyDelete RPCs.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyResponse {
    /// Human-readable description of the action that the Driver took (or, if
    /// dry_run, would have taken). If empty, this Apply is to be considered a
    /// "no-op".
    #[prost(string, tag="1")]
    pub action_description: ::prost::alloc::string::String,
}
/// TransactionRequest is the request type of a Transaction RPC.
/// It will have exactly one top-level field set, which represents its message
/// type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionRequest {
    #[prost(message, optional, tag="1")]
    pub open: ::core::option::Option<transaction_request::Open>,
    #[prost(message, optional, tag="2")]
    pub load: ::core::option::Option<transaction_request::Load>,
    #[prost(message, optional, tag="3")]
    pub prepare: ::core::option::Option<transaction_request::Prepare>,
    #[prost(message, optional, tag="4")]
    pub store: ::core::option::Option<transaction_request::Store>,
    #[prost(message, optional, tag="5")]
    pub commit: ::core::option::Option<transaction_request::Commit>,
    #[prost(message, optional, tag="6")]
    pub acknowledge: ::core::option::Option<transaction_request::Acknowledge>,
}
/// Nested message and enum types in `TransactionRequest`.
pub mod transaction_request {
    /// Open a transaction stream.
    ///
    /// If the Flow recovery log is authoritative:
    /// The driver is given its last committed driver checkpoint in this request.
    /// It MAY return a Flow checkpoint in its opened response -- perhaps an older
    /// Flow checkpoint which was previously embedded within its driver checkpoint.
    ///
    /// If the remote store is authoritative:
    /// The driver MUST fence off other streams of this materialization that
    /// overlap the provided [key_begin, key_end) range, such that those streams
    /// cannot issue further commits. The driver MUST return its stored checkpoint
    /// for this materialization and range [key_begin, key_end] in its Opened
    /// response.
    ///
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Open {
        /// Materialization to be transacted.
        #[prost(message, optional, tag="1")]
        pub materialization: ::core::option::Option<super::super::flow::MaterializationSpec>,
        /// Version of the opened MaterializationSpec.
        /// The driver may want to require that this match the version last
        /// provided to a successful Apply RPC. It's possible that it won't,
        /// due to expected propagation races in Flow's distributed runtime.
        #[prost(string, tag="2")]
        pub version: ::prost::alloc::string::String,
        /// [begin, end] inclusive range of keys processed by this transaction
        /// stream. Ranges are with respect to a 32-bit hash of a packed document
        /// key.
        #[prost(fixed32, tag="3")]
        pub key_begin: u32,
        #[prost(fixed32, tag="4")]
        pub key_end: u32,
        /// Last-persisted driver checkpoint committed in the Flow runtime recovery
        /// log. Or empty, if the driver has cleared or never set its checkpoint.
        #[prost(bytes="vec", tag="5")]
        pub driver_checkpoint_json: ::prost::alloc::vec::Vec<u8>,
    }
    /// Load one or more documents identified by key.
    /// Keys may included documents which have never before been stored,
    /// but a given key will be sent in a transaction Load just one time.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Load {
        /// The materialization binding for documents of this Load request.
        #[prost(uint32, tag="1")]
        pub binding: u32,
        /// Byte arena of the request.
        #[prost(bytes="vec", tag="2")]
        pub arena: ::prost::alloc::vec::Vec<u8>,
        /// Packed tuples of collection keys, enumerating the documents to load.
        #[prost(message, repeated, tag="3")]
        pub packed_keys: ::prost::alloc::vec::Vec<super::super::flow::Slice>,
    }
    /// Prepare to commit. No further Loads will be sent in this transaction.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Prepare {
        /// Flow checkpoint to commit with this transaction.
        #[prost(bytes="vec", tag="1")]
        pub flow_checkpoint: ::prost::alloc::vec::Vec<u8>,
    }
    /// Store documents of this transaction commit.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Store {
        /// The materialization binding for documents of this Store request.
        #[prost(uint32, tag="1")]
        pub binding: u32,
        /// Byte arena of the request.
        #[prost(bytes="vec", tag="2")]
        pub arena: ::prost::alloc::vec::Vec<u8>,
        /// Packed tuples holding keys of each document.
        #[prost(message, repeated, tag="3")]
        pub packed_keys: ::prost::alloc::vec::Vec<super::super::flow::Slice>,
        /// Packed tuples holding values for each document.
        #[prost(message, repeated, tag="4")]
        pub packed_values: ::prost::alloc::vec::Vec<super::super::flow::Slice>,
        /// JSON documents.
        #[prost(message, repeated, tag="5")]
        pub docs_json: ::prost::alloc::vec::Vec<super::super::flow::Slice>,
        /// Exists is true if this document as previously been loaded or stored.
        #[prost(bool, repeated, tag="6")]
        pub exists: ::prost::alloc::vec::Vec<bool>,
    }
    /// Mark the end of the Store phase, and if the remote store is authoritative,
    /// instruct it to commit its transaction.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Commit {
    }
    /// Notify the driver that the previous transaction has committed to the Flow
    /// runtime's recovery log.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Acknowledge {
    }
}
/// TransactionResponse is the response type of a Transaction RPC.
/// It will have exactly one top-level field set, which represents its message
/// type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionResponse {
    #[prost(message, optional, tag="1")]
    pub opened: ::core::option::Option<transaction_response::Opened>,
    #[prost(message, optional, tag="2")]
    pub loaded: ::core::option::Option<transaction_response::Loaded>,
    /// Prepared responds to a TransactionRequest.Prepare of the client.
    /// No further Loaded responses will be sent.
    #[prost(message, optional, tag="3")]
    pub prepared: ::core::option::Option<super::flow::DriverCheckpoint>,
    #[prost(message, optional, tag="4")]
    pub driver_committed: ::core::option::Option<transaction_response::DriverCommitted>,
    #[prost(message, optional, tag="5")]
    pub acknowledged: ::core::option::Option<transaction_response::Acknowledged>,
}
/// Nested message and enum types in `TransactionResponse`.
pub mod transaction_response {
    /// Opened responds to TransactionRequest.Open of the client.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Opened {
        /// Flow checkpoint to begin processing from.
        /// If empty, the most recent checkpoint of the Flow recovery log is used.
        ///
        /// Or, a driver may send the value []byte{0xf8, 0xff, 0xff, 0xff, 0xf, 0x1}
        /// to explicitly begin processing from a zero-valued checkpoint, effectively
        /// rebuilding the materialization from scratch. This sentinel is a trivial
        /// encoding of the max-value 2^29-1 protobuf tag with boolean true.
        #[prost(bytes="vec", tag="1")]
        pub flow_checkpoint: ::prost::alloc::vec::Vec<u8>,
    }
    /// Loaded responds to TransactionRequest.Loads of the client.
    /// It returns documents of requested keys which have previously been stored.
    /// Keys not found in the store MUST be omitted. Documents may be in any order,
    /// both within and across Loaded response messages, but a document of a given
    /// key MUST be sent at most one time in a Transaction.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Loaded {
        /// The materialization binding for documents of this Loaded response.
        #[prost(uint32, tag="1")]
        pub binding: u32,
        /// Byte arena of the request.
        #[prost(bytes="vec", tag="2")]
        pub arena: ::prost::alloc::vec::Vec<u8>,
        /// Loaded JSON documents.
        #[prost(message, repeated, tag="3")]
        pub docs_json: ::prost::alloc::vec::Vec<super::super::flow::Slice>,
    }
    /// Mark the end of the Store phase, indicating that all documents have been
    /// fully stored.
    ///
    /// If the remote store is authoritative, tell the Flow runtime that it has
    /// committed.
    ///
    /// If the recovery log is authoritative, DriverCommitted is sent but no actual
    /// transactional driver commit is performed.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DriverCommitted {
    }
    /// Notify the Flow runtime of receipt of it's confirmation that the
    /// Flow recovery log has committed.
    ///
    /// If the driver utilizes staged data which is idempotently applied,
    /// it must apply staged data of the commit at this time, and respond
    /// with Acknowledged only once that's completed.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Acknowledged {
    }
}
/// Extra messages used by connectors
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Extra {
}
/// Nested message and enum types in `Extra`.
pub mod extra {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ValidateExistingProjectionRequest {
        /// Existing Materialization Binding
        #[prost(message, optional, tag="1")]
        pub existing_binding: ::core::option::Option<super::super::flow::materialization_spec::Binding>,
        /// Proposed ValidateRequest Binding
        #[prost(message, optional, tag="2")]
        pub proposed_binding: ::core::option::Option<super::validate_request::Binding>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ValidateBindingAgainstConstraints {
        /// Materialization Binding
        #[prost(message, optional, tag="1")]
        pub binding: ::core::option::Option<super::super::flow::materialization_spec::Binding>,
        /// Constraints map
        #[prost(map="string, message", tag="2")]
        pub constraints: ::std::collections::HashMap<::prost::alloc::string::String, super::Constraint>,
    }
}
/// Generated client implementations.
#[cfg(feature = "materialize_client")]
pub mod driver_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Driver is the service implemented by a materialization connector.
    #[derive(Debug, Clone)]
    pub struct DriverClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl DriverClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> DriverClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Default + Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> DriverClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            DriverClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        /// Spec returns the specification definition of this driver.
        /// Notably this includes its endpoint and resource configuration JSON schema.
        pub async fn spec(
            &mut self,
            request: impl tonic::IntoRequest<super::SpecRequest>,
        ) -> Result<tonic::Response<super::SpecResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/materialize.Driver/Spec");
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Validate that store resources and proposed collection bindings are
        /// compatible, and return constraints over the projections of each binding.
        pub async fn validate(
            &mut self,
            request: impl tonic::IntoRequest<super::ValidateRequest>,
        ) -> Result<tonic::Response<super::ValidateResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/materialize.Driver/Validate",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// ApplyUpsert applies a new or updated materialization to the store.
        pub async fn apply_upsert(
            &mut self,
            request: impl tonic::IntoRequest<super::ApplyRequest>,
        ) -> Result<tonic::Response<super::ApplyResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/materialize.Driver/ApplyUpsert",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// ApplyDelete deletes an existing materialization from the store.
        pub async fn apply_delete(
            &mut self,
            request: impl tonic::IntoRequest<super::ApplyRequest>,
        ) -> Result<tonic::Response<super::ApplyResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/materialize.Driver/ApplyDelete",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Transactions is a very long lived RPC through which the Flow runtime and a
        /// Driver cooperatively execute an unbounded number of transactions.
        ///
        /// This RPC workflow maintains a materialized view of a Flow collection
        /// in an external system. It has distinct load, prepare, store, and commit
        /// phases. The Flow runtime and driver cooperatively maintain a fully-reduced
        /// view of each document by loading current states from the store, reducing in
        /// a number of updates, and then transactionally storing updated documents and
        /// checkpoints.
        ///
        /// Push-only Endpoints & Delta Updates
        /// ===================================
        ///
        /// Some systems, such as APIs, Webhooks, and Pub/Sub, are push-only in nature.
        /// Flow materializations can run in a "delta updates" mode, where the load
        /// phase is always skipped and Flow does not attempt to store fully-reduced
        /// documents. Instead, during the store phase, the runtime sends delta
        /// updates which reflect the combined roll-up of collection documents
        /// processed only within this transaction.
        ///
        /// To illustrate the meaning of a delta update, consider documents which are
        /// simple counters, having a collection schema that uses a `sum` reduction
        /// strategy.
        ///
        /// Without delta updates, Flow would reduce documents -1, 3, and 2 by `sum`
        /// to arrive at document 4, which is stored. The next transaction,
        /// document 4 is loaded and reduced with 6, -7, and -1 to arrive at a new
        /// stored document 2. This document, 2, represents the full reduction of the
        /// collection documents materialized thus far.
        ///
        /// Compare to delta updates mode: collection documents -1, 3, and 2 are
        /// combined to store a delta-update document of 4. The next transaction starts
        /// anew, and 6, -7, and -1 combine to arrive at a delta-update document of -2.
        /// These delta updates are a windowed combine over documents seen in the
        /// current transaction only, and unlike before are not a full reduction of the
        /// document. If delta updates were written to pub/sub, note that a subscriber
        /// could further reduce over each delta update to recover the fully reduced
        /// document of 2.
        ///
        /// Note that many use cases require only `lastWriteWins` reduction behavior,
        /// and for these use cases delta updates does the "right thing" by trivially
        /// re-writing each document with its most recent version. This matches the
        /// behavior of Kafka Connect, for example.
        ///
        /// On Transactionality
        /// ===================
        ///
        /// The beating heart of transactionality in materializations is this:
        /// there is a consumption checkpoint, and there is a state of the view.
        /// As the materialization progresses, both the checkpoint and the view state
        /// will change. Updates to the checkpoint and to the view state MUST always
        /// commit together, in the exact same transaction.
        ///
        /// Flow transaction tasks have a backing transactional recovery log,
        /// which is capable of durable commits that update both the checkpoint
        /// and also a (reasonably small) driver-defined state. More on driver
        /// states later.
        ///
        /// Many interesting systems are also fully transactional in nature.
        ///
        /// When implementing a matherialization driver, the first question an
        /// implementor must answer is: whose commit is authoritative?
        /// Flow's recovery log, or the materialized system ?
        /// This protocol supports either.
        ///
        /// Implementation Pattern: Remote Store is Authoritative
        /// =====================================================
        ///
        /// In this pattern, the remote store persists view states and the Flow
        /// consumption checkpoints which those views reflect (there are many such
        /// checkpoints: one per task split). The Flow recovery log is not used.
        ///
        /// Typically this workflow runs in the context of a synchronous BEGIN/COMMIT
        /// transaction, which updates table states and a Flow checkpoint together.
        /// The transaction need be scoped only to the store phase of this workflow,
        /// as the Flow runtime assumes only read-committed loads.
        ///
        /// Flow is a distributed system, and an important consideration is the effect
        /// of a "zombie" assignment of a materialization task, which can race a
        /// newly-promoted assignment of that same task.
        ///
        /// Fencing is a technique which uses the transactional capabilities of a store
        /// to "fence off" an older zombie assignment, such that it's prevented from
        /// committing further transactions. This avoids a failure mode where:
        ///  - New assignment N recovers a checkpoint at Ti.
        ///  - Zombie assignment Z commits another transaction at Ti+1.
        ///  - N beings processing from Ti, inadvertently duplicating the effects of
        ///  Ti+1.
        ///
        /// When authoritative, the remote store must implement fencing behavior.
        /// As a sketch, the store can maintain a nonce value alongside the checkpoint
        /// of each task split. The nonce is updated on each open of this RPC,
        /// and each commit transaction then verifies that the nonce has not been
        /// changed.
        ///
        /// In the future, if another RPC opens and updates the nonce, it fences off
        /// this instance of the task split and prevents it from committing further
        /// transactions.
        ///
        /// Implementation Pattern: Recovery Log with Non-Transactional Store
        /// =================================================================
        ///
        /// In this pattern, the recovery log persists the Flow checkpoint and handles
        /// fencing semantics. During the load and store phases, the driver
        /// directly manipulates a non-transactional store or API.
        ///
        /// Note that this pattern is at-least-once. A transaction may fail part-way
        /// through and be restarted, causing its effects to be partially or fully
        /// replayed.
        ///
        /// Care must be taken if the collection's schema has reduction annotations
        /// such as `sum`, as those reductions may be applied more than once due to
        /// a partially completed, but ultimately failed transaction.
        ///
        /// If the collection's schema is last-write-wins, this mode still provides
        /// effectively-once behavior. Collections which aren't last-write-wins
        /// can be turned into last-write-wins through the use of derivation
        /// registers.
        ///
        /// Implementation Pattern: Recovery Log with Idempotent Apply
        /// ==========================================================
        ///
        /// In this pattern the recovery log is authoritative, but the driver uses
        /// external stable storage to stage the effects of a transaction -- rather
        /// than directly applying them to the store -- such that those effects can be
        /// idempotently applied after the transaction commits.
        ///
        /// This allows stores which feature a weaker transactionality guarantee to
        /// still be used in an exactly-once way, so long as they support an idempotent
        /// apply operation.
        ///
        /// Driver checkpoints can facilitate this pattern. For example, a driver might
        /// generate a unique filename in S3 and reference it in its prepared
        /// checkpoint, which is committed to the recovery log. During the "store"
        /// phase, it writes to this S3 file. After the transaction commits, it tells
        /// the store of the new file to incorporate. The store must handle
        /// idempotency, by applying the effects of the unique file just once, even if
        /// told of the file multiple times.
        ///
        /// A related extension of this pattern is for the driver to embed a Flow
        /// checkpoint into its driver checkpoint. Doing so allows the driver to
        /// express an intention to restart from an older alternative checkpoint, as
        /// compared to the most recent committed checkpoint of the recovery log.
        ///
        /// As mentioned above, it's crucial that store states and checkpoints commit
        /// together. While seemingly bending that rule, this pattern is consistent
        /// with it because, on commit, the semantic contents of the store include BOTH
        /// its base state, as well as the staged idempotent update. The store just may
        /// not know it yet, but eventually it must because of the retried idempotent
        /// apply.
        ///
        /// Note the driver must therefore ensure that staged updates are fully applied
        /// before returning an "load" responses, in order to provide the correct
        /// read-committed semantics required by the Flow runtime.
        ///
        /// RPC Lifecycle
        /// =============
        ///
        /// The RPC follows the following lifecycle:
        ///
        /// :TransactionRequest.Open:
        ///    - The Flow runtime opens the stream.
        /// :TransactionResponse.Opened:
        ///    - If the remote store is authoritative, it must fence off other RPCs
        ///      of this task split from committing further transactions,
        ///      and it retrieves a Flow checkpoint which is returned to the runtime.
        ///
        /// TransactionRequest.Open and TransactionResponse.Opened are sent only
        /// once, at the commencement of the stream. Thereafter the protocol loops:
        ///
        /// Load phase
        /// ==========
        ///
        /// The Load phases is Load requests *intermixed* with one
        /// Acknowledge/Acknowledged message flow. The driver must accomodate an
        /// Acknowledge that occurs before, during, or after a sequence of Load
        /// requests. It's guaranteed to see exactly one Acknowledge request during
        /// this phase.
        ///
        /// :TransactionRequest.Acknowledge:
        ///    - The runtime tells the driver that a commit to the recovery log has
        ///      completed.
        ///    - The driver applies a staged update to the base store, where
        ///      applicable.
        ///    - Note Acknowledge is sent in the very first iteration for consistency.
        ///      Semantically, it's an acknowledgement of the recovered checkpoint.
        ///      If a previous invocation failed after recovery log commit but before
        ///      applying the staged change, this is an opportunity to ensure that
        ///      apply occurs.
        /// :TransactionResponse.Acknowledged:
        ///    - The driver responds to the runtime only after applying a staged
        ///      update, where applicable.
        ///    - If there is no staged update, the driver immediately responds on
        ///      seeing Acknowledge.
        ///
        /// :TransactionRequest.Load:
        ///    - The runtime sends zero or more Load messages.
        ///    - The driver may send any number of TransactionResponse.Loaded in
        ///      response.
        ///    - If the driver will apply a staged update, it must await Acknowledge
        ///      and have applied the update to the store *before* evaluating any
        ///      Loads, to ensure correct read-committed behavior.
        ///    - The driver may defer responding with some or all loads until the
        ///      prepare phase.
        /// :TransactionResponse.Loaded:
        ///    - The driver sends zero or more Loaded messages, once for each loaded
        ///      document.
        ///    - Document keys not found in the store are omitted and not sent as
        ///      Loaded.
        ///
        /// Prepare phase
        /// =============
        ///
        /// The prepare phase begins only after the prior transaction has both
        /// committed and also been acknowledged. It marks the bounds of the present
        /// transaction.
        ///
        /// Upon entering this phase, the driver must immediately evaluate any deferred
        /// Load requests and send remaining Loaded responses.
        ///
        /// :TransactionRequest.Prepare:
        ///    - The runtime sends a Prepare message with its Flow checkpoint.
        /// :TransactionResponse.Prepared:
        ///    - The driver sends Prepared after having flushed all Loaded responses.
        ///    - The driver may include a driver checkpoint update which will be
        ///      committed to the recovery log with this transaction.
        ///
        /// Store phase
        /// ===========
        ///
        /// The store phase is when the runtime sends the driver materialized document
        /// updates, as well as an indication of whether the document is an insert,
        /// update, or delete (in other words, was it returned in a Loaded response?).
        ///
        /// :TransactionRequest.Store:
        ///    - The runtime sends zero or more Store messages.
        ///
        /// Commit phase
        /// ============
        ///
        /// The commit phase marks the end of the store phase, and tells the driver of
        /// the runtime's intent to commit to its recovery log. If the remote store is
        /// authoritative, the driver must commit its transaction at this time.
        ///
        /// :TransactionRequest.Commit:
        ///    - The runtime sends a Commit message, denoting its intention to commit.
        ///    - If the remote store is authoritative, the driver includes the Flow
        ///      checkpoint into its transaction and commits it along with view state
        ///      updates.
        ///    - Otherwise, the driver immediately responds with DriverCommitted.
        /// :TransactionResponse.DriverCommitted:
        ///    - The driver sends a DriverCommitted message.
        ///    - The runtime commits Flow and driver checkpoint to its recovery
        ///      log. The completion of this commit will be marked by an
        ///      Acknowledge during the next load phase.
        ///    - Runtime and driver begin a new, pipelined transaction by looping to
        ///      load while this transaction continues to commit.
        ///
        /// An error of any kind rolls back the transaction in progress and terminates
        /// the stream.
        pub async fn transactions(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::TransactionRequest,
            >,
        ) -> Result<
                tonic::Response<tonic::codec::Streaming<super::TransactionResponse>>,
                tonic::Status,
            > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/materialize.Driver/Transactions",
            );
            self.inner.streaming(request.into_streaming_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
#[cfg(feature = "materialize_server")]
pub mod driver_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with DriverServer.
    #[async_trait]
    pub trait Driver: Send + Sync + 'static {
        /// Spec returns the specification definition of this driver.
        /// Notably this includes its endpoint and resource configuration JSON schema.
        async fn spec(
            &self,
            request: tonic::Request<super::SpecRequest>,
        ) -> Result<tonic::Response<super::SpecResponse>, tonic::Status>;
        /// Validate that store resources and proposed collection bindings are
        /// compatible, and return constraints over the projections of each binding.
        async fn validate(
            &self,
            request: tonic::Request<super::ValidateRequest>,
        ) -> Result<tonic::Response<super::ValidateResponse>, tonic::Status>;
        /// ApplyUpsert applies a new or updated materialization to the store.
        async fn apply_upsert(
            &self,
            request: tonic::Request<super::ApplyRequest>,
        ) -> Result<tonic::Response<super::ApplyResponse>, tonic::Status>;
        /// ApplyDelete deletes an existing materialization from the store.
        async fn apply_delete(
            &self,
            request: tonic::Request<super::ApplyRequest>,
        ) -> Result<tonic::Response<super::ApplyResponse>, tonic::Status>;
        ///Server streaming response type for the Transactions method.
        type TransactionsStream: futures_core::Stream<
                Item = Result<super::TransactionResponse, tonic::Status>,
            >
            + Send
            + 'static;
        /// Transactions is a very long lived RPC through which the Flow runtime and a
        /// Driver cooperatively execute an unbounded number of transactions.
        ///
        /// This RPC workflow maintains a materialized view of a Flow collection
        /// in an external system. It has distinct load, prepare, store, and commit
        /// phases. The Flow runtime and driver cooperatively maintain a fully-reduced
        /// view of each document by loading current states from the store, reducing in
        /// a number of updates, and then transactionally storing updated documents and
        /// checkpoints.
        ///
        /// Push-only Endpoints & Delta Updates
        /// ===================================
        ///
        /// Some systems, such as APIs, Webhooks, and Pub/Sub, are push-only in nature.
        /// Flow materializations can run in a "delta updates" mode, where the load
        /// phase is always skipped and Flow does not attempt to store fully-reduced
        /// documents. Instead, during the store phase, the runtime sends delta
        /// updates which reflect the combined roll-up of collection documents
        /// processed only within this transaction.
        ///
        /// To illustrate the meaning of a delta update, consider documents which are
        /// simple counters, having a collection schema that uses a `sum` reduction
        /// strategy.
        ///
        /// Without delta updates, Flow would reduce documents -1, 3, and 2 by `sum`
        /// to arrive at document 4, which is stored. The next transaction,
        /// document 4 is loaded and reduced with 6, -7, and -1 to arrive at a new
        /// stored document 2. This document, 2, represents the full reduction of the
        /// collection documents materialized thus far.
        ///
        /// Compare to delta updates mode: collection documents -1, 3, and 2 are
        /// combined to store a delta-update document of 4. The next transaction starts
        /// anew, and 6, -7, and -1 combine to arrive at a delta-update document of -2.
        /// These delta updates are a windowed combine over documents seen in the
        /// current transaction only, and unlike before are not a full reduction of the
        /// document. If delta updates were written to pub/sub, note that a subscriber
        /// could further reduce over each delta update to recover the fully reduced
        /// document of 2.
        ///
        /// Note that many use cases require only `lastWriteWins` reduction behavior,
        /// and for these use cases delta updates does the "right thing" by trivially
        /// re-writing each document with its most recent version. This matches the
        /// behavior of Kafka Connect, for example.
        ///
        /// On Transactionality
        /// ===================
        ///
        /// The beating heart of transactionality in materializations is this:
        /// there is a consumption checkpoint, and there is a state of the view.
        /// As the materialization progresses, both the checkpoint and the view state
        /// will change. Updates to the checkpoint and to the view state MUST always
        /// commit together, in the exact same transaction.
        ///
        /// Flow transaction tasks have a backing transactional recovery log,
        /// which is capable of durable commits that update both the checkpoint
        /// and also a (reasonably small) driver-defined state. More on driver
        /// states later.
        ///
        /// Many interesting systems are also fully transactional in nature.
        ///
        /// When implementing a matherialization driver, the first question an
        /// implementor must answer is: whose commit is authoritative?
        /// Flow's recovery log, or the materialized system ?
        /// This protocol supports either.
        ///
        /// Implementation Pattern: Remote Store is Authoritative
        /// =====================================================
        ///
        /// In this pattern, the remote store persists view states and the Flow
        /// consumption checkpoints which those views reflect (there are many such
        /// checkpoints: one per task split). The Flow recovery log is not used.
        ///
        /// Typically this workflow runs in the context of a synchronous BEGIN/COMMIT
        /// transaction, which updates table states and a Flow checkpoint together.
        /// The transaction need be scoped only to the store phase of this workflow,
        /// as the Flow runtime assumes only read-committed loads.
        ///
        /// Flow is a distributed system, and an important consideration is the effect
        /// of a "zombie" assignment of a materialization task, which can race a
        /// newly-promoted assignment of that same task.
        ///
        /// Fencing is a technique which uses the transactional capabilities of a store
        /// to "fence off" an older zombie assignment, such that it's prevented from
        /// committing further transactions. This avoids a failure mode where:
        ///  - New assignment N recovers a checkpoint at Ti.
        ///  - Zombie assignment Z commits another transaction at Ti+1.
        ///  - N beings processing from Ti, inadvertently duplicating the effects of
        ///  Ti+1.
        ///
        /// When authoritative, the remote store must implement fencing behavior.
        /// As a sketch, the store can maintain a nonce value alongside the checkpoint
        /// of each task split. The nonce is updated on each open of this RPC,
        /// and each commit transaction then verifies that the nonce has not been
        /// changed.
        ///
        /// In the future, if another RPC opens and updates the nonce, it fences off
        /// this instance of the task split and prevents it from committing further
        /// transactions.
        ///
        /// Implementation Pattern: Recovery Log with Non-Transactional Store
        /// =================================================================
        ///
        /// In this pattern, the recovery log persists the Flow checkpoint and handles
        /// fencing semantics. During the load and store phases, the driver
        /// directly manipulates a non-transactional store or API.
        ///
        /// Note that this pattern is at-least-once. A transaction may fail part-way
        /// through and be restarted, causing its effects to be partially or fully
        /// replayed.
        ///
        /// Care must be taken if the collection's schema has reduction annotations
        /// such as `sum`, as those reductions may be applied more than once due to
        /// a partially completed, but ultimately failed transaction.
        ///
        /// If the collection's schema is last-write-wins, this mode still provides
        /// effectively-once behavior. Collections which aren't last-write-wins
        /// can be turned into last-write-wins through the use of derivation
        /// registers.
        ///
        /// Implementation Pattern: Recovery Log with Idempotent Apply
        /// ==========================================================
        ///
        /// In this pattern the recovery log is authoritative, but the driver uses
        /// external stable storage to stage the effects of a transaction -- rather
        /// than directly applying them to the store -- such that those effects can be
        /// idempotently applied after the transaction commits.
        ///
        /// This allows stores which feature a weaker transactionality guarantee to
        /// still be used in an exactly-once way, so long as they support an idempotent
        /// apply operation.
        ///
        /// Driver checkpoints can facilitate this pattern. For example, a driver might
        /// generate a unique filename in S3 and reference it in its prepared
        /// checkpoint, which is committed to the recovery log. During the "store"
        /// phase, it writes to this S3 file. After the transaction commits, it tells
        /// the store of the new file to incorporate. The store must handle
        /// idempotency, by applying the effects of the unique file just once, even if
        /// told of the file multiple times.
        ///
        /// A related extension of this pattern is for the driver to embed a Flow
        /// checkpoint into its driver checkpoint. Doing so allows the driver to
        /// express an intention to restart from an older alternative checkpoint, as
        /// compared to the most recent committed checkpoint of the recovery log.
        ///
        /// As mentioned above, it's crucial that store states and checkpoints commit
        /// together. While seemingly bending that rule, this pattern is consistent
        /// with it because, on commit, the semantic contents of the store include BOTH
        /// its base state, as well as the staged idempotent update. The store just may
        /// not know it yet, but eventually it must because of the retried idempotent
        /// apply.
        ///
        /// Note the driver must therefore ensure that staged updates are fully applied
        /// before returning an "load" responses, in order to provide the correct
        /// read-committed semantics required by the Flow runtime.
        ///
        /// RPC Lifecycle
        /// =============
        ///
        /// The RPC follows the following lifecycle:
        ///
        /// :TransactionRequest.Open:
        ///    - The Flow runtime opens the stream.
        /// :TransactionResponse.Opened:
        ///    - If the remote store is authoritative, it must fence off other RPCs
        ///      of this task split from committing further transactions,
        ///      and it retrieves a Flow checkpoint which is returned to the runtime.
        ///
        /// TransactionRequest.Open and TransactionResponse.Opened are sent only
        /// once, at the commencement of the stream. Thereafter the protocol loops:
        ///
        /// Load phase
        /// ==========
        ///
        /// The Load phases is Load requests *intermixed* with one
        /// Acknowledge/Acknowledged message flow. The driver must accomodate an
        /// Acknowledge that occurs before, during, or after a sequence of Load
        /// requests. It's guaranteed to see exactly one Acknowledge request during
        /// this phase.
        ///
        /// :TransactionRequest.Acknowledge:
        ///    - The runtime tells the driver that a commit to the recovery log has
        ///      completed.
        ///    - The driver applies a staged update to the base store, where
        ///      applicable.
        ///    - Note Acknowledge is sent in the very first iteration for consistency.
        ///      Semantically, it's an acknowledgement of the recovered checkpoint.
        ///      If a previous invocation failed after recovery log commit but before
        ///      applying the staged change, this is an opportunity to ensure that
        ///      apply occurs.
        /// :TransactionResponse.Acknowledged:
        ///    - The driver responds to the runtime only after applying a staged
        ///      update, where applicable.
        ///    - If there is no staged update, the driver immediately responds on
        ///      seeing Acknowledge.
        ///
        /// :TransactionRequest.Load:
        ///    - The runtime sends zero or more Load messages.
        ///    - The driver may send any number of TransactionResponse.Loaded in
        ///      response.
        ///    - If the driver will apply a staged update, it must await Acknowledge
        ///      and have applied the update to the store *before* evaluating any
        ///      Loads, to ensure correct read-committed behavior.
        ///    - The driver may defer responding with some or all loads until the
        ///      prepare phase.
        /// :TransactionResponse.Loaded:
        ///    - The driver sends zero or more Loaded messages, once for each loaded
        ///      document.
        ///    - Document keys not found in the store are omitted and not sent as
        ///      Loaded.
        ///
        /// Prepare phase
        /// =============
        ///
        /// The prepare phase begins only after the prior transaction has both
        /// committed and also been acknowledged. It marks the bounds of the present
        /// transaction.
        ///
        /// Upon entering this phase, the driver must immediately evaluate any deferred
        /// Load requests and send remaining Loaded responses.
        ///
        /// :TransactionRequest.Prepare:
        ///    - The runtime sends a Prepare message with its Flow checkpoint.
        /// :TransactionResponse.Prepared:
        ///    - The driver sends Prepared after having flushed all Loaded responses.
        ///    - The driver may include a driver checkpoint update which will be
        ///      committed to the recovery log with this transaction.
        ///
        /// Store phase
        /// ===========
        ///
        /// The store phase is when the runtime sends the driver materialized document
        /// updates, as well as an indication of whether the document is an insert,
        /// update, or delete (in other words, was it returned in a Loaded response?).
        ///
        /// :TransactionRequest.Store:
        ///    - The runtime sends zero or more Store messages.
        ///
        /// Commit phase
        /// ============
        ///
        /// The commit phase marks the end of the store phase, and tells the driver of
        /// the runtime's intent to commit to its recovery log. If the remote store is
        /// authoritative, the driver must commit its transaction at this time.
        ///
        /// :TransactionRequest.Commit:
        ///    - The runtime sends a Commit message, denoting its intention to commit.
        ///    - If the remote store is authoritative, the driver includes the Flow
        ///      checkpoint into its transaction and commits it along with view state
        ///      updates.
        ///    - Otherwise, the driver immediately responds with DriverCommitted.
        /// :TransactionResponse.DriverCommitted:
        ///    - The driver sends a DriverCommitted message.
        ///    - The runtime commits Flow and driver checkpoint to its recovery
        ///      log. The completion of this commit will be marked by an
        ///      Acknowledge during the next load phase.
        ///    - Runtime and driver begin a new, pipelined transaction by looping to
        ///      load while this transaction continues to commit.
        ///
        /// An error of any kind rolls back the transaction in progress and terminates
        /// the stream.
        async fn transactions(
            &self,
            request: tonic::Request<tonic::Streaming<super::TransactionRequest>>,
        ) -> Result<tonic::Response<Self::TransactionsStream>, tonic::Status>;
    }
    /// Driver is the service implemented by a materialization connector.
    #[derive(Debug)]
    pub struct DriverServer<T: Driver> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Driver> DriverServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for DriverServer<T>
    where
        T: Driver,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/materialize.Driver/Spec" => {
                    #[allow(non_camel_case_types)]
                    struct SpecSvc<T: Driver>(pub Arc<T>);
                    impl<T: Driver> tonic::server::UnaryService<super::SpecRequest>
                    for SpecSvc<T> {
                        type Response = super::SpecResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SpecRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).spec(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SpecSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/materialize.Driver/Validate" => {
                    #[allow(non_camel_case_types)]
                    struct ValidateSvc<T: Driver>(pub Arc<T>);
                    impl<T: Driver> tonic::server::UnaryService<super::ValidateRequest>
                    for ValidateSvc<T> {
                        type Response = super::ValidateResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ValidateRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).validate(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ValidateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/materialize.Driver/ApplyUpsert" => {
                    #[allow(non_camel_case_types)]
                    struct ApplyUpsertSvc<T: Driver>(pub Arc<T>);
                    impl<T: Driver> tonic::server::UnaryService<super::ApplyRequest>
                    for ApplyUpsertSvc<T> {
                        type Response = super::ApplyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ApplyRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).apply_upsert(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ApplyUpsertSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/materialize.Driver/ApplyDelete" => {
                    #[allow(non_camel_case_types)]
                    struct ApplyDeleteSvc<T: Driver>(pub Arc<T>);
                    impl<T: Driver> tonic::server::UnaryService<super::ApplyRequest>
                    for ApplyDeleteSvc<T> {
                        type Response = super::ApplyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ApplyRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).apply_delete(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ApplyDeleteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/materialize.Driver/Transactions" => {
                    #[allow(non_camel_case_types)]
                    struct TransactionsSvc<T: Driver>(pub Arc<T>);
                    impl<
                        T: Driver,
                    > tonic::server::StreamingService<super::TransactionRequest>
                    for TransactionsSvc<T> {
                        type Response = super::TransactionResponse;
                        type ResponseStream = T::TransactionsStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::TransactionRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).transactions(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = TransactionsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: Driver> Clone for DriverServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Driver> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Driver> tonic::transport::NamedService for DriverServer<T> {
        const NAME: &'static str = "materialize.Driver";
    }
}
