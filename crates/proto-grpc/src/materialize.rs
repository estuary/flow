/// Generated client implementations.
#[cfg(feature = "materialize_client")]
pub mod driver_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
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
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> DriverClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
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
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Spec returns the specification definition of this driver.
        /// Notably this includes its endpoint and resource configuration JSON schema.
        pub async fn spec(
            &mut self,
            request: impl tonic::IntoRequest<::proto_flow::materialize::SpecRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::materialize::SpecResponse>,
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
            let path = http::uri::PathAndQuery::from_static("/materialize.Driver/Spec");
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Validate that store resources and proposed collection bindings are
        /// compatible, and return constraints over the projections of each binding.
        pub async fn validate(
            &mut self,
            request: impl tonic::IntoRequest<::proto_flow::materialize::ValidateRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::materialize::ValidateResponse>,
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
                "/materialize.Driver/Validate",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// ApplyUpsert applies a new or updated materialization to the store.
        pub async fn apply_upsert(
            &mut self,
            request: impl tonic::IntoRequest<::proto_flow::materialize::ApplyRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::materialize::ApplyResponse>,
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
                "/materialize.Driver/ApplyUpsert",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// ApplyDelete deletes an existing materialization from the store.
        pub async fn apply_delete(
            &mut self,
            request: impl tonic::IntoRequest<::proto_flow::materialize::ApplyRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::materialize::ApplyResponse>,
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
                Message = ::proto_flow::materialize::TransactionRequest,
            >,
        ) -> Result<
            tonic::Response<
                tonic::codec::Streaming<::proto_flow::materialize::TransactionResponse>,
            >,
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
            request: tonic::Request<::proto_flow::materialize::SpecRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::materialize::SpecResponse>,
            tonic::Status,
        >;
        /// Validate that store resources and proposed collection bindings are
        /// compatible, and return constraints over the projections of each binding.
        async fn validate(
            &self,
            request: tonic::Request<::proto_flow::materialize::ValidateRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::materialize::ValidateResponse>,
            tonic::Status,
        >;
        /// ApplyUpsert applies a new or updated materialization to the store.
        async fn apply_upsert(
            &self,
            request: tonic::Request<::proto_flow::materialize::ApplyRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::materialize::ApplyResponse>,
            tonic::Status,
        >;
        /// ApplyDelete deletes an existing materialization from the store.
        async fn apply_delete(
            &self,
            request: tonic::Request<::proto_flow::materialize::ApplyRequest>,
        ) -> Result<
            tonic::Response<::proto_flow::materialize::ApplyResponse>,
            tonic::Status,
        >;
        ///Server streaming response type for the Transactions method.
        type TransactionsStream: futures_core::Stream<
                Item = Result<
                    ::proto_flow::materialize::TransactionResponse,
                    tonic::Status,
                >,
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
            request: tonic::Request<
                tonic::Streaming<::proto_flow::materialize::TransactionRequest>,
            >,
        ) -> Result<tonic::Response<Self::TransactionsStream>, tonic::Status>;
    }
    /// Driver is the service implemented by a materialization connector.
    #[derive(Debug)]
    pub struct DriverServer<T: Driver> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
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
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
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
                    impl<
                        T: Driver,
                    > tonic::server::UnaryService<::proto_flow::materialize::SpecRequest>
                    for SpecSvc<T> {
                        type Response = ::proto_flow::materialize::SpecResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                ::proto_flow::materialize::SpecRequest,
                            >,
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
                    impl<
                        T: Driver,
                    > tonic::server::UnaryService<
                        ::proto_flow::materialize::ValidateRequest,
                    > for ValidateSvc<T> {
                        type Response = ::proto_flow::materialize::ValidateResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                ::proto_flow::materialize::ValidateRequest,
                            >,
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
                    impl<
                        T: Driver,
                    > tonic::server::UnaryService<
                        ::proto_flow::materialize::ApplyRequest,
                    > for ApplyUpsertSvc<T> {
                        type Response = ::proto_flow::materialize::ApplyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                ::proto_flow::materialize::ApplyRequest,
                            >,
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
                    impl<
                        T: Driver,
                    > tonic::server::UnaryService<
                        ::proto_flow::materialize::ApplyRequest,
                    > for ApplyDeleteSvc<T> {
                        type Response = ::proto_flow::materialize::ApplyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                ::proto_flow::materialize::ApplyRequest,
                            >,
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
                    > tonic::server::StreamingService<
                        ::proto_flow::materialize::TransactionRequest,
                    > for TransactionsSvc<T> {
                        type Response = ::proto_flow::materialize::TransactionResponse;
                        type ResponseStream = T::TransactionsStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<
                                    ::proto_flow::materialize::TransactionRequest,
                                >,
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
    impl<T: Driver> tonic::server::NamedService for DriverServer<T> {
        const NAME: &'static str = "materialize.Driver";
    }
}
