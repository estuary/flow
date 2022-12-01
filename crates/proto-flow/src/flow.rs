/// Slice represents a contiguous slice of bytes within an associated Arena.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Slice {
    #[prost(uint32, tag="1")]
    pub begin: u32,
    #[prost(uint32, tag="2")]
    pub end: u32,
}
/// UUIDParts is a deconstructed, RFC 4122 v1 variant Universally Unique
/// Identifier as used by Gazette.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UuidParts {
    /// Producer is the unique node identifier portion of a v1 UUID, as the high
    /// 48 bits of |producer_and_flags|. The MSB must be 1 to mark this producer
    /// as "multicast" and not an actual MAC address (as per RFC 4122).
    ///
    /// Bits 49-54 must be zero.
    ///
    /// The low 10 bits are the 10 least-significant bits of the v1 UUID clock
    /// sequence, used by Gazette to represent flags over message transaction
    /// semantics.
    #[prost(fixed64, tag="1")]
    pub producer_and_flags: u64,
    /// Clock is a v1 UUID 60-bit timestamp (60 MSBs), followed by 4 bits of
    /// sequence counter.
    #[prost(fixed64, tag="2")]
    pub clock: u64,
}
/// LambdaSpec describes a Flow transformation lambda and how to invoke it.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LambdaSpec {
    /// If non-empty, this is a TypeScript lambda and the field is its invocation
    /// path. E.x. 'some/derivation/andTransform/Update'.
    #[prost(string, tag="1")]
    pub typescript: ::prost::alloc::string::String,
    /// If non-empty, this is a remote lambda and the field is its invocation URL.
    /// E.x. '<https://my/external/api'.>
    #[prost(string, tag="2")]
    pub remote: ::prost::alloc::string::String,
}
/// Shuffle is a description of a document shuffle, where each document
/// is mapped into:
///   * An extracted, packed composite key (a "shuffle key").
///   * A rotated Clock value (an "r-clock").
/// The packed key and r-clock can then be compared to individual reader
/// RangeSpec's.
///
/// Shuffle is a complete description of how a group of related readers
/// (e.x. a set of shards collectively processing a single derivation or
/// materialization) are performing their read. It contains all (and only!)
/// stable descriptions of the read's behavior, and is a primary structure
/// across both the shuffle server and client implementations.
///
/// Shuffles are also compared using deep equality in order to identify and
/// group related reads, placing all reads having equal Shuffles into common
/// "read rings" which consolidate their underlying journal reads.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Shuffle {
    /// Group to which this shuffle belongs. It's used to suffix all journal
    /// reads undertaken by this shuffle, and must be stable. Examples:
    ///   `derive/{derivation}/{transform}`
    ///   `materialize/{materialization}`
    #[prost(string, tag="1")]
    pub group_name: ::prost::alloc::string::String,
    /// Source collection read by this transform.
    #[prost(string, tag="2")]
    pub source_collection: ::prost::alloc::string::String,
    /// Selector of partitions of the collection which this transform reads.
    #[prost(message, optional, tag="3")]
    pub source_partitions: ::core::option::Option<::proto_gazette::broker::LabelSelector>,
    /// JSON pointer locating the UUID of each source document.
    #[prost(string, tag="4")]
    pub source_uuid_ptr: ::prost::alloc::string::String,
    /// Composite key over which shuffling occurs, specified as one or more
    /// JSON-Pointers indicating a message location to extract.
    #[prost(string, repeated, tag="5")]
    pub shuffle_key_ptrs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// uses_source_key is true if shuffle_key_ptr is the source's native key,
    /// and false if it's some other key. When shuffling using the source's key,
    /// we can minimize data movement by assigning a shard coordinator for each
    /// journal such that the shard's key range overlap that of the journal.
    #[prost(bool, tag="6")]
    pub uses_source_key: bool,
    /// Computed shuffle lambda. If non-nil, then shuffle_key_ptr MUST be empty
    /// and uses_source_key MUST be false.
    #[prost(message, optional, tag="7")]
    pub shuffle_lambda: ::core::option::Option<LambdaSpec>,
    /// filter_r_clocks is true if the shuffle coordinator should filter documents
    /// sent to each subscriber based on its covered r-clock ranges and the
    /// individual document clocks. If false, the subscriber's r-clock range is
    /// ignored and all documents which match the key range are sent.
    ///
    /// filter_r_clocks is set 'true' when reading on behalf of transforms having
    /// a "publish" but not an "update" lambda, as such documents have no
    /// side-effects on the reader's state store, and would not be published anyway
    /// for falling outside of the reader's r-clock range.
    #[prost(bool, tag="11")]
    pub filter_r_clocks: bool,
    /// Number of seconds for which documents of this collection are delayed
    /// while reading, relative to other documents (when back-filling) and the
    /// present wall-clock time (when tailing).
    #[prost(uint32, tag="12")]
    pub read_delay_seconds: u32,
    /// Priority of this shuffle, with respect to other related Shuffle reads
    /// (e.x. Shuffles of a different transformation within the same derivation).
    /// Higher values imply higher priority.
    #[prost(uint32, tag="13")]
    pub priority: u32,
    /// Bundled JSON-schema against which documents are validated. Optional.
    /// If not set, no schema validation is performed by the Shuffle server.
    ///
    /// We always validate documents, but may do so either within the Shuffle
    /// server or later, within the shuffle client:
    /// - Derivations set `validate_schema_json`, as the derivation runtime can
    ///    then by-pass a round of JSON parsing and validation.
    /// - Materializations don't, as the materialization runtime immediately
    ///    combines over the document which requires parsing & validation
    ///    anyway.
    ///
    /// Unlike other schema_json protobuf fields, we don't use a RawMessage
    /// casttype so that the generated Go equals method will work.
    #[prost(string, tag="14")]
    pub validate_schema_json: ::prost::alloc::string::String,
}
/// JournalShuffle is a Shuffle of a Journal by a Coordinator shard.
/// They're compared using deep equality in order to consolidate groups of
/// related logical reads into a single physical read of the journal.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JournalShuffle {
    /// Journal to be shuffled.
    #[prost(string, tag="1")]
    pub journal: ::prost::alloc::string::String,
    /// Coordinator is the Shard ID which is responsible for reads of this journal.
    #[prost(string, tag="2")]
    pub coordinator: ::prost::alloc::string::String,
    /// Shuffle of this JournalShuffle.
    #[prost(message, optional, tag="3")]
    pub shuffle: ::core::option::Option<Shuffle>,
    /// Is this a reply of the journal's content? We separate ongoing vs replayed
    /// reads of a journal's content into distinct rings.
    #[prost(bool, tag="4")]
    pub replay: bool,
    /// Build ID of the task which requested this JournalShuffle.
    #[prost(string, tag="5")]
    pub build_id: ::prost::alloc::string::String,
}
/// Projection is a mapping between a document location, specified as a
/// JSON-Pointer, and a corresponding field string in a flattened
/// (i.e. tabular or SQL) namespace which aliases it.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Projection {
    /// Document location of this projection, as a JSON-Pointer.
    #[prost(string, tag="1")]
    pub ptr: ::prost::alloc::string::String,
    /// Field is the flattened, tabular alias of this projection.
    #[prost(string, tag="2")]
    pub field: ::prost::alloc::string::String,
    /// Was this projection explicitly provided ?
    /// (As opposed to implicitly created through static analysis of the schema).
    #[prost(bool, tag="3")]
    pub explicit: bool,
    /// Does this projection constitute a logical partitioning of the collection?
    #[prost(bool, tag="4")]
    pub is_partition_key: bool,
    /// Does this location form (part of) the collection key?
    #[prost(bool, tag="5")]
    pub is_primary_key: bool,
    /// Inference of this projection.
    #[prost(message, optional, tag="6")]
    pub inference: ::core::option::Option<Inference>,
}
/// Inference details type information which is statically known
/// about a given document location.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Inference {
    /// The possible types for this location.
    /// Subset of ["null", "boolean", "object", "array", "integer", "numeric",
    /// "string"].
    #[prost(string, repeated, tag="1")]
    pub types: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag="3")]
    pub string: ::core::option::Option<inference::String>,
    /// The title from the schema, if provided.
    #[prost(string, tag="4")]
    pub title: ::prost::alloc::string::String,
    /// The description from the schema, if provided.
    #[prost(string, tag="5")]
    pub description: ::prost::alloc::string::String,
    /// The default value from the schema, if provided.
    #[prost(string, tag="6")]
    pub default_json: ::prost::alloc::string::String,
    /// Whether this location is marked as a secret, like a credential or password.
    #[prost(bool, tag="7")]
    pub secret: bool,
    /// Existence of this document location.
    #[prost(enumeration="inference::Exists", tag="8")]
    pub exists: i32,
}
/// Nested message and enum types in `Inference`.
pub mod inference {
    /// String type-specific inferences, or nil iff types doesn't include "string".
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct String {
        /// Annotated Content-Type when the projection is of "string" type.
        #[prost(string, tag="3")]
        pub content_type: ::prost::alloc::string::String,
        /// Annotated format when the projection is of "string" type.
        #[prost(string, tag="4")]
        pub format: ::prost::alloc::string::String,
        /// Annotated Content-Encoding when the projection is of "string" type.
        #[prost(string, tag="7")]
        pub content_encoding: ::prost::alloc::string::String,
        /// Is the Content-Encoding "base64" (case-invariant)?
        #[prost(bool, tag="5")]
        pub is_base64: bool,
        /// Maximum length when the projection is of "string" type. Zero for no
        /// limit.
        #[prost(uint32, tag="6")]
        pub max_length: u32,
    }
    /// Exists enumerates the possible states of existence for a location.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Exists {
        Invalid = 0,
        /// The location must exist.
        Must = 1,
        /// The location may exist or be undefined.
        /// Its schema has explicit keywords which allow it to exist
        /// and which may constrain its shape, such as additionalProperties,
        /// items, unevaluatedProperties, or unevaluatedItems.
        May = 2,
        /// The location may exist or be undefined.
        /// Its schema omits any associated keywords, but the specification's
        /// default behavior allows the location to exist.
        Implicit = 3,
        /// The location cannot exist. For example, it's outside of permitted
        /// array bounds, or is a disallowed property, or has an impossible type.
        Cannot = 4,
    }
    impl Exists {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Exists::Invalid => "INVALID",
                Exists::Must => "MUST",
                Exists::May => "MAY",
                Exists::Implicit => "IMPLICIT",
                Exists::Cannot => "CANNOT",
            }
        }
    }
}
/// Next tag: 10.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionSpec {
    /// Name of this collection.
    #[prost(string, tag="1")]
    pub collection: ::prost::alloc::string::String,
    /// JSON-schema URI against which written collection documents are validated,
    /// and which provides write-time reduction annotations.
    #[prost(string, tag="2")]
    pub write_schema_uri: ::prost::alloc::string::String,
    /// Bundled JSON-schema of the collection
    #[prost(string, tag="8")]
    pub write_schema_json: ::prost::alloc::string::String,
    /// Composite key of the collection, as JSON-Pointers.
    #[prost(string, repeated, tag="3")]
    pub key_ptrs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// JSON pointer locating the UUID of each collection document.
    #[prost(string, tag="4")]
    pub uuid_ptr: ::prost::alloc::string::String,
    /// Logical partition fields of this collection.
    #[prost(string, repeated, tag="5")]
    pub partition_fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Logical projections of this collection
    #[prost(message, repeated, tag="6")]
    pub projections: ::prost::alloc::vec::Vec<Projection>,
    /// JSON-encoded document template for creating Gazette consumer
    /// transaction acknowledgements of writes into this collection.
    #[prost(string, tag="7")]
    pub ack_json_template: ::prost::alloc::string::String,
    /// Template for partitions of this collection.
    #[prost(message, optional, tag="9")]
    pub partition_template: ::core::option::Option<::proto_gazette::broker::JournalSpec>,
}
/// TransformSpec describes a specific transform of a derivation.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransformSpec {
    /// Derivation this transform belongs to.
    #[prost(string, tag="1")]
    pub derivation: ::prost::alloc::string::String,
    /// Name of this transform, scoped to it's derivation.
    #[prost(string, tag="2")]
    pub transform: ::prost::alloc::string::String,
    /// Shuffle applied to source documents for this transform.
    #[prost(message, optional, tag="3")]
    pub shuffle: ::core::option::Option<Shuffle>,
    /// Update lambda of this transform, if any.
    #[prost(message, optional, tag="4")]
    pub update_lambda: ::core::option::Option<LambdaSpec>,
    /// Publish lambda of this transform, if any.
    #[prost(message, optional, tag="5")]
    pub publish_lambda: ::core::option::Option<LambdaSpec>,
}
/// DerivationSpec describes a collection, and it's means of derivation.
///
/// Next tag: 8.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DerivationSpec {
    /// Derivations are collections.
    #[prost(message, optional, tag="1")]
    pub collection: ::core::option::Option<CollectionSpec>,
    /// JSON-schema URI against which derivation registers are validated,
    /// and which provides reduction annotations. Register schemas are always
    /// local to this build, and are a resource URL and fragment pointer.
    #[prost(string, tag="2")]
    pub register_schema_uri: ::prost::alloc::string::String,
    /// Bundled JSON-schema against which register documents are validated.
    #[prost(string, tag="7")]
    pub register_schema_json: ::prost::alloc::string::String,
    /// JSON-encoded initial value of novel document registers.
    #[prost(string, tag="3")]
    pub register_initial_json: ::prost::alloc::string::String,
    /// Transforms of this derivation.
    #[prost(message, repeated, tag="4")]
    pub transforms: ::prost::alloc::vec::Vec<TransformSpec>,
    /// Template for shards of this derivation.
    #[prost(message, optional, tag="5")]
    pub shard_template: ::core::option::Option<::proto_gazette::consumer::ShardSpec>,
    /// Template for recovery logs of shards of this derivation.
    #[prost(message, optional, tag="6")]
    pub recovery_log_template: ::core::option::Option<::proto_gazette::broker::JournalSpec>,
}
/// FieldSelection is a selection of a collection's projection fields.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldSelection {
    /// Fields for each key component of the collection. Included key fields appear
    /// in the collection's key component order, and a given key pointer will be
    /// included at most once.
    #[prost(string, repeated, tag="1")]
    pub keys: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// All other selected fields, other than those in keys and the document field.
    /// Entries are in ascending sorted order, and may be empty.
    #[prost(string, repeated, tag="2")]
    pub values: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Field having a document pointer located at the document root.
    #[prost(string, tag="3")]
    pub document: ::prost::alloc::string::String,
    /// Additional configuration, keyed by fields included in |keys|, |values|, or
    /// |document|. Values are arbitrary JSON-encoded objects.
    #[prost(map="string, string", tag="4")]
    pub field_config_json: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
/// CaptureSpec describes a collection and its capture from an endpoint.
///
/// Next tag: 8.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CaptureSpec {
    /// Name of this capture.
    #[prost(string, tag="1")]
    pub capture: ::prost::alloc::string::String,
    /// Type of the captures's endpoint.
    #[prost(enumeration="EndpointType", tag="2")]
    pub endpoint_type: i32,
    /// JSON-encoded object which specifies this capture with
    /// respect to the endpoint type driver.
    #[prost(string, tag="3")]
    pub endpoint_spec_json: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="4")]
    pub bindings: ::prost::alloc::vec::Vec<capture_spec::Binding>,
    /// Minimum interval of time between successive invocations of the capture.
    #[prost(uint32, tag="5")]
    pub interval_seconds: u32,
    /// Template for shards of this capture.
    #[prost(message, optional, tag="6")]
    pub shard_template: ::core::option::Option<::proto_gazette::consumer::ShardSpec>,
    /// Template for recovery logs of shards of this capture.
    #[prost(message, optional, tag="7")]
    pub recovery_log_template: ::core::option::Option<::proto_gazette::broker::JournalSpec>,
}
/// Nested message and enum types in `CaptureSpec`.
pub mod capture_spec {
    /// Bindings of endpoint resources and collections into which they're captured.
    /// Bindings are ordered and unique on the bound collection name,
    /// and are also unique on the resource path.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Binding {
        /// JSON-encoded object which specifies the endpoint resource to be captured.
        #[prost(string, tag="1")]
        pub resource_spec_json: ::prost::alloc::string::String,
        /// Driver-supplied path components which fully qualify the
        /// subresource being captured.
        #[prost(string, repeated, tag="2")]
        pub resource_path: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// Collection to be captured into.
        #[prost(message, optional, tag="3")]
        pub collection: ::core::option::Option<super::CollectionSpec>,
    }
}
/// MaterializationSpec describes a collection and its materialization to an
/// endpoint.
///
/// Next tag: 7.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MaterializationSpec {
    /// Name of this materialization.
    #[prost(string, tag="1")]
    pub materialization: ::prost::alloc::string::String,
    /// Type of the materialization's endpoint.
    #[prost(enumeration="EndpointType", tag="2")]
    pub endpoint_type: i32,
    /// JSON-encoded object which specifies this materialization with
    /// respect to the endpoint type driver.
    #[prost(string, tag="3")]
    pub endpoint_spec_json: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="4")]
    pub bindings: ::prost::alloc::vec::Vec<materialization_spec::Binding>,
    /// Template for shards of this materialization.
    #[prost(message, optional, tag="5")]
    pub shard_template: ::core::option::Option<::proto_gazette::consumer::ShardSpec>,
    /// Template for recovery logs of shards of this materialization.
    #[prost(message, optional, tag="6")]
    pub recovery_log_template: ::core::option::Option<::proto_gazette::broker::JournalSpec>,
}
/// Nested message and enum types in `MaterializationSpec`.
pub mod materialization_spec {
    /// Bindings of endpoint resources and collections from which they're
    /// materialized. Bindings are ordered and unique on the bound collection name,
    /// and are also unique on the resource path.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Binding {
        /// JSON-encoded object which specifies the endpoint resource to be
        /// materialized.
        #[prost(string, tag="1")]
        pub resource_spec_json: ::prost::alloc::string::String,
        /// Driver-supplied path components which fully qualify the
        /// subresource being materialized.
        #[prost(string, repeated, tag="2")]
        pub resource_path: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// Collection to be materialized.
        #[prost(message, optional, tag="3")]
        pub collection: ::core::option::Option<super::CollectionSpec>,
        /// Resolved fields selected for materialization.
        #[prost(message, optional, tag="4")]
        pub field_selection: ::core::option::Option<super::FieldSelection>,
        /// Materialize delta updates of documents rather than full reductions.
        #[prost(bool, tag="5")]
        pub delta_updates: bool,
        /// Shuffle applied to collection documents for this materialization binding.
        #[prost(message, optional, tag="6")]
        pub shuffle: ::core::option::Option<super::Shuffle>,
    }
}
/// OAuth2Spec describes an OAuth2 provider
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OAuth2Spec {
    /// Name of the OAuth2 provider. This is a machine-readable key and must stay
    /// consistent. One example use case is to map providers to their respective
    /// style of buttons in the UI
    #[prost(string, tag="1")]
    pub provider: ::prost::alloc::string::String,
    // The templates below are handlebars templates and have a set of variables
    // available to them, the variables available everywhere are:
    // client_id: OAuth2 provider client id
    // redirect_uri: OAuth2 provider client registered redirect URI
    //
    // Variables available in Auth URL request:
    // state: the state parameter: this parameter is used to prevent attacks
    // against our users. the parameter must be generated randomly and not
    // guessable. It must be associated with a user session, and we must check in
    // our redirect URI that the state we receive from the OAuth provider is the
    // same as the one we passed in. Scenario: user A can initiate an OAuth2 flow,
    // and send the OAuth Provider's Login URL to another person, user B. Once
    // this other person logs in through the OAuth2 Provider, they will be
    // redirected, and if there is no state check, we will authorise user A
    // to access user B's account. With the state check, the state will not be
    // available in user B's session, and therefore the state check will fail,
    // preventing the attack.
    //
    // Variables available in Access Token request:
    // code: the code resulting from the suthorization step used to fetch the
    // token client_secret: OAuth2 provider client secret
    //
    // Variables available on Refresh Token request:
    // refresh_token: the refresh token
    // client_secret: OAuth2 provider client secret

    /// Template for authorization URL, this is the first step of the OAuth2 flow
    /// where the user is redirected to the OAuth2 provider to authorize access to
    /// their account
    #[prost(string, tag="2")]
    pub auth_url_template: ::prost::alloc::string::String,
    /// Template for access token URL, this is the second step of the OAuth2 flow,
    /// where we request an access token from the provider
    #[prost(string, tag="3")]
    pub access_token_url_template: ::prost::alloc::string::String,
    /// The method used to send access_token request. POST by default.
    #[prost(string, tag="11")]
    pub access_token_method: ::prost::alloc::string::String,
    /// The POST body of the access_token request
    #[prost(string, tag="4")]
    pub access_token_body: ::prost::alloc::string::String,
    /// Headers for the access_token request
    #[prost(string, tag="5")]
    pub access_token_headers_json: ::prost::alloc::string::String,
    /// A json map that maps the response from the OAuth provider for Access Token
    /// request to keys in the connector endpoint configuration.
    /// If the connector supports refresh tokens, must include `refresh_token` and
    /// `expires_in`. If this mapping is not provided, the keys from the response
    /// are passed as-is to the connector config.
    #[prost(string, tag="6")]
    pub access_token_response_map_json: ::prost::alloc::string::String,
    /// Template for refresh token URL, some providers require that the access
    /// token be refreshed.
    #[prost(string, tag="7")]
    pub refresh_token_url_template: ::prost::alloc::string::String,
    /// The method used to send refresh_token request. POST by default.
    #[prost(string, tag="12")]
    pub refresh_token_method: ::prost::alloc::string::String,
    /// The POST body of the refresh_token request
    #[prost(string, tag="8")]
    pub refresh_token_body: ::prost::alloc::string::String,
    /// Headers for the refresh_token request
    #[prost(string, tag="9")]
    pub refresh_token_headers_json: ::prost::alloc::string::String,
    /// A json map that maps the response from the OAuth provider for Refresh Token
    /// request to keys in the connector endpoint configuration.
    /// If the connector supports refresh tokens, must include `refresh_token` and
    /// `expires_in`. If this mapping is not provided, the keys from the response
    /// are passed as-is to the connector config.
    #[prost(string, tag="10")]
    pub refresh_token_response_map_json: ::prost::alloc::string::String,
}
/// TestSpec describes a catalog test.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TestSpec {
    /// Name of this test.
    #[prost(string, tag="1")]
    pub test: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="2")]
    pub steps: ::prost::alloc::vec::Vec<test_spec::Step>,
}
/// Nested message and enum types in `TestSpec`.
pub mod test_spec {
    /// Steps of the test.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Step {
        #[prost(enumeration="step::Type", tag="1")]
        pub step_type: i32,
        /// Index of this step within the test.
        #[prost(uint32, tag="2")]
        pub step_index: u32,
        /// Description of this step.
        #[prost(string, tag="3")]
        pub description: ::prost::alloc::string::String,
        /// Scope of the test definition location.
        #[prost(string, tag="4")]
        pub step_scope: ::prost::alloc::string::String,
        /// Collection ingested or verified by this step.
        #[prost(string, tag="5")]
        pub collection: ::prost::alloc::string::String,
        /// Newline-separated JSON documents to ingest or verify.
        #[prost(string, tag="6")]
        pub docs_json_lines: ::prost::alloc::string::String,
        /// When verifying, selector over logical partitions of the collection.
        #[prost(message, optional, tag="7")]
        pub partitions: ::core::option::Option<::proto_gazette::broker::LabelSelector>,
    }
    /// Nested message and enum types in `Step`.
    pub mod step {
        /// Type of this step.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
        #[repr(i32)]
        pub enum Type {
            Ingest = 0,
            Verify = 1,
        }
        impl Type {
            /// String value of the enum field names used in the ProtoBuf definition.
            ///
            /// The values are not transformed in any way and thus are considered stable
            /// (if the ProtoBuf definition does not change) and safe for programmatic use.
            pub fn as_str_name(&self) -> &'static str {
                match self {
                    Type::Ingest => "INGEST",
                    Type::Verify => "VERIFY",
                }
            }
        }
    }
}
/// RangeSpec describes the ranges of shuffle keys and r-clocks which a reader
/// is responsible for.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RangeSpec {
    /// [begin, end] inclusive range of keys to be shuffled to this reader.
    /// Ranges are with respect to a 32-bit hash of a packed document key.
    ///
    /// The choice of hash function is important: while it need not be
    /// cryptographically secure, it must demonstrate a strong "avalanche effect"
    /// (ideally meeting the strict avalanche criterion), to ensure that small
    /// pertubations of input keys are equally likely to independently affect
    /// hash output bits. Particularly the higest bits of the hash result,
    /// which dominate the selection of a shuffled shard.
    ///
    /// At present, Flow uses the high 32 bits of a HighWayHash 64-bit
    /// checksum, using a fixed 32-byte key.
    #[prost(fixed32, tag="2")]
    pub key_begin: u32,
    #[prost(fixed32, tag="3")]
    pub key_end: u32,
    /// Rotated [begin, end] inclusive ranges of Clocks.
    #[prost(fixed32, tag="4")]
    pub r_clock_begin: u32,
    #[prost(fixed32, tag="5")]
    pub r_clock_end: u32,
}
/// ShuffleRequest is the request message of a Shuffle RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleRequest {
    /// Journal to be shuffled, routed to a coordinator.
    #[prost(message, optional, tag="1")]
    pub shuffle: ::core::option::Option<JournalShuffle>,
    /// Resolution header of the |shuffle.coordinator| shard.
    #[prost(message, optional, tag="2")]
    pub resolution: ::core::option::Option<::proto_gazette::broker::Header>,
    /// Ranges of responsibility which are unique to this reader,
    /// against which document shuffle outcomes are matched to determine
    /// read eligibility.
    #[prost(message, optional, tag="3")]
    pub range: ::core::option::Option<RangeSpec>,
    /// Offset to begin reading the journal from.
    #[prost(int64, tag="4")]
    pub offset: i64,
    /// Offset to stop reading the journal at, or zero if unbounded.
    #[prost(int64, tag="5")]
    pub end_offset: i64,
}
/// ShuffleResponse is the streamed response message of a Shuffle RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleResponse {
    /// Status of the Shuffle RPC.
    #[prost(enumeration="::proto_gazette::consumer::Status", tag="1")]
    pub status: i32,
    /// Header of the response.
    #[prost(message, optional, tag="2")]
    pub header: ::core::option::Option<::proto_gazette::broker::Header>,
    /// Terminal error encountered while serving this ShuffleRequest. A terminal
    /// error is only sent if a future ShuffleRequest of this same configuration
    /// and offset will fail in the exact same way, and operator intervention is
    /// required to properly recover. Such errors are returned so that the caller
    /// can also abort with a useful, contextual error message.
    ///
    /// Examples of terminal errors include the requested journal not existing,
    /// or data corruption. Errors *not* returned as |terminal_error| include
    /// network errors, process failures, and other conditions which can be
    /// retried.
    #[prost(string, tag="3")]
    pub terminal_error: ::prost::alloc::string::String,
    /// Offset which was read through to produce this ShuffleResponse.
    #[prost(int64, tag="4")]
    pub read_through: i64,
    /// WriteHead of the journal as reported by the broker, as of the creation of
    /// this ShuffleResponse.
    #[prost(int64, tag="5")]
    pub write_head: i64,
    /// Memory arena of this message.
    #[prost(bytes="vec", tag="6")]
    pub arena: ::prost::alloc::vec::Vec<u8>,
    /// Shuffled documents, each encoded in the 'application/json'
    /// media-type.
    #[prost(message, repeated, tag="7")]
    pub docs_json: ::prost::alloc::vec::Vec<Slice>,
    /// The journal offsets of each document within the requested journal.
    /// For a document at index i, its offsets are [ offsets\[2*i\], offsets\[2*i+1\]
    /// ).
    #[prost(int64, repeated, packed="false", tag="8")]
    pub offsets: ::prost::alloc::vec::Vec<i64>,
    /// UUIDParts of each document.
    #[prost(message, repeated, tag="9")]
    pub uuid_parts: ::prost::alloc::vec::Vec<UuidParts>,
    /// Packed, embedded encoding of the shuffle key into a byte string.
    /// If the Shuffle specified a Hash to use, it's applied as well.
    #[prost(message, repeated, tag="10")]
    pub packed_key: ::prost::alloc::vec::Vec<Slice>,
}
/// DriverCheckpoint is a driver-originated checkpoint withn a capture or
/// materialization.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DriverCheckpoint {
    /// Optional driver checkpoint of this transaction, to be persisted
    /// by the Flow runtime and returned in a future transaction stream.
    /// If empty, then a previous checkpoint is cleared.
    #[prost(bytes="vec", tag="1")]
    pub driver_checkpoint_json: ::prost::alloc::vec::Vec<u8>,
    /// If true, then the driver checkpoint must be non-empty and is
    /// applied as an RFC7396 Merge Patch atop the immediately preceeding
    /// checkpoint (or to an empty JSON object `{}` if there is no checkpoint).
    #[prost(bool, tag="2")]
    pub rfc7396_merge_patch: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractApi {
}
/// Nested message and enum types in `ExtractAPI`.
pub mod extract_api {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// JSON pointer of the document UUID to extract.
        #[prost(string, tag="1")]
        pub uuid_ptr: ::prost::alloc::string::String,
        /// JSON schema to validate non-ACK documents against.
        /// If empty then schema validation is not performed.
        #[prost(string, tag="2")]
        pub schema_json: ::prost::alloc::string::String,
        /// Field JSON pointers to extract from documents and return as packed
        /// tuples.
        #[prost(string, repeated, tag="3")]
        pub field_ptrs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    }
    /// Code labels message codes passed over the CGO bridge.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Code {
        Invalid = 0,
        /// Configure or re-configure the extractor (Go -> Rust).
        Configure = 1,
        /// Extract from a document (Go -> Rust).
        Extract = 2,
        /// UUID extracted from a document (Rust -> Go).
        ExtractedUuid = 3,
        /// Fields extracted from a document (Rust -> Go).
        ExtractedFields = 4,
    }
    impl Code {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Code::Invalid => "INVALID",
                Code::Configure => "CONFIGURE",
                Code::Extract => "EXTRACT",
                Code::ExtractedUuid => "EXTRACTED_UUID",
                Code::ExtractedFields => "EXTRACTED_FIELDS",
            }
        }
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CombineApi {
}
/// Nested message and enum types in `CombineAPI`.
pub mod combine_api {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// JSON schema against which documents are to be validated,
        /// and which provides reduction annotations.
        #[prost(string, tag="1")]
        pub schema_json: ::prost::alloc::string::String,
        /// Composite key used to group documents to be combined, specified as one or
        /// more JSON-Pointers indicating a message location to extract.
        /// If empty, all request documents are combined into a single response
        /// document.
        #[prost(string, repeated, tag="2")]
        pub key_ptrs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// Field JSON pointers to be extracted from combined documents and returned.
        /// If empty, no fields are extracted.
        #[prost(string, repeated, tag="3")]
        pub field_ptrs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// JSON-Pointer at which a placeholder UUID should be inserted into
        /// returned documents. If empty, no placeholder is inserted.
        #[prost(string, tag="4")]
        pub uuid_placeholder_ptr: ::prost::alloc::string::String,
    }
    /// Stats holds statistics relating to one or more combiner transactions.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Stats {
        #[prost(message, optional, tag="1")]
        pub left: ::core::option::Option<super::DocsAndBytes>,
        #[prost(message, optional, tag="2")]
        pub right: ::core::option::Option<super::DocsAndBytes>,
        #[prost(message, optional, tag="3")]
        pub out: ::core::option::Option<super::DocsAndBytes>,
    }
    /// Code labels message codes passed over the CGO bridge.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Code {
        Invalid = 0,
        /// Configure or re-configure the combiner (Go -> Rust).
        /// A combiner may be configured only on first initialization,
        /// or immediately after having drained.
        Configure = 1,
        /// Reduce a left-hand side document (Go -> Rust).
        ReduceLeft = 2,
        /// Combine a right-hand side document (Go -> Rust).
        CombineRight = 3,
        // The following DRAIN_* / DRAINED_* enum codes are
        // shared with DeriveAPI:

        /// Drain the combiner (Go -> Rust).
        DrainChunk = 200,
        /// Next drained document is partially combined (Rust -> Go).
        DrainedCombinedDocument = 201,
        /// Next drained document is fully reduced (Rust -> Go).
        DrainedReducedDocument = 202,
        /// Next drained key (follows drained document; Rust -> Go).
        DrainedKey = 203,
        /// Next drained fields (follows key; Rust -> Go).
        DrainedFields = 204,
        /// Drain stats, sent after all documents have been drained. (Rust -> Go)
        DrainedStats = 205,
    }
    impl Code {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Code::Invalid => "INVALID",
                Code::Configure => "CONFIGURE",
                Code::ReduceLeft => "REDUCE_LEFT",
                Code::CombineRight => "COMBINE_RIGHT",
                Code::DrainChunk => "DRAIN_CHUNK",
                Code::DrainedCombinedDocument => "DRAINED_COMBINED_DOCUMENT",
                Code::DrainedReducedDocument => "DRAINED_REDUCED_DOCUMENT",
                Code::DrainedKey => "DRAINED_KEY",
                Code::DrainedFields => "DRAINED_FIELDS",
                Code::DrainedStats => "DRAINED_STATS",
            }
        }
    }
}
/// DeriveAPI is a meta-message which name spaces messages of the Derive API
/// bridge.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeriveApi {
}
/// Nested message and enum types in `DeriveAPI`.
pub mod derive_api {
    /// Open the registers database.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Open {
        /// Memory address of an RocksDB Environment to use (as a *rocksdb_env_t).
        /// Ownership of the environment is transferred with this message.
        #[prost(fixed64, tag="1")]
        pub rocksdb_env_memptr: u64,
        /// Local directory for ephemeral processing state.
        #[prost(string, tag="2")]
        pub local_dir: ::prost::alloc::string::String,
    }
    /// Config configures the derived DerivationSpec and its associated schema
    /// index.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// Derivation to derive.
        #[prost(message, optional, tag="1")]
        pub derivation: ::core::option::Option<super::DerivationSpec>,
    }
    /// DocHeader precedes a JSON-encoded document.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DocHeader {
        /// UUID of this document.
        #[prost(message, optional, tag="1")]
        pub uuid: ::core::option::Option<super::UuidParts>,
        /// FDB packed shuffle key of the document.
        #[prost(bytes="vec", tag="2")]
        pub packed_key: ::prost::alloc::vec::Vec<u8>,
        /// Index of the transformation under which this document is being
        /// processed, within the configured DerivationSpec.
        #[prost(uint32, tag="3")]
        pub transform_index: u32,
    }
    /// Invoke a lambda, using Rust-owned memory buffers of invocation content.
    /// Memory will remain pinned until the trampoline task completion.
    /// |sources_length| will never be zero. If |registers_length| is zero,
    /// this invocation is of the update lambda. Otherwise, it's the publish
    /// lambda.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Invoke {
        /// Index of the transformation to be invoked within DerivationSpec.
        #[prost(uint32, tag="1")]
        pub transform_index: u32,
        /// Memory pointer and length of comma-separated source documents.
        #[prost(fixed64, tag="2")]
        pub sources_memptr: u64,
        #[prost(uint64, tag="3")]
        pub sources_length: u64,
        /// Memory pointer and length of comma-separated register documents.
        #[prost(fixed64, tag="4")]
        pub registers_memptr: u64,
        #[prost(uint64, tag="5")]
        pub registers_length: u64,
    }
    /// Prepare a commit of the transaction.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Prepare {
        /// Checkpoint to commit.
        #[prost(message, optional, tag="1")]
        pub checkpoint: ::core::option::Option<::proto_gazette::consumer::Checkpoint>,
    }
    /// Stats holds statistics relating to a single derive transaction.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Stats {
        /// Array indexed by transform_index with stats per transform.
        #[prost(message, repeated, tag="1")]
        pub transforms: ::prost::alloc::vec::Vec<stats::TransformStats>,
        #[prost(message, optional, tag="2")]
        pub registers: ::core::option::Option<stats::RegisterStats>,
        /// The documents drained from the derive pipeline's combiner. This is not
        /// necessarily the same as the sum of all publish lambda outputs because
        /// those outputs may be further reduced.
        #[prost(message, optional, tag="3")]
        pub output: ::core::option::Option<super::DocsAndBytes>,
    }
    /// Nested message and enum types in `Stats`.
    pub mod stats {
        /// Stats about the invocation of update or publish lambdas.
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct InvokeStats {
            /// The total number of documents and bytes that were output from the
            /// invocations.
            #[prost(message, optional, tag="1")]
            pub output: ::core::option::Option<super::super::DocsAndBytes>,
            /// Sum total duration of all invocations, in seconds.
            #[prost(double, tag="2")]
            pub total_seconds: f64,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct TransformStats {
            /// The total inputs that were fed into this transform.
            #[prost(message, optional, tag="1")]
            pub input: ::core::option::Option<super::super::DocsAndBytes>,
            /// Results of invoking the update lambda.
            #[prost(message, optional, tag="2")]
            pub update: ::core::option::Option<InvokeStats>,
            /// Results of invoking the publish lambda.
            #[prost(message, optional, tag="3")]
            pub publish: ::core::option::Option<InvokeStats>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct RegisterStats {
            /// The number of new register values that were created and added to the
            /// registers database. In the future, it may be nice to also expose stats
            /// related to the size of documents stored within registers, but it's not
            /// obvious how to count updates to existing values as a result of
            /// reductions. So this lone field represents the cerservative subset of
            /// register stats that I feel confident we can and should expose as part
            /// of the user-facing stats.
            #[prost(uint32, tag="1")]
            pub created: u32,
        }
    }
    /// Codes passed over the CGO bridge.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Code {
        Invalid = 0,
        /// Open the registers database (Go -> Rust).
        Open = 1,
        /// Restore the last checkpoint from an opened database (Go <-> Rust).
        RestoreCheckpoint = 2,
        /// Configure or re-configure the derive API (Go -> Rust).
        Configure = 3,
        /// Begin a new transaction (Go -> Rust).
        BeginTransaction = 4,
        /// Next source document header (Go -> Rust).
        NextDocumentHeader = 5,
        /// Next source document body (Go -> Rust).
        NextDocumentBody = 6,
        /// Trampoline task start or completion (Rust <-> Go).
        Trampoline = 7,
        /// Trampoline sub-type: invoke transform lambda.
        TrampolineInvoke = 8,
        /// Flush transaction (Go -> Rust).
        FlushTransaction = 9,
        /// Transaction completed flushing (Rust -> Go).
        FlushedTransaction = 10,
        /// Prepare transaction to commit (Go -> Rust).
        PrepareToCommit = 11,
        /// Clear registers values (test support only; Go -> Rust).
        ClearRegisters = 12,
        // The following DRAIN_* / DRAINED_* enum codes are
        // shared with CombineAPI:

        /// Drain the combiner (Go -> Rust).
        DrainChunk = 200,
        /// Next drained document is partially combined (Rust -> Go).
        DrainedCombinedDocument = 201,
        /// Next drained document is fully reduced (Rust -> Go).
        DrainedReducedDocument = 202,
        /// Next drained key (follows drained document; Rust -> Go).
        DrainedKey = 203,
        /// Next drained fields (follows key; Rust -> Go).
        DrainedFields = 204,
        /// Drain stats, sent after all documents have been drained. (Rust -> Go)
        DrainedStats = 205,
    }
    impl Code {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Code::Invalid => "INVALID",
                Code::Open => "OPEN",
                Code::RestoreCheckpoint => "RESTORE_CHECKPOINT",
                Code::Configure => "CONFIGURE",
                Code::BeginTransaction => "BEGIN_TRANSACTION",
                Code::NextDocumentHeader => "NEXT_DOCUMENT_HEADER",
                Code::NextDocumentBody => "NEXT_DOCUMENT_BODY",
                Code::Trampoline => "TRAMPOLINE",
                Code::TrampolineInvoke => "TRAMPOLINE_INVOKE",
                Code::FlushTransaction => "FLUSH_TRANSACTION",
                Code::FlushedTransaction => "FLUSHED_TRANSACTION",
                Code::PrepareToCommit => "PREPARE_TO_COMMIT",
                Code::ClearRegisters => "CLEAR_REGISTERS",
                Code::DrainChunk => "DRAIN_CHUNK",
                Code::DrainedCombinedDocument => "DRAINED_COMBINED_DOCUMENT",
                Code::DrainedReducedDocument => "DRAINED_REDUCED_DOCUMENT",
                Code::DrainedKey => "DRAINED_KEY",
                Code::DrainedFields => "DRAINED_FIELDS",
                Code::DrainedStats => "DRAINED_STATS",
            }
        }
    }
}
/// BuildAPI is a meta-message which name spaces messages of the Build API
/// bridge.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BuildApi {
}
/// Nested message and enum types in `BuildAPI`.
pub mod build_api {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Config {
        /// Identifier of this build.
        /// The path of the output database is determined by joining the
        /// configured directory and configured build ID.
        #[prost(string, tag="1")]
        pub build_id: ::prost::alloc::string::String,
        /// Path to the directory into which the `node_modules` and `flow_generated`
        /// directories are generated, as well as the built NPM package and
        /// the output database.
        #[prost(string, tag="2")]
        pub directory: ::prost::alloc::string::String,
        /// Root catalog source specification. This may be either a local path
        /// relative to the current working directory, or an absolute URL.
        #[prost(string, tag="3")]
        pub source: ::prost::alloc::string::String,
        /// Content type of the source.
        #[prost(enumeration="super::ContentType", tag="4")]
        pub source_type: i32,
        /// Should the TypeScript package be generated?
        #[prost(bool, tag="5")]
        pub typescript_generate: bool,
        /// Should the TypeScript package be built? Implies generation.
        #[prost(bool, tag="6")]
        pub typescript_compile: bool,
        /// Should the TypeScript package be packaged into the catalog?
        /// Implies generation and compilation.
        #[prost(bool, tag="7")]
        pub typescript_package: bool,
        /// The Docker network the connectors are given access to during catalog
        /// builds.
        #[prost(string, tag="8")]
        pub connector_network: ::prost::alloc::string::String,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Fetch {
        #[prost(string, tag="1")]
        pub resource_url: ::prost::alloc::string::String,
        #[prost(enumeration="super::ContentType", tag="2")]
        pub content_type: i32,
    }
    /// Code labels message codes passed over the CGO bridge.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Code {
        /// Begin a build with a Config (Go -> Rust).
        Begin = 0,
        /// Poll the build after completing one or more trampoline tasks (Go ->
        /// Rust).
        Poll = 1,
        /// Trampoline task start or completion (Rust <-> Go).
        Trampoline = 2,
        /// Trampoline sub-type: Start fetch of a resource.
        TrampolineFetch = 3,
        /// Trampoline sub-type: Start validation of a capture.
        TrampolineValidateCapture = 4,
        /// Trampoline sub-type: Start validation of a materialization.
        TrampolineValidateMaterialization = 5,
        /// Build completed successfully (Rust -> Go).
        Done = 6,
        /// Build completed with errors (Rust -> Go).
        DoneWithErrors = 7,
        /// Generate catalog specification JSON schema (Go <-> Rust)
        CatalogSchema = 100,
    }
    impl Code {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Code::Begin => "BEGIN",
                Code::Poll => "POLL",
                Code::Trampoline => "TRAMPOLINE",
                Code::TrampolineFetch => "TRAMPOLINE_FETCH",
                Code::TrampolineValidateCapture => "TRAMPOLINE_VALIDATE_CAPTURE",
                Code::TrampolineValidateMaterialization => "TRAMPOLINE_VALIDATE_MATERIALIZATION",
                Code::Done => "DONE",
                Code::DoneWithErrors => "DONE_WITH_ERRORS",
                Code::CatalogSchema => "CATALOG_SCHEMA",
            }
        }
    }
}
/// ResetStateRequest is the request of the Testing.ResetState RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResetStateRequest {
}
/// ResetStateResponse is the response of the Testing.ResetState RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResetStateResponse {
}
/// AdvanceTimeRequest is the request of the Testing.AdvanceTime RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AdvanceTimeRequest {
    #[prost(uint64, tag="1")]
    pub advance_seconds: u64,
}
/// AdvanceTimeResponse is the response of the Testing.AdvanceTime RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AdvanceTimeResponse {
}
/// DocsAndBytes represents a count of JSON documents, and their cumulative total
/// size in bytes. This is used by the various Stats messages.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DocsAndBytes {
    #[prost(uint32, tag="1")]
    pub docs: u32,
    #[prost(uint32, tag="2")]
    pub bytes: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestRequest {
    /// Name of the collection into which to ingest.
    #[prost(string, tag="1")]
    pub collection: ::prost::alloc::string::String,
    /// Build ID of the ingested collection.
    #[prost(string, tag="2")]
    pub build_id: ::prost::alloc::string::String,
    /// Newline-separated JSON documents to ingest.
    #[prost(string, tag="3")]
    pub docs_json_lines: ::prost::alloc::string::String,
}
/// IngestResponse is the response of the Testing.Ingest RPC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestResponse {
    /// Journals appended to by this ingestion, and their maximum offset on commit.
    #[prost(map="string, int64", tag="1")]
    pub journal_write_heads: ::std::collections::HashMap<::prost::alloc::string::String, i64>,
    /// Etcd header which describes current journal partitions.
    #[prost(message, optional, tag="2")]
    pub journal_etcd: ::core::option::Option<::proto_gazette::broker::header::Etcd>,
}
/// EndpointType enumerates the endpoint types understood by Flow.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum EndpointType {
    /// Reserved for REMOTE gRPC servers if there's ever a use case.
    Invalid = 0,
    Sqlite = 2,
    Ingest = 3,
    AirbyteSource = 7,
    FlowSink = 8,
}
impl EndpointType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            EndpointType::Invalid => "INVALID",
            EndpointType::Sqlite => "SQLITE",
            EndpointType::Ingest => "INGEST",
            EndpointType::AirbyteSource => "AIRBYTE_SOURCE",
            EndpointType::FlowSink => "FLOW_SINK",
        }
    }
}
/// LogLevel is a common representation of a ops log level, which
/// is shared between Rust and Go code. Variants are ordered, making
/// LogLevel comparable.
/// It uses non-conventional lower-case variants so that its canonical
/// JSON encoding also uses lower-case.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum LogLevel {
    Undefined = 0,
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}
impl LogLevel {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            LogLevel::Undefined => "undefined",
            LogLevel::Error => "error",
            LogLevel::Warn => "warn",
            LogLevel::Info => "info",
            LogLevel::Debug => "debug",
            LogLevel::Trace => "trace",
        }
    }
}
/// ContentType enumerates the content types understood by Flow.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ContentType {
    Catalog = 0,
    JsonSchema = 1,
    TypescriptModule = 2,
    NpmPackage = 3,
    Config = 4,
    DocumentsFixture = 5,
}
impl ContentType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ContentType::Catalog => "CATALOG",
            ContentType::JsonSchema => "JSON_SCHEMA",
            ContentType::TypescriptModule => "TYPESCRIPT_MODULE",
            ContentType::NpmPackage => "NPM_PACKAGE",
            ContentType::Config => "CONFIG",
            ContentType::DocumentsFixture => "DOCUMENTS_FIXTURE",
        }
    }
}
