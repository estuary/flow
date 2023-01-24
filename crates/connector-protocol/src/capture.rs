use super::{CollectionSpec, OAuth2, RawValue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Request is a message written into a capture connector by the Flow runtime.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum Request {
    /// Spec requests the specification definition of this connector.
    /// Notably this includes its endpoint and resource configuration JSON schema.
    #[serde(rename_all = "camelCase")]
    Spec {},
    /// Discover returns the set of resources available from this Driver.
    #[serde(rename_all = "camelCase")]
    Discover {
        /// # Connector endpoint configuration.
        config: RawValue,
    },
    /// Validate a connector configuration and proposed bindings.
    #[serde(rename_all = "camelCase")]
    Validate {
        /// # Name of the capture being validated.
        name: String,
        /// # Connector endpoint configuration.
        config: RawValue,
        /// # Proposed bindings of the validated capture.
        bindings: Vec<ValidateBinding>,
    },
    /// Apply a connector configuration and binding specifications.
    #[serde(rename_all = "camelCase")]
    Apply {
        /// # Name of the capture being applied.
        name: String,
        /// # Connector endpoint configuration.
        config: RawValue,
        /// # Binding specifications of the applied capture.
        bindings: Vec<ApplyBinding>,
        /// # Opaque, unique version of this capture application.
        version: String,
        /// # Is this application a dry run?
        /// Dry-run applications take no action.
        dry_run: bool,
    },
    /// Open a capture connector for reading documents from the endpoint.
    /// Unless the connector requests explicit acknowledgements, Open is the
    /// last message which will be sent to the connector's stdin.
    #[serde(rename_all = "camelCase")]
    Open {
        /// # Name of the capture being opened.
        name: String,
        /// # Connector endpoint configuration.
        config: RawValue,
        /// # Binding specifications of the opened capture.
        bindings: Vec<ApplyBinding>,
        /// # Opaque, unique version of this capture.
        version: String,
        /// # Beginning key-range which this connector invocation must capture.
        /// [keyBegin, keyEnd] are the inclusive range of keys processed by this
        /// connector invocation. Ranges reflect the disjoint chunks of ownership
        /// specific to each instance of a scale-out capture.
        ///
        /// The meaning of a "key" in this range is up to the connector.
        /// For example, captures of a partitioned system (like Kafka or Kinesis)
        /// might hash dynamically listed partitions into a 32-bit unsigned integer,
        /// and then capture only those which fall within its opened key range.
        key_begin: u32,
        /// # Ending key-range which this connector invocation must capture.
        key_end: u32,
        /// # Last-persisted driver checkpoint from a previous capture invocation.
        /// Or empty, if the driver has cleared or never set its checkpoint.
        /// Each key-range of the capture has its own durable checkpoint,
        /// which is managed by the Flow runtime.
        driver_checkpoint: RawValue,
        /// # Frequency of connector restarts.
        /// Restart intervals are applicable only for captures which poll ready
        /// documents from their endpoint and then exit. Unbounded, streaming
        /// connectors are restarted only when necessary.
        interval_seconds: u32,
    },
    /// Acknowledge to the connector that its checkpoint has committed to the Flow runtime recovery log.
    /// Acknowledgments are sent only if requested by the connector in its Opened response,
    /// and one Acknowledge is sent for each preceding Checkpoint response of the connector.
    #[serde(rename_all = "camelCase")]
    Acknowledge {},
}

/// A proposed binding of the capture to validate.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ValidateBinding {
    /// # Collection of the proposed binding.
    pub collection: CollectionSpec,
    /// # Resource configuration of the proposed binding.
    pub resource_config: RawValue,
}

/// A binding specification of the capture.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApplyBinding {
    /// # Collection of this binding.
    pub collection: CollectionSpec,
    /// # Resource configuration of this binding.
    pub resource_config: RawValue,
    /// # Resource path which fully qualifies the endpoint resource identified by this binding.
    /// For an RDBMS, this might be ["my-schema", "my-table"].
    /// For Kafka, this might be ["my-topic-name"].
    /// For Redis or DynamoDB, this might be ["/my/key/prefix"].
    pub resource_path: Vec<String>,
}

/// Response is a message written by a capture connector to the Flow runtime.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum Response {
    /// Spec responds to a Request "spec" command.
    #[serde(rename_all = "camelCase")]
    Spec {
        /// # URL for connector's documentation.
        documentation_url: String,
        /// # JSON schema of the connector's endpoint configuration.
        config_schema: RawValue,
        /// # JSON schema of a binding's resource specification.
        resource_config_schema: RawValue,
        /// # Optional OAuth2 configuration.
        #[serde(default)]
        oauth2: Option<OAuth2>,
    },
    /// Discovered responds to a Request "discover" command.
    #[serde(rename_all = "camelCase")]
    Discovered {
        /// # Discovered bindings of the endpoint.
        bindings: Vec<DiscoveredBinding>,
    },
    /// Validated responds to a Request "validate" command.
    #[serde(rename_all = "camelCase")]
    Validated {
        /// # Validated bindings of the endpoint.
        bindings: Vec<ValidatedBinding>,
    },
    /// Applied responds to a Request "apply" command.
    #[serde(rename_all = "camelCase")]
    Applied {
        /// # User-facing description of the action taken by this application.
        /// If the apply was a dry-run, then this is a description of actions
        /// that would have been taken.
        action_description: String,
    },
    /// Opened responds to a Request "open" command.
    #[serde(rename_all = "camelCase")]
    Opened {
        /// # Should the runtime explicitly respond to the connector's Checkpoints?
        explicit_acknowledgements: bool,
    },
    /// Document captured by this connector invocation.
    /// Emitted documents are pending, and are not committed to their bound collection
    /// until a following Checkpoint is emitted.
    #[serde(rename_all = "camelCase")]
    Document {
        /// # Index of the Open binding for which this document is captured.
        binding: u32,
        /// # Document value.
        doc: RawValue,
    },
    /// Checkpoint all preceding documents of this invocation since the last checkpoint.
    /// The Flow runtime may begin to commit documents in a transaction.
    /// Note that the runtime may include more than one checkpoint in a single transaction.
    #[serde(rename_all = "camelCase")]
    Checkpoint {
        /// # Updated driver checkpoint to commit with this checkpoint.
        driver_checkpoint: RawValue,
        /// # Is this a partial update of the driver's checkpoint?
        /// If true, then treat the driver checkpoint as a partial state update
        /// which is incorporated into the full checkpoint as a RFC7396 Merge patch.
        /// Otherwise the checkpoint is completely replaced.
        merge_patch: bool,
    },
}

/// A discovered endpoint resource which may be bound to a Flow Collection.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiscoveredBinding {
    /// # Recommended partial name for this binding's collection.
    /// For example, a SQL database capture might use the table's name.
    pub recommended_name: String,
    /// # Resource configuration for this binding.
    pub resource_config: RawValue,
    /// # JSON Schema for documents captured via the binding.
    pub document_schema: RawValue,
    /// # Composite key of documents captured via the binding.
    /// Keys are specified as an ordered sequence of JSON-Pointers.
    pub key: Vec<String>,
}

/// A validated binding.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ValidatedBinding {
    /// # Resource path which fully qualifies the endpoint resource identified by this binding.
    pub resource_path: Vec<String>,
}
