use super::{CollectionSpec, OAuth2, RawValue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Request is a message written into a materialization connector by the Flow runtime.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum Request {
    /// Spec requests the specification definition of this connector.
    /// Notably this includes its endpoint and resource configuration JSON schema.
    #[serde(rename_all = "camelCase")]
    Spec {},
    /// Validate a connector configuration and proposed bindings.
    #[serde(rename_all = "camelCase")]
    Validate {
        /// # Name of the materialization being validated.
        name: String,
        /// # Connector endpoint configuration.
        config: RawValue,
        /// # Proposed bindings of the validated materialization.
        bindings: Vec<ValidateBinding>,
    },
    /// Apply a connector configuration and binding specifications.
    #[serde(rename_all = "camelCase")]
    Apply {
        /// # Name of the materialization being applied.
        name: String,
        /// # Connector endpoint configuration.
        config: RawValue,
        /// # Binding specifications of the applied materialization.
        bindings: Vec<ApplyBinding>,
        /// # Opaque, unique version of this materialization application.
        version: String,
        /// # Is this application a dry run?
        /// Dry-run applications take no action.
        dry_run: bool,
    },
    /// Open a materialization connector for materialization of documents to the endpoint.
    #[serde(rename_all = "camelCase")]
    Open {
        /// # Name of the materialization being opened.
        name: String,
        /// # Connector endpoint configuration.
        config: RawValue,
        /// # Binding specifications of the opened materialization.
        bindings: Vec<ApplyBinding>,
        /// # Opaque, unique version of this materialization.
        version: String,
        /// # Beginning key-range which this connector invocation will materialize.
        /// [keyBegin, keyEnd] are the inclusive range of keys processed by this
        /// connector invocation. Ranges reflect the disjoint chunks of ownership
        /// specific to each instance of a scale-out materialization.
        ///
        /// The Flow runtime manages the routing of document keys to distinct
        /// connector invocations, and each invocation will receive only disjoint
        /// subsets of possible keys. Thus, this key range is merely advisory.
        key_begin: u32,
        /// # Ending key-range which this connector invocation will materialize.
        key_end: u32,
        /// # Last-persisted driver checkpoint from a previous materialization invocation.
        /// Or empty, if the driver has cleared or never set its checkpoint.
        driver_checkpoint: RawValue,
    },
    /// Acknowledge to the connector that the previous transaction has committed
    /// to the Flow runtime's recovery log.
    #[serde(rename_all = "camelCase")]
    Acknowledge {},
    /// Load a document identified by its key. The given key may have never before been stored,
    /// but a given key will be sent in a transaction Load just one time.
    #[serde(rename_all = "camelCase")]
    Load {
        /// # Index of the Open binding for which this document is loaded.
        binding: u32,
        /// # Packed hexadecimal encoding of the key to load.
        /// The packed encoding is order-preserving: given two instances of a
        /// composite key K1 and K2, if K2 > K1 then K2's packed key is lexicographically
        /// greater than K1's packed key.
        key_packed: String,
        /// # Composite key to load.
        key: Vec<Value>,
    },
    /// Flush loads. No further Loads will be sent in this transaction,
    /// and the runtime will await the connector's remaining Loaded responses
    /// followed by one Flushed response.
    #[serde(rename_all = "camelCase")]
    Flush {},
    /// Store documents updated by the current transaction.
    #[serde(rename_all = "camelCase")]
    Store {
        /// # Index of the Open binding for which this document is stored.
        binding: u32,
        /// # Packed hexadecimal encoding of the key to store.
        key_packed: String,
        /// # Composite key to store.
        key: Vec<Value>,
        /// # Array of selected, projected document values to store.
        values: Vec<Value>,
        /// # Complete JSON document to store.
        doc: Box<RawValue>,
        /// # Does this key exist in the endpoint?
        /// True if this document was previously loaded or stored.
        /// A SQL materialization might toggle between INSERT vs UPDATE behavior
        /// depending on this value.
        exists: bool,
    },
    /// Mark the end of the Store phase, and if the remote store is authoritative,
    /// instruct it to start committing its transaction.
    #[serde(rename_all = "camelCase")]
    StartCommit {
        /// # Opaque, base64-encoded Flow runtime checkpoint.
        /// If the endpoint is authoritative, the connector must store this checkpoint
        /// for a retrieval upon a future Open.
        runtime_checkpoint: String,
    },
}

/// A proposed binding of the materialization to validate.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ValidateBinding {
    /// # Collection of the proposed binding.
    pub collection: CollectionSpec,
    /// # Resource configuration of the proposed binding.
    pub resource_config: Box<RawValue>,
    /// # Field configuration of the proposed binding.
    pub field_config: BTreeMap<String, Box<RawValue>>,
}

/// A binding specification of the materialization.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApplyBinding {
    /// # Collection of this binding.
    pub collection: CollectionSpec,
    /// # Resource configuration of this binding.
    pub resource_config: Box<RawValue>,
    /// # Resource path which fully qualifies the endpoint resource identified by this binding.
    /// For an RDBMS, this might be ["my-schema", "my-table"].
    /// For Kafka, this might be ["my-topic-name"].
    /// For Redis or DynamoDB, this might be ["/my/key/prefix"].
    pub resource_path: Vec<String>,
    /// # Does this binding use delta-updates instead of standard materialization?
    pub delta_updates: bool,
    /// # Fields which have been selected for materialization.
    pub field_selection: FieldSelection,
}

/// Field selection describes the projected keys, values, and (optionally)
/// the document to materialize, as well as any custom field configuration.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FieldSelection {
    /// # Selected fields which are collection key components.
    pub keys: Vec<String>,
    /// # Selected fields which are values.
    pub values: Vec<String>,
    /// # Field which represents the Flow document, or null if the document isn't materialized.
    pub document: Option<String>,
    /// # Custom field configuration of the binding, keyed by field.
    pub field_config: BTreeMap<String, Box<RawValue>>,
}

/// Response is a message written by a materialization connector to the Flow runtime.
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
        /// # Flow runtime checkpoint to begin processing from. Optional.
        /// If empty, the most recent checkpoint of the Flow recovery log is used.
        ///
        /// Or, a driver may send the value []byte{0xf8, 0xff, 0xff, 0xff, 0xf, 0x1}
        /// to explicitly begin processing from a zero-valued checkpoint, effectively
        /// rebuilding the materialization from scratch.
        #[serde(default)]
        runtime_checkpoint: String,
    },
    /// Loaded responds to a Request "load" command.
    #[serde(rename_all = "camelCase")]
    Loaded {
        /// # Index of the Open binding for which this document is loaded.
        binding: u32,
        /// # Loaded document.
        doc: Box<RawValue>,
    },
    /// Loaded responds to a Request "flush" command.
    #[serde(rename_all = "camelCase")]
    Flushed {},
    /// StartedCommit responds to a Request "startCommit" command.
    #[serde(rename_all = "camelCase")]
    StartedCommit {
        /// # Updated driver checkpoint to commit with this checkpoint.
        driver_checkpoint: Box<RawValue>,
        /// # Is this a partial update of the driver's checkpoint?
        /// If true, then treat the driver checkpoint as a partial state update
        /// which is incorporated into the full checkpoint as a RFC7396 Merge patch.
        /// Otherwise the checkpoint is completely replaced.
        merge_patch: bool,
    },
    /// Acknowledged follows Open and also StartedCommit, and tells the Flow Runtime
    /// that a previously started commit has completed.
    ///
    /// Acknowledged is _not_ a direct response to Request "acknowledge" command,
    /// and Acknowledge vs Acknowledged may be written in either order.
    #[serde(rename_all = "camelCase")]
    Acknowledged {},
}

/// A validated binding.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ValidatedBinding {
    /// # Resource path which fully qualifies the endpoint resource identified by this binding.
    pub resource_path: Vec<String>,
    /// # Mapping of fields to their connector-imposed constraints.
    /// The Flow runtime resolves a final set of fields from the user's specification
    /// and the set of constraints returned by the connector.
    pub constraints: BTreeMap<String, Constraint>,
    /// # Should delta-updates be used for this binding?
    pub delta_updates: bool,
}

/// A Constraint constrains the use of a collection projection within a materialization binding.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Constraint {
    /// # The type of this constraint.
    pub r#type: ConstraintType,
    /// # A user-facing reason for the constraint on this field.
    pub reason: String,
}

/// The type of a field constraint.
#[derive(Deserialize, Serialize, JsonSchema)]
pub enum ConstraintType {
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
