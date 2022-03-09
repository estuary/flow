use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

//TODO: bring back validations and unmarshal logic!
//      Using Enums instead of strings.

#[derive(Debug, Serialize, Deserialize, strum_macros::Display, Clone)]
#[strum(serialize_all = "snake_case")]
pub enum SyncMode {
    Incremental,
    FullRefresh,
}

//var AllSyncModes = []SyncMode{SyncModeIncremental, SyncModeFullRefresh}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Stream {
    pub name: String,
    pub json_schema: Box<RawValue>,
    pub supported_sync_modes: Vec<String>, //Vec<SyncMode>,
    pub source_defined_cursor: Option<bool>,
    pub default_cursor_field: Option<Vec<String>>,
    pub source_defined_primary_key: Option<Vec<Vec<String>>>,
    pub namespace: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, strum_macros::Display, Clone)]
#[strum(serialize_all = "snake_case")]
pub enum DestinationSyncMode {
    Append,
    Overwrite,
    AppendDedup,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct ConfiguredStream {
    pub stream: Stream,
    pub sync_mode: String,             //SyncMode,
    pub destination_sync_mode: String, //DestinationSyncMode,
    pub cursor_field: Option<Vec<String>>,
    pub primary_key: Option<Vec<Vec<String>>>,

    #[serde(rename = "estuary.dev/projections")]
    pub projections: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Catalog {
    #[serde(rename = "streams")]
    streams: Vec<Stream>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Range {
    pub begin: String, //u32,
    pub end: String,   //u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConfiguredCatalog {
    #[serde(rename = "streams")]
    pub streams: Vec<ConfiguredStream>,

    #[serde(rename = "estuary.dev/tail")]
    pub tail: bool,

    #[serde(rename = "estuary.dev/range")]
    pub range: Range,
}

#[derive(Debug, Serialize, Deserialize, strum_macros::Display, Clone)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum Status {
    Succeeded,
    Failed,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct ConnectionStatus {
    pub status: String, //Status,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Record {
    pub stream: String,
    pub data: Box<RawValue>,
    pub emitted_at: Option<i64>,
    pub namespace: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, strum_macros::Display, Clone)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    level: String, //LogLevel,
    message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct State {
    // Data is the actual state associated with the ingestion. This must be a JSON _Object_ in order
    // to comply with the airbyte specification.
    #[serde(rename = "data")]
    pub data: Box<RawValue>,

    // TODO: check the logic on merging of both merge fields.
    #[serde(rename = "estuary.dev/merge")]
    pub ns_merge: Option<bool>,

    // Merge indicates that Data is an RFC 7396 JSON Merge Patch, and should
    // be be reduced into the previous state accordingly.
    #[serde(rename = "merge")]
    pub merge: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Spec {
    pub documentation_url: Option<String>,
    pub changelog_url: Option<String>,
    pub connection_specification: Box<RawValue>,
    pub supports_incremental: bool,

    // SupportedDestinationSyncModes is ignored by Flow
    //SupportedDestinationSyncModes []DestinationSyncMode `json:"supported_destination_sync_modes,omitempty"`
    // SupportsNormalization is not currently used or supported by Flow or estuary-developed
    // connectors
    //SupportsNormalization bool `json:"supportsNormalization,omitempty"`
    // SupportsDBT is not currently used or supported by Flow or estuary-developed
    // connectors
    //SupportsDBT bool `json:"supportsDBT,omitempty"`

    // AuthSpecification is not currently used or supported by Flow or estuary-developed
    // connectors, and it is deprecated in the airbyte spec.
    pub auth_specification: Option<Box<RawValue>>,
    // AdvancedAuth is not currently used or supported by Flow or estuary-developed
    // connectors.
    pub advanced_auth: Option<Box<RawValue>>,
}

#[derive(Debug, Serialize, Deserialize, strum_macros::Display, Clone)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageType {
    Record,
    State,
    Log,
    #[strum(serialize = "SPEC")]
    Spec,
    ConnectionStatus,
    Catalog,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    #[serde(rename = "type")]
    pub message_type: String, //MessageType,

    pub log: Option<Log>,
    pub state: Option<State>,
    pub record: Option<Record>,
    pub connection_status: Option<ConnectionStatus>,
    pub spec: Option<Spec>,
    pub catalog: Option<Catalog>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
#[serde(rename_all = "camelCase")]
// ResourceSpec is the configuration for Airbyte source streams.
pub struct ResourceSpec {
    pub stream: String,
    pub namespace: Option<String>,
    pub sync_mode: String, //SyncMode,
}
