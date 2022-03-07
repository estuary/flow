use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{value::RawValue, Result};

//TODO: bring back validations and unmarshal logic!!!!!!!!!!

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
    name: String,
    json_schema: Box<RawValue>,
    supported_sync_modes: Vec<SyncMode>,
    source_defined_cursor: bool,
    default_cursor_field: Vec<String>,
    source_defined_primary_key: Vec<Vec<String>>,
    namespace: String,
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
    stream: Stream,
    sync_mode: SyncMode,
    destination_sync_mode: DestinationSyncMode,
    cursor_field: Vec<String>,
    primary_key: Vec<Vec<String>>,

    #[serde(rename = "estuary.dev/projections")]
    projections: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Catalog {
    #[serde(rename = "streams")]
    streams: Vec<Stream>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Range {
    begin: u32,
    end: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConfiguredCatalog {
    #[serde(rename = "streams")]
    streams: Vec<ConfiguredStream>,

    #[serde(rename = "estuary.dev/tail")]
    tail: bool,

    #[serde(rename = "estuary.dev/range")]
    range: Range,
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
    status: Status,
    message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Record {
    stream: String,
    data: Box<RawValue>,
    emitted_at: i64,
    namespace: String,
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
    level: LogLevel,
    message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct State {
    // Data is the actual state associated with the ingestion. This must be a JSON _Object_ in order
    // to comply with the airbyte specification.
    #[serde(rename = "data")]
    data: Box<RawValue>,

    // Merge indicates that Data is an RFC 7396 JSON Merge Patch, and should
    // be be reduced into the previous state accordingly.
    #[serde(rename = "estuary.dev/merge")]
    merge: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Spec {
    documentation_url: String,
    changelog_url: String,
    connection_specification: Box<RawValue>,
    supports_incremental: bool,

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
    auth_specification: Box<RawValue>,
    // AdvancedAuth is not currently used or supported by Flow or estuary-developed
    // connectors.
    advanced_auth: Box<RawValue>,
}

#[derive(Debug, Serialize, Deserialize, strum_macros::Display, Clone)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageType {
    Record,
    State,
    Log,
    Spec,
    ConnectionStatus,
    Catalog,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    #[serde(rename = "type")]
    pub message_type: MessageType,

    pub log: Option<Log>,
    pub state: Option<State>,
    pub record: Option<Record>,
    pub connection_status: Option<ConnectionStatus>,
    pub spec: Option<Spec>,
    pub catalog: Option<Catalog>,
}
