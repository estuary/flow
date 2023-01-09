// NOTE constants in this file must be mirrored in
// go/labels/labels.go
// See that file for descriptions of each label.

// JournalSpec & ShardSpec labels.
pub const BUILD: &str = "estuary.dev/build";
pub const COLLECTION: &str = "estuary.dev/collection";
pub const FIELD_PREFIX: &str = "estuary.dev/field/";
pub const KEY_BEGIN: &str = "estuary.dev/key-begin";
pub const KEY_BEGIN_MIN: &str = "00000000";
pub const KEY_END: &str = "estuary.dev/key-end";
pub const KEY_END_MAX: &str = "ffffffff";
pub const MANAGED_BY_FLOW: &str = "estuary.dev/flow";

// ShardSpec labels.
pub const TASK_NAME: &str = "estuary.dev/task-name";
pub const TASK_TYPE: &str = "estuary.dev/task-type";
pub const TASK_TYPE_CAPTURE: &str = "capture";
pub const TASK_TYPE_DERIVATION: &str = "derivation";
pub const TASK_TYPE_MATERIALIZATION: &str = "materialization";
pub const RCLOCK_BEGIN: &str = "estuary.dev/rclock-begin";
pub const RCLOCK_BEGIN_MIN: &str = KEY_BEGIN;
pub const RCLOCK_END: &str = "estuary.dev/rclock-end";
pub const RCLOCK_END_MAX: &str = KEY_END_MAX;
pub const SPLIT_TARGET: &str = "estuary.dev/split-target";
pub const SPLIT_SOURCE: &str = "estuary.dev/split-source";
pub const LOG_LEVEL: &str = "estuary.dev/log-level";
// Shard labels related to network connectivity to shards.
pub const HOSTNAME: &str = "estuary.dev/hostname";
pub const EXPOSE_PORT: &str = "estuary.dev/expose-port";
pub const PORT_PROTO_PREFIX: &str = "estuary.dev/port-proto/";
pub const PORT_PUBLIC_PREFIX: &str = "estuary.dev/port-public/";

// A used subset of Gazette labels, defined in go.gazette.dev/core/labels/labels.go.
pub const CONTENT_TYPE: &str = "content-type";
pub const CONTENT_TYPE_JSON_LINES: &str = "application/x-ndjson";
pub const CONTENT_TYPE_RECOVERY_LOG: &str = "application/x-gazette-recoverylog";

pub const MANAGED_BY: &str = "app.gazette.dev/managed-by";
