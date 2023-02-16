// Generated from collection schema builtin://flow/ops-catalog/ops-log-schema.json.
// Referenced from builtin://flow/ops/generated/collections.
export type Document =
    /* Flow task logs Logs related to the processing of a Flow capture, derivation, or materialization */ {
        fields?: /* Map of keys and values that are associated with this log entry. */ {
            [k: string]: unknown;
        };
        level: 'debug' | 'error' | 'info' | 'trace' | 'warn';
        message: string;
        shard: /* Flow shard id Identifies a specific shard of a task, which may be the source of a log message or metrics */ {
            keyBegin: /* The inclusive beginning of the shard's assigned key range */ string;
            kind: /* The type of the catalog task */ 'capture' | 'derivation' | 'materialization';
            name: /* The name of the catalog task (without the task type prefix) */ string;
            rClockBegin: /* The inclusive beginning of the shard's assigned rClock range */ string;
        };
        ts: /* Timestamp corresponding to the start of the transaction */ string;
    };

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
