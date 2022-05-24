// Generated from collection schema builtin://flow/ops-log-schema.json.
// Referenced from builtin://flow/ops/generated/collections.
export type Document =
    /* Flow task logs Logs related to the processing of a Flow capture, derivation, or materialization */ {
        fields?: /* Map of keys and values that are associated with this log entry. */ {
            [k: string]: unknown;
        };
        level: 'debug' | 'error' | 'info' | 'warn';
        message: string;
        shard: /* Flow shard id Identifies a specific shard of a task, which may be the source of a log message or metrics */ {
            keyBegin: /* The inclusive beginning of the shard's assigned key range */ string;
            kind: /* The type of the catalog task */ 'capture' | 'derivation' | 'materialization';
            name: /* The name of the catalog task (without the task type prefix) */ string;
            rClockBegin: /* The inclusive beginning of the shard's assigned rClock range */ string;
        };
        ts: /* Timestamp corresponding to the start of the transaction */ string;
    };
