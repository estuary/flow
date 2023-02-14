// Generated from collection schema builtin://flow/ops-stats-schema.json.
// Referenced from builtin://flow/ops/generated/collections.
export type Document =
    /* Flow task stats Statistics related to the processing of a Flow capture, derivation, or materialization */ {
        capture?: /* Capture stats, organized by collection. The keys of this object are the collection names, and the values are the stats for that collection. */ {
            [k: string]: {
                out?: {
                    bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                    docsTotal: /* Total number of documents */ number;
                };
                right?: /* Documents fed into the combiner from the source */ {
                    bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                    docsTotal: /* Total number of documents */ number;
                };
            };
        };
        derive?: {
            out: {
                bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                docsTotal: /* Total number of documents */ number;
            };
            registers?: {
                createdTotal: /* The total number of new register keys that were created */ number;
            };
            transforms: /* A map of each transform (transform name, not collection name) to stats for that transform */ {
                [
                    k: string
                ]: /* Stats for a specific transform of a derivation, which will have an update, publish, or both. */ {
                    input: /* The input documents that were fed into this transform. */ {
                        bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                        docsTotal: /* Total number of documents */ number;
                    };
                    publish?: /* The outputs from publish lambda invocations. */ {
                        out: {
                            bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                            docsTotal: /* Total number of documents */ number;
                        };
                        secondsTotal: number;
                    };
                    update?: /* The outputs from update lambda invocations, which were combined into registers. */ {
                        out: {
                            bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                            docsTotal: /* Total number of documents */ number;
                        };
                        secondsTotal: number;
                    };
                };
            };
        };
        materialize?: /* A map of each binding source (collection name) to combiner stats for that binding */ {
            [k: string]: {
                left?: {
                    bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                    docsTotal: /* Total number of documents */ number;
                };
                out?: {
                    bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                    docsTotal: /* Total number of documents */ number;
                };
                right?: {
                    bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                    docsTotal: /* Total number of documents */ number;
                };
            };
        };
        openSecondsTotal: /* Total time that the transaction was open before starting to commit */ number;
        shard: /* Flow shard id Identifies a specific shard of a task, which may be the source of a log message or metrics */ {
            keyBegin: /* The inclusive beginning of the shard's assigned key range */ string;
            kind: /* The type of the catalog task */ 'capture' | 'derivation' | 'materialization';
            name: /* The name of the catalog task (without the task type prefix) */ string;
            rClockBegin: /* The inclusive beginning of the shard's assigned rClock range */ string;
        };
        ts: /* Timestamp corresponding to the start of the transaction */ string;
        txnCount: /* Total number of transactions represented by this stats document */ number;
    };

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
