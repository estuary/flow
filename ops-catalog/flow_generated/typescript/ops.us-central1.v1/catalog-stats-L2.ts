
// Generated for published documents of derived collection ops.us-central1.v1/catalog-stats-L2.
export type Document = /* Flow catalog task stats Statistics related to the processing of a Flow catalog. */ {
    catalogName: /* Name of the Flow catalog */ string;
    grain: /* Time grain that the stats are aggregated over */ "daily" | "hourly" | "monthly";
    statsSummary: {
        errors?: /* Total number of logged errors */ number;
        failures?: /* Total number of shard failures */ number;
        readByMe?: {
            bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
            docsTotal: /* Total number of documents */ number;
        };
        readFromMe?: {
            bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
            docsTotal: /* Total number of documents */ number;
        };
        usageSeconds?: /* Cumulative number of metered seconds of task usage */ number;
        warnings?: /* Total number of logged warnings */ number;
        writtenByMe?: {
            bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
            docsTotal: /* Total number of documents */ number;
        };
        writtenToMe?: {
            bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
            docsTotal: /* Total number of documents */ number;
        };
    };
    taskStats?: {
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
            out?: {
                bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                docsTotal: /* Total number of documents */ number;
            };
            published?: {
                bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                docsTotal: /* Total number of documents */ number;
            };
            transforms?: /* A map of each transform (transform name, not collection name) to stats for that transform */ {
                [k: string]: /* Stats for a specific transform of a derivation, which will have an update, publish, or both. */ {
                    input: /* The input documents that were fed into this transform. */ {
                        bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                        docsTotal: /* Total number of documents */ number;
                    };
                    source?: /* The name of the collection that this transform sources from */ string;
                };
            };
        };
        interval?: {
            uptimeSeconds: /* Number of seconds that the task shard is metered as having been running */ number;
            usageRate?: /* Usage rate which adjusts `uptimeSeconds` to determine the task's effective usage */ number;
        };
        materialize?: /* A map of each binding source (collection name) to combiner stats for that binding */ {
            [k: string]: {
                left?: {
                    bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                    docsTotal: /* Total number of documents */ number;
                };
                out?: {
                    bytesTotal?: /* Total number of bytes representing the JSON encoded documents */ number;
                    docsTotal: /* Total number of documents */ number;
                };
                right?: {
                    bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                    docsTotal: /* Total number of documents */ number;
                };
            };
        };
    };
    ts: /* Timestamp of the catalog stat aggregate */ string;
};


// Generated for read documents of sourced collection ops/rollups/L1/BASE_NAME/catalog-stats.
export type SourceFromBaseName = /* Flow catalog task stats Statistics related to the processing of a Flow catalog. */ {
    catalogName: /* Name of the Flow catalog */ string;
    grain: /* Time grain that the stats are aggregated over */ "daily" | "hourly" | "monthly";
    statsSummary: {
        errors?: /* Total number of logged errors */ number;
        failures?: /* Total number of shard failures */ number;
        readByMe?: {
            bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
            docsTotal: /* Total number of documents */ number;
        };
        readFromMe?: {
            bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
            docsTotal: /* Total number of documents */ number;
        };
        usageSeconds?: /* Cumulative number of metered seconds of task usage */ number;
        warnings?: /* Total number of logged warnings */ number;
        writtenByMe?: {
            bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
            docsTotal: /* Total number of documents */ number;
        };
        writtenToMe?: {
            bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
            docsTotal: /* Total number of documents */ number;
        };
    };
    taskStats?: {
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
            out?: {
                bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                docsTotal: /* Total number of documents */ number;
            };
            published?: {
                bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                docsTotal: /* Total number of documents */ number;
            };
            transforms?: /* A map of each transform (transform name, not collection name) to stats for that transform */ {
                [k: string]: /* Stats for a specific transform of a derivation, which will have an update, publish, or both. */ {
                    input: /* The input documents that were fed into this transform. */ {
                        bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                        docsTotal: /* Total number of documents */ number;
                    };
                    source?: /* The name of the collection that this transform sources from */ string;
                };
            };
        };
        interval?: {
            uptimeSeconds: /* Number of seconds that the task shard is metered as having been running */ number;
            usageRate?: /* Usage rate which adjusts `uptimeSeconds` to determine the task's effective usage */ number;
        };
        materialize?: /* A map of each binding source (collection name) to combiner stats for that binding */ {
            [k: string]: {
                left?: {
                    bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                    docsTotal: /* Total number of documents */ number;
                };
                out?: {
                    bytesTotal?: /* Total number of bytes representing the JSON encoded documents */ number;
                    docsTotal: /* Total number of documents */ number;
                };
                right?: {
                    bytesTotal: /* Total number of bytes representing the JSON encoded documents */ number;
                    docsTotal: /* Total number of documents */ number;
                };
            };
        };
    };
    ts: /* Timestamp of the catalog stat aggregate */ string;
};


export abstract class IDerivation {
    // Construct a new Derivation instance from a Request.Open message.
    constructor(_open: { state: unknown }) { }

    // flush awaits any remaining documents to be published and returns them.
    // deno-lint-ignore require-await
    async flush(): Promise<Document[]> {
        return [];
    }

    // reset is called only when running catalog tests, and must reset any internal state.
    async reset() { }

    // startCommit is notified of a runtime commit in progress, and returns an optional
    // connector state update to be committed.
    startCommit(_startCommit: { runtimeCheckpoint: unknown }): { state?: { updated: unknown, mergePatch: boolean } } {
        return {};
    }

    abstract fromBaseName(read: { doc: SourceFromBaseName }): Document[];
}
