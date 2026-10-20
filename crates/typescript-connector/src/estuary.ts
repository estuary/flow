// Platform types of every connector protocol, written into every TypeScript
// project by Estuary. A connector's protocol messages import them.
//
// They're the protocol's JSON messages. A field which the protocol omits when
// empty or zero is optional. Messages aren't validated at runtime.

// Inference of a projected location, from the collection's schema.
export type Inference = {
    types?: string[];
    string?: {
        contentType?: string;
        format?: string;
        contentEncoding?: string;
        maxLength?: number;
        strMinimum?: string;
        strMaximum?: string;
    };
    title?: string;
    description?: string;
    default?: unknown;
    secret?: boolean;
    exists?: "INVALID" | "MUST" | "MAY" | "IMPLICIT" | "CANNOT";
    numeric?: {
        hasMinimum?: boolean;
        minimum?: number;
        hasMaximum?: boolean;
        maximum?: number;
    };
    array?: {
        minItems?: number;
        hasMaxItems?: boolean;
        maxItems?: number;
        itemTypes?: string[];
    };
    enum?: unknown[];
    reduce?:
        | "REDUCE_UNSET"
        | "REDUCE_MULTIPLE"
        | "REDUCE_APPEND"
        | "REDUCE_FIRST_WRITE_WINS"
        | "REDUCE_LAST_WRITE_WINS"
        | "REDUCE_MAXIMIZE"
        | "REDUCE_MERGE"
        | "REDUCE_MINIMIZE"
        | "REDUCE_SET"
        | "REDUCE_SUM"
        | "REDUCE_JSON_SCHEMA_MERGE";
    redact?: "REDACT_UNSET" | "REDACT_MULTIPLE" | "REDACT_BLOCK" | "REDACT_SHA256";
    contentMediaType?: string;
};

// A document location, projected as a named field of a collection.
export type Projection = {
    ptr?: string;
    field: string;
    explicit?: boolean;
    isPartitionKey?: boolean;
    isPrimaryKey?: boolean;
    inference?: Inference;
};

export type CollectionSpec = {
    name: string;
    key: string[];
    writeSchema: unknown;
    readSchema?: unknown;
    partitionFields?: string[];
    projections?: Projection[];
};

export type RangeSpec = {
    keyBegin?: number;
    keyEnd?: number;
    rClockBegin?: number;
    rClockEnd?: number;
};

export type UUIDParts = {
    node: string;
    clock: string;
};

export type ConnectorStateUpdate<S> = {
    // An updated state, or a patch thereof.
    updated: S;
    // Is `updated` an RFC 7396 JSON merge patch, rather than a full replacement?
    mergePatch?: boolean;
};
