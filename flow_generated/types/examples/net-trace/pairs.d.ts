// Generated from collection schema examples/net-trace/pairs.flow.yaml?ptr=/collections/examples~1net-trace~1pairs/schema.
// Referenced from examples/net-trace/pairs.flow.yaml#/collections/examples~1net-trace~1pairs.
export type Document = {
    bwd?: {
        bytes?: number;
        packets?: number;
    };
    dst: {
        ip: string;
        port: number;
    };
    fwd?: {
        bytes?: number;
        packets?: number;
    };
    millis?: number;
    protocol: 0 | 6 | 17;
    src: {
        ip: string;
        port: number;
    };
    timestamp: string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
