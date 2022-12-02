// Generated from collection schema examples/reduction-types/fww_lww.flow.yaml?ptr=/collections/example~1reductions~1fww-lww/schema.
// Referenced from examples/reduction-types/fww_lww.flow.yaml#/collections/example~1reductions~1fww-lww.
export type Document = {
    fww?: unknown;
    key: string;
    lww?: unknown;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
