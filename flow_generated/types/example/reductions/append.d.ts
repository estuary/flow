// Generated from collection schema examples/reduction-types/append.flow.yaml?ptr=/collections/example~1reductions~1append/schema.
// Referenced from examples/reduction-types/append.flow.yaml#/collections/example~1reductions~1append.
export type Document = {
    key: number | string;
    value?: unknown[];
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
