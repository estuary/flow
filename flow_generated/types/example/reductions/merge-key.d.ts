// Generated from collection schema examples/reduction-types/merge_key.flow.yaml?ptr=/collections/example~1reductions~1merge-key/schema.
// Referenced from examples/reduction-types/merge_key.flow.yaml#/collections/example~1reductions~1merge-key.
export type Document = {
    key: string;
    value?: unknown[];
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
