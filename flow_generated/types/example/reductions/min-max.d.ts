// Generated from collection schema examples/reduction-types/min_max.flow.yaml?ptr=/collections/example~1reductions~1min-max/schema.
// Referenced from examples/reduction-types/min_max.flow.yaml#/collections/example~1reductions~1min-max.
export type Document = {
    key: string;
    max?: unknown;
    min?: unknown;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
