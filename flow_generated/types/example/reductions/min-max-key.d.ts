// Generated from collection schema examples/reduction-types/min_max_key.flow.yaml?ptr=/collections/example~1reductions~1min-max-key/schema.
// Referenced from examples/reduction-types/min_max_key.flow.yaml#/collections/example~1reductions~1min-max-key.
export type Document = {
    key: string;
    max?: [string?, number?];
    min?: [string?, number?];
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
