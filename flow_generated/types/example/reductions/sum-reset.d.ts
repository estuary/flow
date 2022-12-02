// Generated from collection schema examples/reduction-types/reset_counter.flow.yaml?ptr=/collections/example~1reductions~1sum-reset/schema.
// Referenced from examples/reduction-types/reset_counter.flow.yaml#/collections/example~1reductions~1sum-reset.
export type Document = {
    action?: 'reset' | 'sum';
    key: string;
    value?: number;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
