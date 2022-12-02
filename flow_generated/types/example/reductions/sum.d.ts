// Generated from collection schema examples/reduction-types/sum.flow.yaml?ptr=/collections/example~1reductions~1sum/schema.
// Referenced from examples/reduction-types/sum.flow.yaml#/collections/example~1reductions~1sum.
export type Document = {
    key: string;
    value?: number;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
