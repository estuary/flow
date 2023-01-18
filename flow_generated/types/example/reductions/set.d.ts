// Generated from collection schema examples/reduction-types/set.flow.yaml?ptr=/collections/example~1reductions~1set/schema.
// Referenced from examples/reduction-types/set.flow.yaml#/collections/example~1reductions~1set.
export type Document = {
    key: string;
    value?: {
        [k: string]: {
            [k: string]: number;
        };
    };
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
