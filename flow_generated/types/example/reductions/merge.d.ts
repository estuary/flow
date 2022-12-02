// Generated from collection schema examples/reduction-types/merge.flow.yaml?ptr=/collections/example~1reductions~1merge/schema.
// Referenced from examples/reduction-types/merge.flow.yaml#/collections/example~1reductions~1merge.
export type Document = {
    key: string;
    value?:
        | {
              [k: string]: number;
          }
        | number[];
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
