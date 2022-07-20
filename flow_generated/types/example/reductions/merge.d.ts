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
