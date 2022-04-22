// Generated from collection schema examples/reduction-types/set_array.flow.yaml?ptr=/collections/example~1reductions~1set-array/schema.
// Referenced from examples/reduction-types/set_array.flow.yaml#/collections/example~1reductions~1set-array.
export type Document = {
    key: string;
    value?: {
        [k: string]: [string?, number?][];
    };
};
