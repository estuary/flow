// Generated from $anchor schema examples/soak-tests/set-ops/schema.yaml#Output."
export type Output = /* Output merges expected and actual values for a given stream */ {
    appliedAdd?: number;
    appliedOps?: number;
    appliedRemove?: number;
    author: number;
    derived?: {
        [k: string]: {
            [k: string]: number;
        };
    };
    expectValues?: {
        [k: string]: number;
    };
    id: number;
    timestamp?: string;
};

// Generated from collection schema examples/soak-tests/set-ops/schema.yaml#/$defs/operation.
// Referenced from examples/soak-tests/set-ops/flow.yaml#/collections/soak~1set-ops~1operations.
export type Document = /* Mutates a set and provides the values that are expected after this operation is applied */ {
    author: number;
    expectValues: /* Final values that are expected after this operation has been applied */ {
        [k: string]: number;
    };
    id: number;
    ones: number;
    op: number;
    timestamp: string;
    type: 'add' | 'remove';
    values: {
        [k: string]: number;
    };
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;
