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

// Generated from collection schema examples/soak-tests/set-ops/schema.yaml#/$defs/outputWithReductions.
// Referenced from examples/soak-tests/set-ops/flow.yaml#/collections/soak~1set-ops~1sets.
export type Document = /* Output merges expected and actual values for a given stream */ {
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

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;

// Generated from derivation register schema examples/soak-tests/set-ops/flow.yaml?ptr=/collections/soak~1set-ops~1sets/derivation/register/schema.
// Referenced from examples/soak-tests/set-ops/flow.yaml#/collections/soak~1set-ops~1sets/derivation.
export type Register = unknown;

// Generated from transform onOperation as a re-export of collection soak/set-ops/operations.
// Referenced from examples/soak-tests/set-ops/flow.yaml#/collections/soak~1set-ops~1sets/derivation/transform/onOperation."
import { SourceDocument as OnOperationSource } from './operations';
export { SourceDocument as OnOperationSource } from './operations';

// Generated from derivation examples/soak-tests/set-ops/flow.yaml#/collections/soak~1set-ops~1sets/derivation.
// Required to be implemented by examples/soak-tests/set-ops/sets.ts.
export interface IDerivation {
    onOperationPublish(source: OnOperationSource, register: Register, previous: Register): OutputDocument[];
}
