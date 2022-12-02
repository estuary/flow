// Generated from $anchor schema examples/derive-patterns/schema.yaml#Int."
export type Int = /* A document that holds an integer */ {
    Int: number;
    Key: string;
};

// Generated from $anchor schema examples/derive-patterns/schema.yaml#Join."
export type Join = /* Document for join examples */ {
    Key?: string;
    LHS?: number;
    RHS?: string[];
};

// Generated from $anchor schema examples/derive-patterns/schema.yaml#String."
export type String = /* A document that holds a string */ {
    Key: string;
    String: string;
};

// Generated from collection schema examples/derive-patterns/join-one-sided.flow.yaml?ptr=/collections/patterns~1one-sided-join/schema.
// Referenced from examples/derive-patterns/join-one-sided.flow.yaml#/collections/patterns~1one-sided-join.
export type Document = /* Document for join examples */ {
    Key: string;
    LHS?: number;
    RHS?: string[];
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;

// Generated from derivation register schema examples/derive-patterns/schema.yaml#Join.
// Referenced from examples/derive-patterns/join-one-sided.flow.yaml#/collections/patterns~1one-sided-join/derivation.
export type Register = /* Document for join examples */ {
    Key?: string;
    LHS?: number;
    RHS?: string[];
};

// Generated from transform publishLHS as a re-export of collection patterns/ints.
// Referenced from examples/derive-patterns/join-one-sided.flow.yaml#/collections/patterns~1one-sided-join/derivation/transform/publishLHS."
import { SourceDocument as PublishLHSSource } from './ints';
export { SourceDocument as PublishLHSSource } from './ints';

// Generated from transform updateRHS as a re-export of collection patterns/strings.
// Referenced from examples/derive-patterns/join-one-sided.flow.yaml#/collections/patterns~1one-sided-join/derivation/transform/updateRHS."
import { SourceDocument as UpdateRHSSource } from './strings';
export { SourceDocument as UpdateRHSSource } from './strings';

// Generated from derivation examples/derive-patterns/join-one-sided.flow.yaml#/collections/patterns~1one-sided-join/derivation.
// Required to be implemented by examples/derive-patterns/join-one-sided.flow.ts.
export interface IDerivation {
    publishLHSPublish(source: PublishLHSSource, register: Register, previous: Register): OutputDocument[];
    updateRHSUpdate(source: UpdateRHSSource): Register[];
}
