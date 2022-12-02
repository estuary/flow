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

// Generated from collection schema examples/derive-patterns/join-inner.flow.yaml?ptr=/collections/patterns~1inner-join/schema.
// Referenced from examples/derive-patterns/join-inner.flow.yaml#/collections/patterns~1inner-join.
export type Document = /* Document for join examples */ {
    Key: string;
    LHS?: number;
    RHS?: string[];
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;

// Generated from derivation register schema examples/derive-patterns/schema.yaml#Join.
// Referenced from examples/derive-patterns/join-inner.flow.yaml#/collections/patterns~1inner-join/derivation.
export type Register = /* Document for join examples */ {
    Key?: string;
    LHS?: number;
    RHS?: string[];
};

// Generated from transform fromInts as a re-export of collection patterns/ints.
// Referenced from examples/derive-patterns/join-inner.flow.yaml#/collections/patterns~1inner-join/derivation/transform/fromInts."
import { SourceDocument as FromIntsSource } from './ints';
export { SourceDocument as FromIntsSource } from './ints';

// Generated from transform fromStrings as a re-export of collection patterns/strings.
// Referenced from examples/derive-patterns/join-inner.flow.yaml#/collections/patterns~1inner-join/derivation/transform/fromStrings."
import { SourceDocument as FromStringsSource } from './strings';
export { SourceDocument as FromStringsSource } from './strings';

// Generated from derivation examples/derive-patterns/join-inner.flow.yaml#/collections/patterns~1inner-join/derivation.
// Required to be implemented by examples/derive-patterns/join-inner.flow.ts.
export interface IDerivation {
    fromIntsUpdate(source: FromIntsSource): Register[];
    fromIntsPublish(source: FromIntsSource, register: Register, previous: Register): OutputDocument[];
    fromStringsUpdate(source: FromStringsSource): Register[];
    fromStringsPublish(source: FromStringsSource, register: Register, previous: Register): OutputDocument[];
}
