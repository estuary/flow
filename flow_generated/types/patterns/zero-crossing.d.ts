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

// Generated from collection schema examples/derive-patterns/schema.yaml#Int.
// Referenced from examples/derive-patterns/zero-crossing.flow.yaml#/collections/patterns~1zero-crossing.
export type Document = /* A document that holds an integer */ {
    Int: number;
    Key: string;
};

// The collection has one schema, used for both reads and writes.
export type SourceDocument = Document;
export type OutputDocument = Document;

// Generated from derivation register schema examples/derive-patterns/zero-crossing.flow.yaml?ptr=/collections/patterns~1zero-crossing/derivation/register/schema.
// Referenced from examples/derive-patterns/zero-crossing.flow.yaml#/collections/patterns~1zero-crossing/derivation.
export type Register = number;

// Generated from transform fromInts as a re-export of collection patterns/ints.
// Referenced from examples/derive-patterns/zero-crossing.flow.yaml#/collections/patterns~1zero-crossing/derivation/transform/fromInts."
import { SourceDocument as FromIntsSource } from './ints';
export { SourceDocument as FromIntsSource } from './ints';

// Generated from derivation examples/derive-patterns/zero-crossing.flow.yaml#/collections/patterns~1zero-crossing/derivation.
// Required to be implemented by examples/derive-patterns/zero-crossing.flow.ts.
export interface IDerivation {
    fromIntsUpdate(source: FromIntsSource): Register[];
    fromIntsPublish(source: FromIntsSource, register: Register, previous: Register): OutputDocument[];
}
