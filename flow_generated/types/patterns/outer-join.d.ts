
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


// Generated from collection schema examples/derive-patterns/join-outer.flow.yaml?ptr=/collections/patterns~1outer-join/schema.
// Referenced from examples/derive-patterns/join-outer.flow.yaml#/collections/patterns~1outer-join.
export type Document = /* Document for join examples */ {
    Key: string;
    LHS?: number;
    RHS?: string[];
};


// Generated from derivation register schema examples/derive-patterns/join-outer.flow.yaml?ptr=/collections/patterns~1outer-join/derivation/register/schema.
// Referenced from examples/derive-patterns/join-outer.flow.yaml#/collections/patterns~1outer-join/derivation.
export type Register = unknown;


// Generated from transform fromInts as a re-export of collection patterns/ints.
// Referenced from examples/derive-patterns/join-outer.flow.yaml#/collections/patterns~1outer-join/derivation/transform/fromInts."
import { Document as FromIntsSource } from "./ints";
export { Document as FromIntsSource } from "./ints";


// Generated from transform fromStrings as a re-export of collection patterns/strings.
// Referenced from examples/derive-patterns/join-outer.flow.yaml#/collections/patterns~1outer-join/derivation/transform/fromStrings."
import { Document as FromStringsSource } from "./strings";
export { Document as FromStringsSource } from "./strings";


// Generated from derivation examples/derive-patterns/join-outer.flow.yaml#/collections/patterns~1outer-join/derivation.
// Required to be implemented by examples/derive-patterns/join-outer.flow.ts.
export interface IDerivation {
    fromIntsPublish(
        source: FromIntsSource,
        register: Register,
        previous: Register,
    ): Document[];
    fromStringsPublish(
        source: FromStringsSource,
        register: Register,
        previous: Register,
    ): Document[];
}
