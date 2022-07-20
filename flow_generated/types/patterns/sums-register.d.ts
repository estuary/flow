
// Generated from collection schema examples/derive-patterns/summer.flow.yaml?ptr=/collections/patterns~1sums-register/schema.
// Referenced from examples/derive-patterns/summer.flow.yaml#/collections/patterns~1sums-register.
export type Document = {
    Key: string;
    Sum?: number;
};


// Generated from derivation register schema examples/derive-patterns/summer.flow.yaml?ptr=/collections/patterns~1sums-register/derivation/register/schema.
// Referenced from examples/derive-patterns/summer.flow.yaml#/collections/patterns~1sums-register/derivation.
export type Register = number;


// Generated from transform fromInts as a re-export of collection patterns/ints.
// Referenced from examples/derive-patterns/summer.flow.yaml#/collections/patterns~1sums-register/derivation/transform/fromInts."
import { Document as FromIntsSource } from "./ints";
export { Document as FromIntsSource } from "./ints";


// Generated from derivation examples/derive-patterns/summer.flow.yaml#/collections/patterns~1sums-register/derivation.
// Required to be implemented by examples/derive-patterns/summer-reg.flow.ts.
export interface IDerivation {
    fromIntsUpdate(
        source: FromIntsSource,
    ): Register[];
    fromIntsPublish(
        source: FromIntsSource,
        register: Register,
        previous: Register,
    ): Document[];
}
