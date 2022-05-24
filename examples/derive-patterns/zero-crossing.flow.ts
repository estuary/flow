import { IDerivation, Document, Register, FromIntsSource } from 'flow/patterns/zero-crossing';

// Implementation for derivation examples/derive-patterns/zero-crossing.flow.yaml#/collections/patterns~1zero-crossing/derivation.
export class Derivation implements IDerivation {
    fromIntsUpdate(source: FromIntsSource): Register[] {
        return [source.Int];
    }
    fromIntsPublish(source: FromIntsSource, register: Register, previous: Register): Document[] {
        if (register > 0 != previous > 0) {
            return [source];
        }
        return [];
    }
}
