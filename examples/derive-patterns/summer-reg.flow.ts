import { IDerivation, Document, Register, FromIntsSource } from 'flow/patterns/sums-register';

// Implementation for derivation examples/derive-patterns/summer.flow.yaml#/collections/patterns~1sums-register/derivation.
export class Derivation implements IDerivation {
    fromIntsUpdate(source: FromIntsSource): Register[] {
        return [source.Int];
    }
    fromIntsPublish(source: FromIntsSource, register: Register, _previous: Register): Document[] {
        return [{ Key: source.Key, Sum: register }];
    }
}
