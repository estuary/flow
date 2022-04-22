import { IDerivation, Document, Register, FromIntsSource } from 'flow/patterns/sums-db';

// Implementation for derivation examples/derive-patterns/summer.flow.yaml#/collections/patterns~1sums-db/derivation.
export class Derivation implements IDerivation {
    fromIntsPublish(source: FromIntsSource, _register: Register, _previous: Register): Document[] {
        return [{ Key: source.Key, Sum: source.Int }];
    }
}
