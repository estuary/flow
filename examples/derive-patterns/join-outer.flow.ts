import { IDerivation, Document, Register, FromIntsSource, FromStringsSource } from 'flow/patterns/outer-join';

// Implementation for derivation examples/derive-patterns/join-outer.flow.yaml#/collections/patterns~1outer-join/derivation.
export class Derivation implements IDerivation {
    fromIntsPublish(source: FromIntsSource, _register: Register, _previous: Register): Document[] {
        return [{ Key: source.Key, LHS: source.Int }];
    }
    fromStringsPublish(source: FromStringsSource, _register: Register, _previous: Register): Document[] {
        return [{ Key: source.Key, RHS: [source.String] }];
    }
}
