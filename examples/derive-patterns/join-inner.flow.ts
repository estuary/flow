import { IDerivation, Document, Register, FromIntsSource, FromStringsSource } from 'flow/patterns/inner-join';

// Implementation for derivation examples/derive-patterns/join-inner.flow.yaml#/collections/patterns~1inner-join/derivation.
export class Derivation implements IDerivation {
    fromIntsUpdate(source: FromIntsSource): Register[] {
        return [{ LHS: source.Int }];
    }
    fromIntsPublish(source: FromIntsSource, register: Register, _previous: Register): Document[] {
        // Inner join requires that both sides be matched.
        if (register.LHS && register.RHS) {
            return [{ Key: source.Key, ...register }];
        }
        return [];
    }
    fromStringsUpdate(source: FromStringsSource): Register[] {
        return [{ RHS: [source.String] }];
    }
    fromStringsPublish(source: FromStringsSource, register: Register, _previous: Register): Document[] {
        if (register.LHS && register.RHS) {
            return [{ Key: source.Key, ...register }];
        }
        return [];
    }
}
