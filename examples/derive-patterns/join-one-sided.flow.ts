import { IDerivation, Document, Register, PublishLHSSource, UpdateRHSSource } from 'flow/patterns/one-sided-join';

// Implementation for derivation examples/derive-patterns/join-one-sided.flow.yaml#/collections/patterns~1one-sided-join/derivation.
export class Derivation implements IDerivation {
    publishLHSPublish(source: PublishLHSSource, register: Register, _previous: Register): Document[] {
        return [{ Key: source.Key, LHS: source.Int, RHS: register.RHS }];
    }
    updateRHSUpdate(source: UpdateRHSSource): Register[] {
        return [{ RHS: [source.String] }];
    }
}
