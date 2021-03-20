import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation derive-patterns/join-one-sided.flow.yaml#/collections/patterns~1one-sided-join/derivation.
export class PatternsOneSidedJoin implements interfaces.PatternsOneSidedJoin {
    publishLHSPublish(
        source: collections.PatternsInts,
        register: registers.PatternsOneSidedJoin,
        _previous: registers.PatternsOneSidedJoin,
    ): collections.PatternsOneSidedJoin[] {
        return [{ Key: source.Key, LHS: source.Int, RHS: register.RHS }];
    }
    updateRHSUpdate(source: collections.PatternsStrings): registers.PatternsOneSidedJoin[] {
        return [{ RHS: [source.String] }];
    }
}
