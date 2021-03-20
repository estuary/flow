import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation derive-patterns/join-outer.flow.yaml#/collections/patterns~1outer-join/derivation.
export class PatternsOuterJoin implements interfaces.PatternsOuterJoin {
    fromIntsPublish(
        source: collections.PatternsInts,
        _register: registers.PatternsOuterJoin,
        _previous: registers.PatternsOuterJoin,
    ): collections.PatternsOuterJoin[] {
        return [{ Key: source.Key, LHS: source.Int }];
    }
    fromStringsPublish(
        source: collections.PatternsStrings,
        _register: registers.PatternsOuterJoin,
        _previous: registers.PatternsOuterJoin,
    ): collections.PatternsOuterJoin[] {
        return [{ Key: source.Key, RHS: [source.String] }];
    }
}
