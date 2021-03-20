import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation derive-patterns/join-inner.flow.yaml#/collections/patterns~1inner-join/derivation.
export class PatternsInnerJoin implements interfaces.PatternsInnerJoin {
    fromIntsUpdate(source: collections.PatternsInts): registers.PatternsInnerJoin[] {
        return [{ LHS: source.Int }];
    }
    fromIntsPublish(
        source: collections.PatternsInts,
        register: registers.PatternsInnerJoin,
        _previous: registers.PatternsInnerJoin,
    ): collections.PatternsInnerJoin[] {
        // Inner join requires that both sides be matched.
        if (register.LHS && register.RHS) {
            return [{ Key: source.Key, ...register }];
        }
        return [];
    }
    fromStringsUpdate(source: collections.PatternsStrings): registers.PatternsInnerJoin[] {
        return [{ RHS: [source.String] }];
    }
    fromStringsPublish(
        source: collections.PatternsStrings,
        register: registers.PatternsInnerJoin,
        _previous: registers.PatternsInnerJoin,
    ): collections.PatternsInnerJoin[] {
        if (register.LHS && register.RHS) {
            return [{ Key: source.Key, ...register }];
        }
        return [];
    }
}
