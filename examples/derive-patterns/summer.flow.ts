import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation derive-patterns/summer.flow.yaml#/collections/patterns~1sums-db/derivation.
export class PatternsSumsDb implements interfaces.PatternsSumsDb {
    fromIntsPublish(
        source: collections.PatternsInts,
        _register: registers.PatternsSumsDb,
        _previous: registers.PatternsSumsDb,
    ): collections.PatternsSumsDb[] {
        return [{ Key: source.Key, Sum: source.Int }];
    }
}

// Implementation for derivation derive-patterns/summer.flow.yaml#/collections/patterns~1sums-register/derivation.
export class PatternsSumsRegister implements interfaces.PatternsSumsRegister {
    fromIntsUpdate(source: collections.PatternsInts): registers.PatternsSumsRegister[] {
        return [source.Int];
    }
    fromIntsPublish(
        source: collections.PatternsInts,
        register: registers.PatternsSumsRegister,
        _previous: registers.PatternsSumsRegister,
    ): collections.PatternsSumsRegister[] {
        return [{ Key: source.Key, Sum: register }];
    }
}
