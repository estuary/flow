import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation derive-patterns/zero-crossing.flow.yaml#/collections/patterns~1zero-crossing/derivation.
export class PatternsZeroCrossing implements interfaces.PatternsZeroCrossing {
    fromIntsUpdate(source: collections.PatternsInts): registers.PatternsZeroCrossing[] {
        return [source.Int];
    }
    fromIntsPublish(
        source: collections.PatternsInts,
        register: registers.PatternsZeroCrossing,
        previous: registers.PatternsZeroCrossing,
    ): collections.PatternsZeroCrossing[] {
        if (register > 0 != previous > 0) {
            return [source];
        }
        return [];
    }
}
