import { IDerivation, Document, Register, HourToDaySource } from 'flow/estuary/ops/task-stats/by-day';

// Implementation for derivation derivations.flow.yaml#/collections/estuary~1ops~1task-stats~1by-day/derivation.
export class Derivation implements IDerivation {
    hourToDayPublish(
        source: HourToDaySource,
        _register: Register,
        _previous: Register,
    ): Document[] {
        const ts = new Date(source.ts);
        ts.setUTCHours(0, 0, 0, 0);
        source.ts = ts.toISOString();
        return [source];
    }
}
