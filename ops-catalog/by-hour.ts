import { IDerivation, Document, Register, MinuteToHourSource } from 'flow/estuary/ops/task-stats/by-hour';


// Implementation for derivation derivations.flow.yaml#/collections/estuary~1ops~1task-stats~1by-hour/derivation.
export class Derivation implements IDerivation {
    minuteToHourPublish(
        source: MinuteToHourSource,
        _register: Register,
        _previous: Register,
    ): Document[] {
        const ts = new Date(source.ts);
        ts.setUTCMilliseconds(0);
        ts.setUTCSeconds(0);
        ts.setUTCMinutes(0);
        source.ts = ts.toISOString();
        return [source];
    }
}
