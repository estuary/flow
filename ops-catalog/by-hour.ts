import { IDerivation, Document, Register, ByHourSource } from 'flow/ops/TENANT/stats-by-hour';

// Implementation for derivation template.flow.yaml#/collections/ops~1TENANT~1stats-by-hour/derivation.
export class Derivation implements IDerivation {
    byHourPublish(source: ByHourSource, _register: Register, _previous: Register): Document[] {
        const ts = new Date(source.ts);
        ts.setUTCMilliseconds(0);
        ts.setUTCSeconds(0);
        ts.setUTCMinutes(0);

        return [
            {
                // Pass-through most of `source`...
                ...source,
                // But override `ts` with its truncated form.
                ts: ts.toISOString(),
                // And extend `source.shard` with a composed `split` property.
                shard: {
                    ...source.shard,
                    split: source.shard.keyBegin + ':' + source.shard.rClockBegin,
                },
            },
        ];
    }
}
