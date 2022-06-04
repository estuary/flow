import { IDerivation, Document, Register, AcmeCoSource, DeepsyncSource, FenestraSource, PhilSource, RocksetSource, WgdSource } from 'flow/estuary/ops/task-stats/by-minute';

interface HasTimestamp {
    ts: string;
}

function truncateToMinute(doc: HasTimestamp) {
    const date = new Date(doc.ts);
    date.setUTCMilliseconds(0);
    date.setUTCSeconds(0);
    doc.ts = date.toISOString();
}

// Implementation for derivation derivations.flow.yaml#/collections/estuary~1ops~1task-stats~1by-minute/derivation.
export class Derivation implements IDerivation {
    acmeCoPublish(
        source: AcmeCoSource,
        _register: Register,
        _previous: Register,
    ): Document[] {
        truncateToMinute(source);
        return [source];
    }
    deepsyncPublish(
        source: DeepsyncSource,
        _register: Register,
        _previous: Register,
    ): Document[] {
        truncateToMinute(source);
        return [source];
    }
    fenestraPublish(
        source: FenestraSource,
        _register: Register,
        _previous: Register,
    ): Document[] {
        truncateToMinute(source);
        return [source];
    }
    philPublish(
        source: PhilSource,
        _register: Register,
        _previous: Register,
    ): Document[] {
        truncateToMinute(source);
        return [source];
    }
    rocksetPublish(
        source: RocksetSource,
        _register: Register,
        _previous: Register,
    ): Document[] {
        truncateToMinute(source);
        return [source];
    }
    wgdPublish(
        source: WgdSource,
        _register: Register,
        _previous: Register,
    ): Document[] {
        truncateToMinute(source);
        return [source];
    }
}
