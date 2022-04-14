import { collections, interfaces, registers } from 'flow/modules';

// Implementation for derivation flow.yaml#/collections/estuary~1ops~1task-stats~1by-day/derivation.
export class EstuaryOpsTaskStatsByDay implements interfaces.EstuaryOpsTaskStatsByDay {
    hourToDayPublish(
        source: collections.EstuaryOpsTaskStatsByHour,
        _register: registers.EstuaryOpsTaskStatsByDay,
        _previous: registers.EstuaryOpsTaskStatsByDay,
    ): collections.EstuaryOpsTaskStatsByDay[] {
        const ts = new Date(source.ts);
        ts.setUTCHours(0, 0, 0, 0);
        source.ts = ts.toISOString();
        return [source];
    }
}

// Implementation for derivation flow.yaml#/collections/estuary~1ops~1task-stats~1by-hour/derivation.
export class EstuaryOpsTaskStatsByHour implements interfaces.EstuaryOpsTaskStatsByHour {
    minuteToHourPublish(
        source: collections.EstuaryOpsTaskStatsByMinute,
        _register: registers.EstuaryOpsTaskStatsByHour,
        _previous: registers.EstuaryOpsTaskStatsByHour,
    ): collections.EstuaryOpsTaskStatsByHour[] {
        const ts = new Date(source.ts);
        ts.setUTCMilliseconds(0);
        ts.setUTCSeconds(0);
        ts.setUTCMinutes(0);
        source.ts = ts.toISOString();
        return [source];
    }
}

interface HasTimestamp {
    ts: string;
}

function truncateToMinute(doc: HasTimestamp) {
    const date = new Date(doc.ts);
    date.setUTCMilliseconds(0);
    date.setUTCSeconds(0);
    doc.ts = date.toISOString();
}

// Implementation for derivation flow.yaml#/collections/estuary~1ops~1task-stats~1by-minute/derivation.
export class EstuaryOpsTaskStatsByMinute implements interfaces.EstuaryOpsTaskStatsByMinute {
    acmeCoPublish(
        source: collections.OpsAcmeCoStats,
        _register: registers.EstuaryOpsTaskStatsByMinute,
        _previous: registers.EstuaryOpsTaskStatsByMinute,
    ): collections.EstuaryOpsTaskStatsByMinute[] {
        truncateToMinute(source);
        return [source];
    }
    deepsyncPublish(
        source: collections.OpsDeepsyncStats,
        _register: registers.EstuaryOpsTaskStatsByMinute,
        _previous: registers.EstuaryOpsTaskStatsByMinute,
    ): collections.EstuaryOpsTaskStatsByMinute[] {
        truncateToMinute(source);
        return [source];
    }
    fenestraPublish(
        source: collections.OpsFenestraStats,
        _register: registers.EstuaryOpsTaskStatsByMinute,
        _previous: registers.EstuaryOpsTaskStatsByMinute,
    ): collections.EstuaryOpsTaskStatsByMinute[] {
        truncateToMinute(source);
        return [source];
    }
    philPublish(
        source: collections.OpsPhilStats,
        _register: registers.EstuaryOpsTaskStatsByMinute,
        _previous: registers.EstuaryOpsTaskStatsByMinute,
    ): collections.EstuaryOpsTaskStatsByMinute[] {
        truncateToMinute(source);
        return [source];
    }
    rocksetPublish(
        source: collections.OpsRocksetStats,
        _register: registers.EstuaryOpsTaskStatsByMinute,
        _previous: registers.EstuaryOpsTaskStatsByMinute,
    ): collections.EstuaryOpsTaskStatsByMinute[] {
        truncateToMinute(source);
        return [source];
    }
    wgdPublish(
        source: collections.OpsWgdStats,
        _register: registers.EstuaryOpsTaskStatsByMinute,
        _previous: registers.EstuaryOpsTaskStatsByMinute,
    ): collections.EstuaryOpsTaskStatsByMinute[] {
        truncateToMinute(source);
        return [source];
    }
}
