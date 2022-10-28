import { IDerivation, Document, Register, ByGrainSource } from 'flow/ops/TENANT/catalog-stats';

// Implementation for derivation template-common.flow.yaml#/collections/ops~1TENANT~1catalog-stats/derivation.
export class Derivation implements IDerivation {
    byGrainPublish(source: ByGrainSource, _register: Register, _previous: Register): Document[] {
        const ts = new Date(source.ts);
        const grains = grainsFromTS(ts);

        const taskDocs = mapStatsToDocsByGrain(grains, taskStats(source)).map((doc) => ({
            ...doc,
            // For documents generated specific to this task, retain the detailed information about
            // the task itself.
            taskStats: {
                capture: source.capture,
                derive: source.derive,
                materialize: source.materialize,
            },
        }));

        // Documents generated for collections involved in this task will not have associated
        // detailed task information. If the collection is a derivation, that will be accounted for
        // above.
        const collectionDocs = mapStatsToDocsByGrain(grains, collectionStats(source));

        return [...taskDocs, ...collectionDocs];
    }
}

type TimeGrain = {
    grain: Document['grain'];
    ts: string;
};

const grainsFromTS = (ts: Date): TimeGrain[] => {
    ts.setUTCMilliseconds(0);
    ts.setUTCSeconds(0);
    ts.setUTCMinutes(0);

    const hourlyTS = ts.toISOString();
    ts.setUTCHours(0);
    const dailyTS = ts.toISOString();
    ts.setUTCDate(1);
    const monthlyTS = ts.toISOString();

    return [
        {
            grain: 'hourly' as Document['grain'],
            ts: hourlyTS,
        },
        {
            grain: 'daily' as Document['grain'],
            ts: dailyTS,
        },
        {
            grain: 'monthly' as Document['grain'],
            ts: monthlyTS,
        },
    ];
};

type StatsData = {
    [k: string]: Document['statsSummary'];
};

const newDocumentStats = (): Document['statsSummary'] => ({
    writtenBy: {
        bytesTotal: 0,
        docsTotal: 0,
    },
    readBy: {
        bytesTotal: 0,
        docsTotal: 0,
    },
    writtenTo: {
        bytesTotal: 0,
        docsTotal: 0,
    },
    readFrom: {
        bytesTotal: 0,
        docsTotal: 0,
    },
});

const mapStatsToDocsByGrain = (grains: TimeGrain[], stats: StatsData): Document[] =>
    Object.entries(stats).flatMap(([name, statsSummary]) =>
        grains.map((g) => ({
            ...g,
            name,
            statsSummary,
        })),
    );

const taskStats = (source: ByGrainSource) => {
    const stats = newDocumentStats();

    switch (source.shard.kind) {
        case 'capture':
            for (const collectionStats of Object.values(source.capture!)) {
                stats.writtenBy.bytesTotal += collectionStats.out!.bytesTotal;
                stats.writtenBy.docsTotal += collectionStats.out!.docsTotal;
            }
            break;
        case 'materialization':
            for (const collectionStats of Object.values(source.materialize!)) {
                stats.readBy.bytesTotal += collectionStats.right!.bytesTotal;
                stats.readBy.docsTotal += collectionStats.right!.docsTotal;
            }
            break;
        case 'derivation':
            stats.writtenBy.bytesTotal += source.derive!.out.bytesTotal;
            stats.writtenBy.docsTotal += source.derive!.out.docsTotal;

            for (const transformStats of Object.values(source.derive!.transforms)) {
                stats.readBy.bytesTotal += transformStats.input.bytesTotal;
                stats.readBy.docsTotal += transformStats.input.docsTotal;
            }
    }

    const output: StatsData = {};
    output[source.shard.name] = stats;
    return output;
};

const collectionStats = (source: ByGrainSource): StatsData => {
    const output: StatsData = {};

    switch (true) {
        case !!source.capture:
            for (const [collectionName, stats] of Object.entries(source.capture!)) {
                if (!output[collectionName]) {
                    output[collectionName] = newDocumentStats();
                }

                output[collectionName].writtenTo.bytesTotal += stats.out!.bytesTotal;
                output[collectionName].writtenTo.docsTotal += stats.out!.docsTotal;
            }
            break;
        case !!source.materialize:
            for (const [collectionName, stats] of Object.entries(source.materialize!)) {
                if (!output[collectionName]) {
                    output[collectionName] = newDocumentStats();
                }

                output[collectionName].readFrom.bytesTotal += stats.right!.bytesTotal;
                output[collectionName].readFrom.docsTotal += stats.right!.docsTotal;
            }
            break;

        case !!source.derive:
            // The collection being written to is the name of the task.
            if (!output[source.shard.name]) {
                output[source.shard.name] = newDocumentStats();
            }

            output[source.shard.name].writtenTo.bytesTotal += source.derive!.out.bytesTotal;
            output[source.shard.name].writtenTo.docsTotal += source.derive!.out.docsTotal;

            // Each transform will include a source collection that is read from.
            for (const transform of Object.values(source.derive!.transforms)) {
                if (!transform.source) {
                    continue;
                }

                if (!output[transform.source]) {
                    output[transform.source] = newDocumentStats();
                }

                output[transform.source].readFrom.bytesTotal += transform.input.bytesTotal;
                output[transform.source].readFrom.docsTotal += transform.input.docsTotal;
            }
    }

    return output;
};
