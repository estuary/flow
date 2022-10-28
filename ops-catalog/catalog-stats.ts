import { IDerivation, Document, Register, ByGrainSource } from 'flow/ops/TENANT/catalog-stats';

// Implementation for derivation template-common.flow.yaml#/collections/ops~1TENANT~1catalog-stats/derivation.
export class Derivation implements IDerivation {
    byGrainPublish(source: ByGrainSource, _register: Register, _previous: Register): Document[] {
        const ts = new Date(source.ts);
        const grains = grainsFromTS(ts);

        const taskDocs = mapStatsToDocsByGrain(grains, taskStats(source));
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
    [k: string]: {
        bytesWrittenByMe: number;
        docsWrittenByMe: number;
        bytesReadByMe: number;
        docsReadByMe: number;
        bytesWrittenToMe: number;
        docsWrittenToMe: number;
        bytesReadFromMe: number;
        docsReadFromMe: number;
    };
};

const initStatsData = () => ({
    bytesWrittenByMe: 0,
    docsWrittenByMe: 0,
    bytesReadByMe: 0,
    docsReadByMe: 0,
    bytesWrittenToMe: 0,
    docsWrittenToMe: 0,
    bytesReadFromMe: 0,
    docsReadFromMe: 0,
});

const mapStatsToDocsByGrain = (grains: TimeGrain[], stats: StatsData): Document[] =>
    Object.entries(stats).flatMap(([name, stats]) =>
        grains.map((g) => ({
            name,
            grain: g.grain,
            ts: g.ts,
            readBy: {
                bytesTotal: stats.bytesReadByMe,
                docsTotal: stats.docsReadByMe,
            },
            writtenBy: {
                bytesTotal: stats.bytesWrittenByMe,
                docsTotal: stats.docsWrittenByMe,
            },
            readFrom: {
                bytesTotal: stats.bytesReadFromMe,
                docsTotal: stats.docsReadFromMe,
            },
            writtenTo: {
                bytesTotal: stats.bytesWrittenToMe,
                docsTotal: stats.docsWrittenToMe,
            },
        })),
    );

const taskStats = (source: ByGrainSource) => {
    const output: StatsData = {};
    output[source.shard.name] = initStatsData();

    switch (source.shard.kind) {
        case 'capture':
            for (const collectionStats of Object.values(source.capture!)) {
                output[source.shard.name].bytesWrittenByMe += collectionStats.out!.bytesTotal;
                output[source.shard.name].docsWrittenByMe += collectionStats.out!.docsTotal;
            }
            break;
        case 'materialization':
            for (const collectionStats of Object.values(source.materialize!)) {
                output[source.shard.name].bytesReadByMe += collectionStats.right!.bytesTotal;
                output[source.shard.name].docsReadByMe += collectionStats.right!.docsTotal;
            }
            break;
        case 'derivation':
            output[source.shard.name].bytesWrittenByMe += source.derive!.out.bytesTotal;
            output[source.shard.name].docsWrittenByMe += source.derive!.out.docsTotal;

            for (const transformStats of Object.values(source.derive!.transforms)) {
                output[source.shard.name].bytesReadByMe += transformStats.input.bytesTotal;
                output[source.shard.name].docsReadByMe += transformStats.input.docsTotal;
            }
    }

    return output;
};

const collectionStats = (source: ByGrainSource): StatsData => {
    const output: StatsData = {};

    switch (true) {
        case !!source.capture:
            for (const [collectionName, stats] of Object.entries(source.capture!)) {
                if (!output[collectionName]) {
                    output[collectionName] = initStatsData();
                }

                output[collectionName].bytesWrittenToMe += stats.out!.bytesTotal;
                output[collectionName].docsWrittenToMe += stats.out!.docsTotal;
            }
            break;
        case !!source.materialize:
            for (const [collectionName, stats] of Object.entries(source.materialize!)) {
                if (!output[collectionName]) {
                    output[collectionName] = initStatsData();
                }

                output[collectionName].bytesReadFromMe += stats.right!.bytesTotal;
                output[collectionName].docsReadFromMe += stats.right!.docsTotal;
            }
            break;

        case !!source.derive:
            // The collection being written to is the name of the task.
            if (!output[source.shard.name]) {
                output[source.shard.name] = initStatsData();
            }

            output[source.shard.name].bytesWrittenToMe += source.derive!.out.bytesTotal;
            output[source.shard.name].docsWrittenToMe += source.derive!.out.docsTotal;

            // Each transform will include a source collection that is read from.
            for (const transform of Object.values(source.derive!.transforms)) {
                if (!transform.source) {
                    continue;
                }

                if (!output[transform.source]) {
                    output[transform.source] = initStatsData();
                }

                output[transform.source].bytesReadFromMe += transform.input.bytesTotal;
                output[transform.source].docsReadFromMe += transform.input.docsTotal;
            }
    }

    return output;
};
