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

const mapStatsToDocsByGrain = (grains: TimeGrain[], stats: StatsData): Document[] =>
    Object.entries(stats).flatMap(([catalogName, statsSummary]) =>
        grains.map((g) => ({
            ...g,
            catalogName,
            statsSummary,
        })),
    );

const taskStats = (source: ByGrainSource): StatsData => {
    const stats: Document['statsSummary'] = {};

    switch (source.shard.kind) {
        // For captures and materializations, go through all of the bound collections and sum up the
        // data written or read by this task.
        case 'capture':
            for (const collectionStats of Object.values(source.capture!)) {
                stats.writtenByMe = accumulateStats(stats.writtenByMe, collectionStats.out);
            }
            break;
        case 'materialization':
            for (const collectionStats of Object.values(source.materialize!)) {
                stats.readByMe = accumulateStats(stats.readByMe, collectionStats.right);
            }
            break;
        // A derivation will both read and write: Writes to a single collection (the derivation
        // itself), and reads from the collections named in the transforms of the derivation.
        case 'derivation':
            stats.writtenByMe = accumulateStats(stats.writtenByMe, source.derive!.out);

            for (const transformStats of Object.values(source.derive!.transforms)) {
                stats.readByMe = accumulateStats(stats.readByMe, transformStats.input);
            }
    }

    const output: StatsData = {};
    output[source.shard.name] = stats;
    return output;
};

const collectionStats = (source: ByGrainSource): StatsData => {
    const output: StatsData = {};

    switch (true) {
        // An individual collection can be written to/read from a single time by a
        // capture/materialization in a a single stats document, but as noted above there can be
        // multiple collections bound by a task. So we will potentially emit multiple collection
        // stats documents for a single task.
        case !!source.capture:
            for (const [collectionName, stats] of Object.entries(source.capture!)) {
                if (!output[collectionName]) {
                    output[collectionName] = {};
                }
                output[collectionName].writtenToMe = accumulateStats(output[collectionName].writtenToMe, stats.out);
            }
            break;
        case !!source.materialize:
            for (const [collectionName, stats] of Object.entries(source.materialize!)) {
                if (!output[collectionName]) {
                    output[collectionName] = {};
                }
                output[collectionName].readFromMe = accumulateStats(output[collectionName].readFromMe, stats.right);
            }
            break;
        // A derivation will have one collection written to (itself), and can read from multiple
        // collections named in the transforms.
        case !!source.derive:
            // The collection being written to is the name of the task.
            if (!output[source.shard.name]) {
                output[source.shard.name] = {};
            }

            output[source.shard.name].writtenToMe = accumulateStats(
                output[source.shard.name].writtenToMe,
                source.derive!.out,
            );

            // Each transform will include a source collection that is read from.
            for (const transform of Object.values(source.derive!.transforms)) {
                if (!transform.source) {
                    // Legacy stats docs may not list a source collection for derivations.
                    continue;
                }

                if (!output[transform.source]) {
                    output[transform.source] = {};
                }

                output[transform.source].readFromMe = accumulateStats(
                    output[transform.source].readFromMe,
                    transform.input,
                );
            }
    }

    return output;
};

// accumulateStats will reduce stats into the accumlator via addition with special handling to
// return "undefined" rather than an explicit zero value if the stats are zero.
const accumulateStats = (
    accumulator: { bytesTotal: number; docsTotal: number } | undefined,
    stats: { bytesTotal: number; docsTotal: number } | undefined,
): { bytesTotal: number; docsTotal: number } | undefined => {
    // If there are no stats to add return the accumulator as-is.
    if (!stats || (stats.bytesTotal === 0 && stats.docsTotal === 0)) {
        return accumulator;
    }

    // There are stats to add, so make sure the accumulator is defined before adding them.
    const returnedAccumulated = accumulator || { bytesTotal: 0, docsTotal: 0 };
    returnedAccumulated.bytesTotal += stats.bytesTotal;
    returnedAccumulated.docsTotal += stats.docsTotal;

    return returnedAccumulated;
};
