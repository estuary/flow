import { Document, IDerivation, SourceLogs, SourceStats } from "flow/ops/rollups/L1/BASE_NAME/catalog-stats.ts";

// Implementation for derivation ops/rollups/L1/BASE_NAME/catalog-stats.
export class Derivation extends IDerivation {
    logs(read: { doc: SourceLogs }): Document[] {
        const source = read.doc;
        let stats: Document["statsSummary"] = {};

        if (source.level == "error" && source.message == "shard failed") {
            stats = { failures: 1 };
        } else if (source.level == "error") {
            stats = { errors: 1 };
        } else if (source.level == "warn") {
            stats = { warnings: 1 };
        } else {
            return [];
        }

        const grains = grainsFromTS(new Date(source.ts));
        return mapStatsToDocsByGrain(grains, { [source.shard.name]: stats });
    }

    stats(read: { doc: SourceStats }): Document[] {
        const source = read.doc;
        const ts = new Date(source.ts);
        const grains = grainsFromTS(ts);

        const taskDocs = mapStatsToDocsByGrain(grains, taskStats(source)).map((doc) => {
            if (doc.catalogName.endsWith("/")) {
                return doc;
            } else {
                // For documents generated specific to this task, retain the detailed information about
                // the task itself.
                return {
                    ...doc,
                    taskStats: {
                        capture: source.capture,
                        derive: source.derive,
                        materialize: source.materialize,
                        interval: source.interval,
                    },
                }
            }
        });

        // Documents generated for collections involved in this task will not have associated
        // detailed task information. If the collection is a derivation, that will be accounted for
        // above.
        const collectionDocs = mapStatsToDocsByGrain(grains, collectionStats(source));

        return [...taskDocs, ...collectionDocs];
    }
}

type TimeGrain = {
    grain: Document["grain"];
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
            grain: "hourly" as Document["grain"],
            ts: hourlyTS,
        },
        {
            grain: "daily" as Document["grain"],
            ts: dailyTS,
        },
        {
            grain: "monthly" as Document["grain"],
            ts: monthlyTS,
        },
    ];
};

type StatsData = {
    [k: string]: Document["statsSummary"];
};

function catalogPrefixes(catalogName: string): string[] {
    let splits = catalogName.split("/");
    let out: string[] = [];
    for (let i = 1; i < splits.length; i++) {
        let parts = splits.slice(0, i);
        let name = parts.join("/") + "/";
        out.push(name);
    }
    return out;
}

function mapStatsToDocsByGrain(grains: TimeGrain[], stats: StatsData): Document[] {
    let docs = Object.entries(stats).flatMap(([catalogName, statsSummary]) =>
        grains.map((g) => ({
            ...g,
            catalogName,
            statsSummary,
        }))
    );

    let out: Document[] = docs.slice();
    // Also emit stats for each catalog name prefix.
    for (var doc of docs) {
        let prefixes = catalogPrefixes(doc.catalogName);
        for (var p of prefixes) {
            let newDoc = {
                ...doc,
                catalogName: p,
            };
            // Remove the `taskStats` from these prefix stats documents, because
            // they contain the raw stats broken down by binding, which would
            // otherwise result in objects with a potentially absurd number of
            // keys after they get reduced.
            // if (newDoc.taskStats) {
            //     delete newDoc['taskStats'];
            // }
            out.push(newDoc);
        }
    }
    return out;
}

const taskStats = (source: SourceStats): StatsData => {
    const stats: Document["statsSummary"] = {};

    // For captures, derivations, and materializations, we walk through all
    // bound collections and sum up the total data written or read by this task.
    if (source.capture) {
        for (const collectionStats of Object.values(source.capture!)) {
            stats.writtenByMe = accumulateStats(stats.writtenByMe, collectionStats.out);
        }
    } else if (source.materialize) {
        for (const collectionStats of Object.values(source.materialize!)) {
            stats.readByMe = accumulateStats(stats.readByMe, collectionStats.right);
        }
    } else if (source.derive) {
        stats.writtenByMe = accumulateStats(stats.writtenByMe, source.derive!.out);
        for (const transformStats of Object.values(source.derive!.transforms || {})) {
            stats.readByMe = accumulateStats(stats.readByMe, transformStats.input);
        }
    } else if (source.interval?.usageRate) {
        stats.usageSeconds = Math.round(source.interval.uptimeSeconds * source.interval.usageRate);
    }

    const output: StatsData = {};
    output[source.shard.name] = stats;
    return output;
};

const collectionStats = (source: SourceStats): StatsData => {
    const output: StatsData = {};

    // An individual collection can be written to/read from a single time by a
    // capture/materialization in a a single stats document, but as noted above there can be
    // multiple collections bound by a task. So we will potentially emit multiple collection
    // stats documents for a single task.
    if (source.capture) {
        for (const [collectionName, stats] of Object.entries(source.capture!)) {
            if (!output[collectionName]) {
                output[collectionName] = {};
            }
            output[collectionName].writtenToMe = accumulateStats(output[collectionName].writtenToMe, stats.out);
        }
    } else if (source.materialize) {
        for (const [collectionName, stats] of Object.entries(source.materialize!)) {
            if (!output[collectionName]) {
                output[collectionName] = {};
            }
            output[collectionName].readFromMe = accumulateStats(output[collectionName].readFromMe, stats.right);
        }
    } else if (source.derive) {
        // A derivation will have one collection written to (itself), and can read from multiple
        // collections named in the transforms.

        // The collection being written to is the name of the task.
        if (!output[source.shard.name]) {
            output[source.shard.name] = {};
        }

        output[source.shard.name].writtenToMe = accumulateStats(
            output[source.shard.name].writtenToMe,
            source.derive!.out,
        );

        // Each transform will include a source collection that is read from.
        for (const transform of Object.values(source.derive!.transforms || {})) {
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

// accumulateStats will reduce stats into the accumulator via addition with special handling to
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
