import { IDerivation, Document, Register, ByGrainSource } from 'flow/ops/TENANT/catalog-stats';

// Implementation for derivation template-common.flow.yaml#/collections/ops~1TENANT~1catalog-stats/derivation.
export class Derivation implements IDerivation {
    byGrainPublish(source: ByGrainSource, _register: Register, _previous: Register): Document[] {
        const ts = new Date(source.ts);
        const grains = grainsFromTS(ts);

        const { taskBytesRead, taskBytesWrote } = taskBytes(source);
        const taskDocs = grains.map((g) => ({
            name: source.shard.name,
            grain: g.grain,
            bytes_read_by: taskBytesRead,
            bytes_written_by: taskBytesWrote,
            bytes_read_from: 0,
            bytes_written_to: 0,
            ts: g.ts,
        }));

        const collectionStats = collectionBytes(source);
        const collectionDocs = [];
        for (const [catalogName, stats] of Object.entries(collectionStats)) {
            const thisCollectionDocs = grains.map((g) => ({
                name: catalogName,
                grain: g.grain,
                bytes_read_by: 0,
                bytes_written_by: 0,
                bytes_read_from: stats.bytesRead,
                bytes_written_to: stats.bytesWrote,
                ts: g.ts,
            }));

            collectionDocs.push(...thisCollectionDocs);
        }

        return [...taskDocs, ...collectionDocs];
    }
}

function grainsFromTS(ts: Date) {
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
}

function taskBytes(source: ByGrainSource) {
    let taskBytesWrote = 0; // Bytes this task wrote to a collection
    let taskBytesRead = 0; // Bytes this task read from a collection

    switch (source.shard.kind) {
        case 'capture':
            for (const collectionStats of Object.values(source.capture!)) {
                taskBytesWrote += collectionStats.out!.bytesTotal;
            }
            break;
        case 'materialization':
            for (const collectionStats of Object.values(source.materialize!)) {
                taskBytesRead += collectionStats.right!.bytesTotal;
            }
            break;
        case 'derivation':
            taskBytesWrote += source.derive!.out.bytesTotal;

            for (const transformStats of Object.values(source.derive!.transforms)) {
                taskBytesRead += transformStats.input.bytesTotal;
            }
    }

    return { taskBytesRead, taskBytesWrote };
}

type CollectionData = {
    [k: string]: {
        // Name of the collection
        bytesRead: number; // Bytes read from this collection by a materialization
        bytesWrote: number; // Bytes written to this collection by a capture
    };
};

function collectionBytes(source: ByGrainSource): CollectionData {
    const output: CollectionData = {};

    switch (true) {
        case !!source.capture:
            for (const [collectionName, stats] of Object.entries(source.capture!)) {
                if (!output[collectionName]) {
                    output[collectionName] = { bytesRead: 0, bytesWrote: 0 };
                }

                output[collectionName].bytesWrote += stats.out!.bytesTotal;
            }
            break;
        case !!source.materialize:
            for (const [collectionName, stats] of Object.entries(source.materialize!)) {
                if (!output[collectionName]) {
                    output[collectionName] = { bytesRead: 0, bytesWrote: 0 };
                }

                output[collectionName].bytesRead += stats.right!.bytesTotal;
            }
            break;

        case !!source.derive:
            // The collection being written to is the name of the task.
            if (!output[source.shard.name]) {
                output[source.shard.name] = { bytesRead: 0, bytesWrote: 0 };
            }

            output[source.shard.name].bytesWrote += source.derive!.out.bytesTotal;

            // Each transform will include a source collection that is read from.
            for (const transform of Object.values(source.derive!.transforms)) {
                if (!transform.source) {
                    continue;
                }

                if (!output[transform.source]) {
                    output[transform.source] = { bytesRead: 0, bytesWrote: 0 };
                }

                output[transform.source].bytesRead += transform.input.bytesTotal;
            }
    }

    return output;
}
